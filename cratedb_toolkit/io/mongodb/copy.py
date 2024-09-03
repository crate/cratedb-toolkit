# ruff: noqa: S608
import logging
import typing as t

import pymongo
import sqlalchemy as sa
from bson.raw_bson import RawBSONDocument
from commons_codec.model import SQLOperation
from commons_codec.transform.mongodb import MongoDBCDCTranslatorCrateDB
from tqdm import tqdm

from cratedb_toolkit.io.mongodb.export import extract_value
from cratedb_toolkit.io.mongodb.transform import TransformationManager
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util import DatabaseAdapter

logger = logging.getLogger(__name__)


class MongoDBFullLoadTranslator(MongoDBCDCTranslatorCrateDB):
    def __init__(self, table_name: str, tm: TransformationManager = None):
        super().__init__(table_name=table_name)
        self.tm = tm

    @staticmethod
    def get_document_key(record: t.Dict[str, t.Any]) -> str:
        """
        Return value of document key (MongoDB document OID) from CDC record.

        "documentKey": {"_id": ObjectId("669683c2b0750b2c84893f3e")}
        """
        return record["_id"]

    def to_sql(self, document: t.Dict[str, t.Any]) -> SQLOperation:
        """
        Produce CrateDB INSERT SQL statement from MongoDB document.
        """

        # Define SQL INSERT statement.
        sql = f"INSERT INTO {self.table_name} ({self.ID_COLUMN}, {self.DATA_COLUMN}) VALUES (:oid, :record);"

        # Converge MongoDB document to SQL parameters.
        record = extract_value(self.decode_bson(document))
        oid: str = self.get_document_key(record)
        parameters = {"oid": oid, "record": record}

        return SQLOperation(sql, parameters)


class MongoDBFullLoad:
    """
    Copy MongoDB collection into CrateDB table.
    """

    def __init__(
        self,
        mongodb_url: str,
        mongodb_database: str,
        mongodb_collection: str,
        cratedb_url: str,
        tm: t.Union[TransformationManager, None],
        mongodb_limit: int = 0,
        progress: bool = False,
        debug: bool = True,
    ):
        cratedb_address = DatabaseAddress.from_string(cratedb_url)
        cratedb_sqlalchemy_url, cratedb_table_address = cratedb_address.decode()
        cratedb_table = cratedb_table_address.fullname

        self.mongodb_client: pymongo.MongoClient = pymongo.MongoClient(
            mongodb_url,
            document_class=RawBSONDocument,
            datetime_conversion="DATETIME_AUTO",
        )
        self.mongodb_collection = self.mongodb_client[mongodb_database][mongodb_collection]
        self.mongodb_limit = mongodb_limit
        self.cratedb_adapter = DatabaseAdapter(str(cratedb_sqlalchemy_url), echo=False)
        self.cratedb_table = self.cratedb_adapter.quote_relation_name(cratedb_table)
        self.translator = MongoDBFullLoadTranslator(table_name=self.cratedb_table, tm=tm)

        self.progress = progress
        self.debug = debug

    def start(self):
        """
        Read items from DynamoDB table, convert to SQL INSERT statements, and submit to CrateDB.
        """
        records_in = self.mongodb_collection.count_documents(filter={})
        logger.info(f"Source: MongoDB collection={self.mongodb_collection} count={records_in}")
        logger_on_error = logger.warning
        if self.debug:
            logger_on_error = logger.exception
        with self.cratedb_adapter.engine.connect() as connection:
            if not self.cratedb_adapter.table_exists(self.cratedb_table):
                connection.execute(sa.text(self.translator.sql_ddl))
                connection.commit()
            records_target = self.cratedb_adapter.count_records(self.cratedb_table)
            logger.info(f"Target: CrateDB table={self.cratedb_table} count={records_target}")
            progress_bar = tqdm(total=records_in)
            records_out = 0

            for document in self.mongodb_collection.find().limit(self.mongodb_limit):
                try:
                    operation = self.translator.to_sql(document)
                    logger.debug("SQL operation: %s", operation)
                except Exception as ex:
                    logger_on_error(f"Transforming query failed: {ex}")
                    continue
                try:
                    result = connection.execute(sa.text(operation.statement), operation.parameters)
                    result_size = result.rowcount
                    records_out += result_size
                    progress_bar.update(n=result_size)
                except Exception as ex:
                    logger_on_error(f"Executing query failed: {ex}")

            progress_bar.close()
            connection.commit()
            logger.info(f"Number of records written: {records_out}")
            if records_out == 0:
                logger.warning("No data has been copied")
