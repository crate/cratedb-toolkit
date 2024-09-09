# ruff: noqa: S608
import logging
import typing as t

import sqlalchemy as sa
from boltons.urlutils import URL
from commons_codec.model import SQLOperation
from commons_codec.transform.mongodb import MongoDBCDCTranslatorCrateDB
from pymongo.cursor import Cursor
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from zyp.model.collection import CollectionAddress

from cratedb_toolkit.io.mongodb.adapter import mongodb_adapter_factory
from cratedb_toolkit.io.mongodb.export import CrateDBConverter
from cratedb_toolkit.io.mongodb.model import DocumentDict
from cratedb_toolkit.io.mongodb.transform import TransformationManager
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util import DatabaseAdapter

logger = logging.getLogger(__name__)


class MongoDBFullLoadTranslator(MongoDBCDCTranslatorCrateDB):
    """
    Translate a MongoDB document into a CrateDB document.
    """

    def __init__(self, table_name: str, converter: CrateDBConverter, tm: TransformationManager = None):
        super().__init__(table_name=table_name)
        self.converter = converter
        self.tm = tm

    @staticmethod
    def get_document_key(record: t.Dict[str, t.Any]) -> str:
        """
        Return value of document key (MongoDB document OID) from CDC record.

        "documentKey": {"_id": ObjectId("669683c2b0750b2c84893f3e")}
        """
        return record["_id"]

    def to_sql(self, data: t.Union[DocumentDict, t.List[DocumentDict]]) -> SQLOperation:
        """
        Produce CrateDB SQL INSERT batch operation from multiple MongoDB documents.
        """
        if isinstance(data, Cursor):
            data = list(data)
        if not isinstance(data, list):
            data = [data]

        # Define SQL INSERT statement.
        sql = f"INSERT INTO {self.table_name} ({self.ID_COLUMN}, {self.DATA_COLUMN}) VALUES (:oid, :record);"

        # Converge multiple MongoDB documents into SQL parameters for `executemany` operation.
        parameters: t.List[DocumentDict] = []
        for document in data:
            record = self.converter.convert(self.decode_bson(document))
            oid: str = self.get_document_key(record)
            parameters.append({"oid": oid, "record": record})

        return SQLOperation(sql, parameters)


class MongoDBFullLoad:
    """
    Copy MongoDB collection into CrateDB table.
    """

    def __init__(
        self,
        mongodb_url: str,
        cratedb_url: str,
        tm: t.Union[TransformationManager, None],
        on_error: t.Literal["ignore", "raise"] = "raise",
        progress: bool = False,
        debug: bool = True,
    ):
        # Decode database URL: MongoDB.
        self.mongodb_uri = URL(mongodb_url)
        self.mongodb_adapter = mongodb_adapter_factory(self.mongodb_uri)

        # Decode database URL: CrateDB.
        cratedb_address = DatabaseAddress.from_string(cratedb_url)
        cratedb_sqlalchemy_url, cratedb_table_address = cratedb_address.decode()
        cratedb_table = cratedb_table_address.fullname

        self.cratedb_adapter = DatabaseAdapter(str(cratedb_sqlalchemy_url), echo=False)
        self.cratedb_table = self.cratedb_adapter.quote_relation_name(cratedb_table)

        # Transformation machinery.
        transformation = None
        if tm:
            address = CollectionAddress(
                container=self.mongodb_adapter.database_name, name=self.mongodb_adapter.collection_name
            )
            try:
                transformation = tm.project.get(address=address)
            except KeyError:
                pass
        self.converter = CrateDBConverter(transformation=transformation)
        self.translator = MongoDBFullLoadTranslator(table_name=self.cratedb_table, converter=self.converter, tm=tm)

        self.on_error = on_error
        self.progress = progress
        self.debug = debug

    def start(self):
        """
        Read items from DynamoDB table, convert to SQL INSERT statements, and submit to CrateDB.
        """
        records_in = self.mongodb_adapter.record_count()
        logger.info(f"Source: MongoDB collection={self.mongodb_adapter.collection_name} count={records_in}")
        logger_on_error = logger.warning
        if self.debug:
            logger_on_error = logger.exception
        with self.cratedb_adapter.engine.connect() as connection, logging_redirect_tqdm():
            if not self.cratedb_adapter.table_exists(self.cratedb_table):
                connection.execute(sa.text(self.translator.sql_ddl))
                connection.commit()
            records_target = self.cratedb_adapter.count_records(self.cratedb_table)
            logger.info(f"Target: CrateDB table={self.cratedb_table} count={records_target}")
            progress_bar = tqdm(total=records_in)
            records_out: int = 0

            # Acquire batches of documents, convert to SQL operations, and submit to CrateDB.
            for documents in self.mongodb_adapter.query():
                progress_bar.set_description("ACQUIRE")

                try:
                    operation = self.translator.to_sql(documents)
                except Exception as ex:
                    logger_on_error(f"Computing query failed: {ex}")
                    if self.on_error == "raise":
                        raise
                    continue

                # Submit operation to CrateDB.
                progress_bar.set_description("SUBMIT ")
                try:
                    result = connection.execute(sa.text(operation.statement), operation.parameters)
                    result_size = result.rowcount
                    if result_size < 0:
                        raise ValueError("Unable to insert one or more records")
                    records_out += result_size
                    progress_bar.update(n=result_size)
                except Exception as ex:
                    logger_on_error(f"Executing operation failed: {ex}\nOperation:\n{operation}")
                    if self.on_error == "raise":
                        raise
                    continue

            progress_bar.close()
            connection.commit()
            logger.info(f"Number of records written: {records_out}")
            if records_out == 0:
                logger.warning("No data has been copied")
