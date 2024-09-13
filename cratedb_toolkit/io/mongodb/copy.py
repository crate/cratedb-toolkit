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

from cratedb_toolkit.io.core import BulkProcessor
from cratedb_toolkit.io.mongodb.adapter import mongodb_adapter_factory
from cratedb_toolkit.io.mongodb.export import CrateDBConverter
from cratedb_toolkit.io.mongodb.model import DocumentDict
from cratedb_toolkit.io.mongodb.transform import TransformationManager
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.sqlalchemy.patch import monkeypatch_executemany
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
        if not isinstance(data, Cursor) and not isinstance(data, list):
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
        mongodb_url: t.Union[str, URL],
        cratedb_url: t.Union[str, URL],
        tm: t.Union[TransformationManager, None],
        on_error: t.Literal["ignore", "raise"] = "ignore",
        progress: bool = False,
        debug: bool = True,
    ):
        monkeypatch_executemany()

        self.mongodb_uri = URL(mongodb_url)
        self.cratedb_uri = URL(cratedb_url)

        # Decode database URL: MongoDB.
        self.mongodb_adapter = mongodb_adapter_factory(self.mongodb_uri)

        # Decode database URL: CrateDB.
        self.cratedb_address = DatabaseAddress(self.cratedb_uri)
        self.cratedb_sqlalchemy_url, self.cratedb_table_address = self.cratedb_address.decode()
        cratedb_table = self.cratedb_table_address.fullname

        self.cratedb_adapter = DatabaseAdapter(str(self.cratedb_sqlalchemy_url), echo=False)
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
        Read items from MongoDB table, convert to SQL INSERT statements, and submit to CrateDB.
        """
        logger.info(f"Starting MongoDBFullLoad. source={self.mongodb_uri}, target={self.cratedb_uri}")
        records_in = self.mongodb_adapter.record_count()
        logger.info(f"Source: MongoDB {self.mongodb_adapter.address} count={records_in}")
        with self.cratedb_adapter.engine.connect() as connection, logging_redirect_tqdm():
            if not self.cratedb_adapter.table_exists(self.cratedb_table):
                connection.execute(sa.text(self.translator.sql_ddl))
                connection.commit()
            records_target = self.cratedb_adapter.count_records(self.cratedb_table)
            logger.info(f"Target: CrateDB table={self.cratedb_table} count={records_target}")
            progress_bar = tqdm(total=records_in)

            processor = BulkProcessor(
                connection=connection,
                data=self.mongodb_adapter.query(),
                batch_to_operation=self.translator.to_sql,
                progress_bar=progress_bar,
                on_error=self.on_error,
                debug=self.debug,
            )
            metrics = processor.start()
            logger.info(f"Bulk processor metrics: {metrics}")

            logger.info(
                "Number of records written: "
                f"success={metrics.count_success_total}, error={metrics.count_error_total}"
            )
            if metrics.count_success_total == 0:
                logger.warning("No data has been copied")

        return True
