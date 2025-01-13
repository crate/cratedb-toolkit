# ruff: noqa: S608
import logging
import typing as t

import sqlalchemy as sa
from boltons.urlutils import URL
from commons_codec.transform.mongodb import MongoDBCrateDBConverter, MongoDBFullLoadTranslator
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from zyp.model.collection import CollectionAddress

from cratedb_toolkit.io.core import BulkProcessor
from cratedb_toolkit.io.mongodb.adapter import mongodb_adapter_factory
from cratedb_toolkit.io.mongodb.transform import TransformationManager
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util import DatabaseAdapter

logger = logging.getLogger(__name__)


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
                logger.info(f"Applying transformation to: {address}")
            except KeyError:
                logger.warning(f"No transformation found for: {address}")
        self.converter = MongoDBCrateDBConverter(
            timestamp_to_epoch=True,
            timestamp_use_milliseconds=True,
            transformation=transformation,
        )
        self.translator = MongoDBFullLoadTranslator(table_name=self.cratedb_table, converter=self.converter)

        self.on_error = on_error
        self.progress = progress
        self.debug = debug

    def start(self):
        """
        Read items from MongoDB table, convert to SQL INSERT statements, and submit to CrateDB.
        """
        logger.info(f"Starting MongoDBFullLoad. source={self.mongodb_uri}, target={self.cratedb_uri}")
        limit = self.mongodb_adapter.limit
        if limit > 0:
            records_in = limit
        else:
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
                f"Number of records written: success={metrics.count_success_total}, error={metrics.count_error_total}"
            )
            if metrics.count_success_total == 0:
                logger.warning("No data has been copied")

        return True
