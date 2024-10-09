"""
Basic relaying of a MongoDB Change Stream into CrateDB table.

Documentation:
- https://github.com/daq-tools/commons-codec/blob/main/doc/mongodb.md
- https://www.mongodb.com/docs/manual/changeStreams/
- https://www.mongodb.com/developer/languages/python/python-change-streams/
"""

import logging
import typing as t

import sqlalchemy as sa
from boltons.urlutils import URL
from commons_codec.transform.mongodb import MongoDBCDCTranslator, MongoDBCrateDBConverter
from zyp.model.collection import CollectionAddress

from cratedb_toolkit.io.mongodb.adapter import mongodb_adapter_factory
from cratedb_toolkit.io.mongodb.transform import TransformationManager
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util import DatabaseAdapter

logger = logging.getLogger(__name__)


class MongoDBCDCRelayCrateDB:
    """
    Relay MongoDB Change Stream into CrateDB table.
    """

    def __init__(
        self,
        mongodb_url: t.Union[str, URL],
        cratedb_url: t.Union[str, URL],
        tm: t.Union[TransformationManager, None],
        on_error: t.Literal["ignore", "raise"] = "ignore",
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

        self.cdc = MongoDBCDCTranslator(table_name=self.cratedb_table, converter=self.converter)

        self.on_error = on_error
        self.debug = debug

    def start(self):
        """
        Subscribe to change stream events, convert to SQL, and submit to CrateDB.
        """
        # FIXME: Note that the function does not perform any sensible error handling yet.
        with self.cratedb_adapter.engine.connect() as connection:
            connection.execute(sa.text(self.cdc.sql_ddl))
            for operation in self.cdc_to_sql():
                if operation:
                    connection.execute(sa.text(operation.statement), operation.parameters)

    def cdc_to_sql(self):
        """
        Subscribe to change stream events, and emit corresponding SQL statements.
        """
        # Note that `.subscribe()` (calling `.watch()`) will block until events are ready
        # for consumption, so this is not a busy loop.
        # FIXME: Note that the function does not perform any sensible error handling yet.
        while True:
            with self.mongodb_adapter.subscribe() as change_stream:
                for change in change_stream:
                    yield self.cdc.to_sql(change)
