"""
Basic relaying of a MongoDB Change Stream into CrateDB table.

Documentation:
- https://github.com/daq-tools/commons-codec/blob/main/doc/mongodb.md
- https://www.mongodb.com/docs/manual/changeStreams/
- https://www.mongodb.com/developer/languages/python/python-change-streams/
"""

import logging

import pymongo
import sqlalchemy as sa
from commons_codec.transform.mongodb import MongoDBCDCTranslatorCrateDB

from cratedb_toolkit.util import DatabaseAdapter

logger = logging.getLogger(__name__)


class MongoDBCDCRelayCrateDB:
    """
    Relay MongoDB Change Stream into CrateDB table.
    """

    def __init__(
        self,
        mongodb_url: str,
        mongodb_database: str,
        mongodb_collection: str,
        cratedb_sqlalchemy_url: str,
        cratedb_table: str,
    ):
        self.cratedb_adapter = DatabaseAdapter(cratedb_sqlalchemy_url, echo=True)
        self.mongodb_client: pymongo.MongoClient = pymongo.MongoClient(mongodb_url)
        self.mongodb_collection = self.mongodb_client[mongodb_database][mongodb_collection]
        self.table_name = self.cratedb_adapter.quote_relation_name(cratedb_table)
        self.cdc = MongoDBCDCTranslatorCrateDB(table_name=self.table_name)

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
        # Note that `.watch()` will block until events are ready for consumption, so
        # this is not a busy loop.
        # FIXME: Note that the function does not perform any sensible error handling yet.
        while True:
            with self.mongodb_collection.watch(full_document="updateLookup") as change_stream:
                for change in change_stream:
                    yield self.cdc.to_sql(change)
