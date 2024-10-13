"""
Relay a MongoDB Change Stream into a CrateDB table.

Documentation:
- https://www.mongodb.com/docs/manual/changeStreams/
- https://www.mongodb.com/developer/languages/python/python-change-streams/
- https://github.com/daq-tools/commons-codec/blob/main/doc/mongodb.md
"""

import logging
import typing as t

import pymongo
import pymongo.errors
import sqlalchemy as sa
from boltons.urlutils import URL
from commons_codec.transform.mongodb import MongoDBCDCTranslator, MongoDBCrateDBConverter
from pymongo.change_stream import CollectionChangeStream
from zyp.model.collection import CollectionAddress

from cratedb_toolkit.io.mongodb.adapter import mongodb_adapter_factory
from cratedb_toolkit.io.mongodb.model import DocumentDict
from cratedb_toolkit.io.mongodb.transform import TransformationManager
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.util.process import FixedBackoff

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

        logger.info(f"Initializing MongoDB CDC Relay. mongodb={mongodb_url}, cratedb={cratedb_url}")

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
        self.ccs: CollectionChangeStream

        self.on_error = on_error
        self.debug = debug
        self.stopping: bool = False

    def start(self):
        """
        Subscribe to change stream events, convert to SQL, and submit to CrateDB.
        """
        # FIXME: Note that the function does not perform any sensible error handling yet.
        with self.cratedb_adapter.engine.connect() as connection:
            connection.execute(sa.text(self.cdc.sql_ddl))
            for event in self.consume():
                operation = self.cdc.to_sql(event)
                if operation:
                    connection.execute(sa.text(operation.statement), operation.parameters)

    def stop(self):
        self.stopping = True
        self.ccs._closed = True

    def consume(self, resume_after: t.Optional[DocumentDict] = None):
        """
        Subscribe to change stream events, and emit change events.
        """
        self.ccs = self.mongodb_adapter.subscribe_cdc(resume_after=resume_after)

        if self.stopping:
            return

        backoff = FixedBackoff(sequence=[2, 4, 6, 8, 10])
        resume_token = None
        try:
            with self.ccs as stream:
                for event in stream:
                    yield event
                    resume_token = stream.resume_token
                    backoff.reset()
        except pymongo.errors.PyMongoError:
            # The ChangeStream encountered an unrecoverable error or the
            # resume attempt failed to recreate the cursor.
            if resume_token is None:
                # There is no usable resume token because there was a
                # failure during ChangeStream initialization.
                logger.exception("Initializing change stream failed")
            else:
                # Use the interrupted ChangeStream's resume token to create
                # a new ChangeStream. The new stream will continue from the
                # last seen insert change without missing any events.
                backoff.next()
                logger.info("Resuming change stream")
                self.consume(resume_after=resume_after)
