import base64
import json
import logging
import typing as t

import sqlalchemy as sa
from commons_codec.model import SkipOperation
from commons_codec.transform.aws_dms import DMSTranslatorCrateDB
from commons_codec.transform.dynamodb import DynamoDBCDCTranslator
from sqlalchemy.exc import OperationalError, ProgrammingError
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from yarl import URL

from cratedb_toolkit.io.exception import SkipAdapterException
from cratedb_toolkit.io.kinesis.adapter import KinesisAdapterBase, KinesisStreamAdapter
from cratedb_toolkit.io.kinesis.model import RecipeDefinition
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


class KinesisRelay:
    """
    Relay events from Kinesis into CrateDB.
    """

    def __init__(
        self,
        kinesis_url: str,
        cratedb_url: str,
        recipe: t.Union[RecipeDefinition, None] = None,
    ):
        self.cratedb_address = DatabaseAddress.from_string(cratedb_url)
        cratedb_sqlalchemy_url, cratedb_table_address = self.cratedb_address.decode()

        self.kinesis_url = URL(kinesis_url)
        self.kinesis_adapter = KinesisAdapterBase.factory(self.kinesis_url)
        self.cratedb_adapter = DatabaseAdapter(str(cratedb_sqlalchemy_url), echo=False)

        self.recipe = recipe

        self.cratedb_table = None
        if self.kinesis_url.scheme.startswith("kinesis+dynamodb"):
            cratedb_table_name = cratedb_table_address.fullname
            self.cratedb_table = self.cratedb_adapter.quote_relation_name(cratedb_table_name)
            self.translator = DynamoDBCDCTranslator(table_name=self.cratedb_table)
        elif self.kinesis_url.scheme == "kinesis+dms":
            pks, cms, mapping_strategy, ignore_ddl = None, None, None, None
            if self.recipe:
                pks, cms, mapping_strategy, ignore_ddl = self.recipe.codec_options()
            self.translator = DMSTranslatorCrateDB(
                primary_keys=pks,  # ty: ignore[invalid-argument-type]
                column_types=cms,  # ty: ignore[invalid-argument-type]
                mapping_strategy=mapping_strategy,  # ty: ignore[invalid-argument-type]
                ignore_ddl=ignore_ddl,  # ty: ignore[invalid-argument-type]
            )
        else:
            raise SkipAdapterException(f"Not processing {self.kinesis_url} here")

        self.connection: sa.Connection
        self.progress_bar: tqdm

    def _setup_checkpointer(self) -> None:
        """
        Create and wire a checkpointer into the Kinesis adapter, if configured.
        """
        if not isinstance(self.kinesis_adapter, KinesisStreamAdapter):
            return

        checkpointer_type = self.kinesis_adapter.checkpointer_type
        if checkpointer_type is None:
            logger.warning(
                "No persistent checkpointer configured; stream will resume from '%s' on next start.",
                self.kinesis_adapter.start,
            )
            return

        if checkpointer_type == "cratedb":
            from cratedb_toolkit.io.kinesis.checkpointer import CrateDBCheckPointer

            checkpointer = CrateDBCheckPointer(
                engine=self.cratedb_adapter.engine,
                name=self.kinesis_adapter.checkpointer_name,
                schema=self.kinesis_adapter.checkpointer_schema,
            )
        elif checkpointer_type == "memory":
            from kinesis.checkpointers import MemoryCheckPointer

            checkpointer = MemoryCheckPointer(name=self.kinesis_adapter.checkpointer_name)
        else:
            raise ValueError(f"Unsupported checkpointer type: {checkpointer_type!r}. Supported: memory, cratedb")

        logger.info(f"Using {checkpointer_type} checkpointer (name={self.kinesis_adapter.checkpointer_name})")
        self.kinesis_adapter.set_checkpointer(checkpointer)

    def start(self, once: bool = False):
        """
        Read events from Kinesis stream, convert to SQL statements, and submit to CrateDB.
        """
        logger.info(f"Source: Kinesis stream={self.kinesis_adapter.stream_name} count=unknown")
        self._setup_checkpointer()
        self.connection = self.cratedb_adapter.engine.connect()
        try:
            if self.cratedb_table is not None:
                if not self.cratedb_adapter.table_exists(self.cratedb_table):
                    assert self.translator and hasattr(self.translator, "sql_ddl")  # noqa: S101
                    self.connection.execute(sa.text(t.cast(str, self.translator.sql_ddl)))
                    self.connection.commit()
                records_target = self.cratedb_adapter.count_records(self.cratedb_table)
                logger.info(f"Target: CrateDB table={self.cratedb_table} count={records_target}")
            # Harmonize logging and progress bar.
            # https://github.com/tqdm/tqdm#redirecting-logging
            self.progress_bar = tqdm()
            with logging_redirect_tqdm():
                if once:
                    self.kinesis_adapter.consume_once(self.process_event)
                else:
                    self.kinesis_adapter.consume_forever(self.process_event)
        except Exception:
            self.stop()
            raise

    def stop(self):
        if hasattr(self, "progress_bar"):
            self.progress_bar.close()
        if hasattr(self, "connection"):
            try:
                self.connection.close()
            except Exception:
                logger.debug("Ignoring error while closing connection during cleanup")
            del self.connection
        if hasattr(self, "kinesis_adapter"):
            self.kinesis_adapter.stop()

    def process_event(self, event):
        logger.debug(f"Processing event={event}")
        data = event.get("kinesis", {}).get("data")
        if data:
            try:
                record = json.loads(base64.b64decode(data).decode("utf-8"))
            except Exception:
                logger.exception("Decoding Kinesis event failed")
                return
        else:
            record = event

        # If the user supplied a target schema, always use it when it is not a system schema.
        if self.cratedb_address.schema:
            metadata = record.setdefault("metadata", {})
            if metadata.get("schema-name") != "dms":
                metadata["schema-name"] = self.cratedb_address.schema

        try:
            operation = self.translator.to_sql(record)
        except SkipOperation:
            return
        except Exception:
            logger.exception("Translating CDC event to SQL failed")
            return

        try:
            # Process record.
            self.connection.execute(sa.text(operation.statement), operation.parameters)

            # Processing alternating CDC events requires write synchronization.
            # FIXME: With DMS, multiple tables need to be taken into account.
            if self.cratedb_table:
                self.connection.execute(sa.text(f"REFRESH TABLE {self.cratedb_table}"))

            self.connection.commit()
        except (ProgrammingError, OperationalError):
            logger.exception("Executing query failed")
            raise
        else:
            self.progress_bar.update()

    def __del__(self):
        self.stop()
