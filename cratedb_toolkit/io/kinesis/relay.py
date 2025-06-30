import base64
import json
import logging
import typing as t

import sqlalchemy as sa
from commons_codec.model import SkipOperation
from commons_codec.transform.aws_dms import DMSTranslatorCrateDB
from commons_codec.transform.dynamodb import DynamoDBCDCTranslator
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from yarl import URL

from cratedb_toolkit.io.kinesis.adapter import KinesisAdapterBase
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
                primary_keys=pks, column_types=cms, mapping_strategy=mapping_strategy, ignore_ddl=ignore_ddl
            )
        else:
            raise NotImplementedError(f"Data processing not implemented for {self.kinesis_url}")

        self.connection: sa.Connection
        self.progress_bar: tqdm

    def start(self, once: bool = False):
        """
        Read events from Kinesis stream, convert to SQL statements, and submit to CrateDB.
        """
        logger.info(f"Source: Kinesis stream={self.kinesis_adapter.stream_name} count=unknown")
        self.connection = self.cratedb_adapter.engine.connect()
        if self.cratedb_table is not None:
            if not self.cratedb_adapter.table_exists(self.cratedb_table):
                self.connection.execute(sa.text(self.translator.sql_ddl))
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

    def stop(self):
        if hasattr(self, "progress_bar"):
            self.progress_bar.close()
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
        except sa.exc.ProgrammingError as ex:
            logger.error(f"Executing query failed: {ex}")
        self.progress_bar.update()

    def __del__(self):
        self.stop()
