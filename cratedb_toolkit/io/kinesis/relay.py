import base64
import json
import logging

import sqlalchemy as sa
from commons_codec.transform.dynamodb import DynamoDBCDCTranslator
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from yarl import URL

from cratedb_toolkit.io.kinesis.adapter import KinesisAdapter
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util import DatabaseAdapter

logger = logging.getLogger(__name__)


class KinesisRelay:
    """
    Relay events from Kinesis into CrateDB table.
    """

    def __init__(
        self,
        kinesis_url: str,
        cratedb_url: str,
    ):
        cratedb_address = DatabaseAddress.from_string(cratedb_url)
        cratedb_sqlalchemy_url, cratedb_table_address = cratedb_address.decode()
        cratedb_table = cratedb_table_address.fullname

        self.kinesis_url = URL(kinesis_url)
        self.kinesis_adapter = KinesisAdapter(self.kinesis_url)
        self.cratedb_adapter = DatabaseAdapter(str(cratedb_sqlalchemy_url), echo=False)
        self.cratedb_table = self.cratedb_adapter.quote_relation_name(cratedb_table)

        if "dynamodb+cdc" in self.kinesis_url.scheme:
            self.translator = DynamoDBCDCTranslator(table_name=self.cratedb_table)
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
        try:
            record = json.loads(base64.b64decode(event["kinesis"]["data"]).decode("utf-8"))
            operation = self.translator.to_sql(record)
        except Exception:
            logger.exception("Decoding Kinesis event failed")
            return
        try:
            # Process record.
            self.connection.execute(sa.text(operation.statement), operation.parameters)

            # Processing alternating CDC events requires write synchronization.
            self.connection.execute(sa.text(f"REFRESH TABLE {self.cratedb_table}"))

            self.connection.commit()
        except sa.exc.ProgrammingError as ex:
            logger.exception(f"Executing query failed: {ex}")
        self.progress_bar.update()

    def __del__(self):
        self.stop()
