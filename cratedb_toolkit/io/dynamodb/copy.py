# ruff: noqa: S608
import logging
import typing as t

import sqlalchemy as sa
from commons_codec.transform.dynamodb import DynamoDBFullLoadTranslator
from tqdm import tqdm
from yarl import URL

from cratedb_toolkit.io.core import BulkProcessor
from cratedb_toolkit.io.dynamodb.adapter import DynamoDBAdapter
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.util.data import asbool

logger = logging.getLogger(__name__)


class DynamoDBFullLoad:
    """
    Copy DynamoDB table into CrateDB table.
    """

    def __init__(
        self,
        dynamodb_url: str,
        cratedb_url: str,
        on_error: t.Literal["ignore", "raise"] = "ignore",
        progress: bool = False,
        debug: bool = True,
    ):
        cratedb_address = DatabaseAddress.from_string(cratedb_url)
        cratedb_sqlalchemy_url, cratedb_table_address = cratedb_address.decode()
        cratedb_table = cratedb_table_address.fullname

        self.dynamodb_url = URL(dynamodb_url)
        self.dynamodb_adapter = DynamoDBAdapter(self.dynamodb_url)
        self.dynamodb_table = self.dynamodb_url.path.lstrip("/")
        self.cratedb_adapter = DatabaseAdapter(str(cratedb_sqlalchemy_url), echo=False)
        self.cratedb_table = self.cratedb_adapter.quote_relation_name(cratedb_table)

        self.on_error = on_error
        self.progress = progress
        self.debug = debug

        self.batch_size: int = int(self.dynamodb_url.query.get("batch-size", 100))
        self.consistent_read: bool = asbool(self.dynamodb_url.query.get("consistent-read", False))

    def start(self):
        """
        Read items from DynamoDB table, convert to SQL INSERT statements, and submit to CrateDB.
        """
        records_in = self.dynamodb_adapter.count_records(table_name=self.dynamodb_table)
        logger.info(f"Source: DynamoDB table={self.dynamodb_table} count={records_in}")

        primary_key_schema = self.dynamodb_adapter.primary_key_schema(table_name=self.dynamodb_table)
        translator = DynamoDBFullLoadTranslator(table_name=self.cratedb_table, primary_key_schema=primary_key_schema)

        with self.cratedb_adapter.engine.connect() as connection:
            if not self.cratedb_adapter.table_exists(self.cratedb_table):
                connection.execute(sa.text(translator.sql_ddl))
                connection.commit()
            records_target = self.cratedb_adapter.count_records(self.cratedb_table)
            logger.info(f"Target: CrateDB table={self.cratedb_table} count={records_target}")
            progress_bar = tqdm(total=records_in)

            processor = BulkProcessor(
                connection=connection,
                data=self.fetch(),
                batch_to_operation=translator.to_sql,
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

    def fetch(self) -> t.Generator[t.List[t.Dict[str, t.Any]], None, None]:
        """
        Fetch data from DynamoDB. Generate batches of items.
        """
        data = self.dynamodb_adapter.scan(
            table_name=self.dynamodb_table,
            consistent_read=self.consistent_read,
            batch_size=self.batch_size,
        )
        for result in data:
            yield result["Items"]
