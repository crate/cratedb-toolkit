# ruff: noqa: S608
import logging

import sqlalchemy as sa
from commons_codec.transform.dynamodb import DynamoDBFullLoadTranslator
from tqdm import tqdm
from yarl import URL

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
        self.translator = DynamoDBFullLoadTranslator(table_name=self.cratedb_table)

        self.progress = progress
        self.debug = debug

        self.page_size: int = int(self.dynamodb_url.query.get("page-size", 1000))
        self.consistent_read: bool = asbool(self.dynamodb_url.query.get("consistent-read", False))

    def start(self):
        """
        Read items from DynamoDB table, convert to SQL INSERT statements, and submit to CrateDB.
        """
        records_in = self.dynamodb_adapter.count_records(self.dynamodb_table)
        logger.info(f"Source: DynamoDB table={self.dynamodb_table} count={records_in}")
        logger_on_error = logger.warning
        if self.debug:
            logger_on_error = logger.exception
        with self.cratedb_adapter.engine.connect() as connection:
            if not self.cratedb_adapter.table_exists(self.cratedb_table):
                connection.execute(sa.text(self.translator.sql_ddl))
                connection.commit()
            records_target = self.cratedb_adapter.count_records(self.cratedb_table)
            logger.info(f"Target: CrateDB table={self.cratedb_table} count={records_target}")
            progress_bar = tqdm(total=records_in)
            records_out = 0
            for result in self.dynamodb_adapter.scan(
                table_name=self.dynamodb_table,
                consistent_read=self.consistent_read,
                page_size=self.page_size,
            ):
                result_size = len(result["Items"])
                try:
                    operation = self.translator.to_sql(result["Items"])
                except Exception as ex:
                    logger_on_error(f"Transforming query failed: {ex}")
                    continue
                try:
                    connection.execute(sa.text(operation.statement), operation.parameters)
                    records_out += result_size
                    progress_bar.update(n=result_size)
                except Exception as ex:
                    logger_on_error(f"Executing query failed: {ex}")
            progress_bar.close()
            connection.commit()
            logger.info(f"Number of records written: {records_out}")
            if records_out == 0:
                logger.warning("No data has been copied")
