"""
Implements a retention policy by dropping expired partitions

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913

Prerequisites
-------------
In CrateDB, tables for storing retention policies need to be created once manually.
See the file setup/schema.sql in this repository.
"""
import dataclasses
import logging
from importlib.resources import read_text

from cratedb_rollup.util.database import run_sql

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DeleteAction:
    """
    Manage metadata representing a data retention operation on a single table.
    """

    table_fqn: str
    column: str
    value: int

    @classmethod
    def from_record(cls, record):
        """
        Factory for creating instance from database record.
        """
        return cls(**cls.record_mapper(record))

    def to_sql(self):
        """
        Render as SQL statement.
        """
        # FIXME: S608 Possible SQL injection vector through string-based query construction
        sql = f"""DELETE FROM {self.table_fqn} WHERE {self.column} = {self.value};"""  # noqa: S608
        return sql

    @staticmethod
    def record_mapper(record):
        """
        Map database record to instance attributes.
        """
        return {
            "table_fqn": record[0],
            "column": record[1],
            "value": record[2],
        }


@dataclasses.dataclass
class DeleteRetention:
    """
    Represent a complete data retention job, using the `delete` strategy.
    """

    # Database connection URI.
    dburi: str

    # Retention cutoff timestamp.
    day: str

    def start(self):
        """
        Evaluate retention policies, and invoke actions.
        """
        for policy in self.get_policies():
            logger.info(f"Executing data retention: {self}")
            sql = policy.to_sql()

            logger.info(f"Running data retention SQL statement: {sql}")
            run_sql(dburi=self.dburi, sql=sql)

    def get_policies(self):
        """
        Resolve retention policy items.
        """
        # Read SQL DDL statement.
        sql = read_text("cratedb_rollup.strategy", "delete_policies.sql")
        sql = sql.format(day=self.day)

        # Resolve retention policies.
        policy_records = run_sql(self.dburi, sql)
        logger.info(f"Policies: {policy_records}")
        for record in policy_records:
            policy = DeleteAction.from_record(record)
            yield policy


def run_delete_job(dburi: str, day: str):
    """
    Invoke data retention using `delete` strategy.
    """

    ret = DeleteRetention(dburi=dburi, day=day)
    ret.start()
