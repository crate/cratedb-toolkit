# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
Implements a retention policy by reallocating cold partitions

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-building-a-hot-cold-storage-data-retention-policy/934

Prerequisites
-------------
- CrateDB 5.2.0 or later
- Tables for storing retention policies need to be created once manually in
  CrateDB. See the file setup/schema.sql in this repository.
"""
import dataclasses
import logging
from importlib.resources import read_text

from cratedb_rollup.util.database import run_sql

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ReallocateAction:
    """
    Manage metadata representing a data retention operation on a single table.
    """

    schema: str
    table: str
    table_fqn: str
    column: str
    value: str
    attribute_name: str
    attribute_value: str

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
        sql = f"""
            ALTER TABLE {self.table_fqn} PARTITION ({self.column} = {self.value})
            SET ("routing.allocation.require.{self.attribute_name}" = '{self.attribute_value}');
        """  # noqa: S608
        return sql

    @staticmethod
    def record_mapper(record):
        """
        Map database record to instance attributes.
        """
        return {
            "schema": record[0],
            "table": record[1],
            "table_fqn": record[2],
            "column": record[3],
            "value": record[4],
            "attribute_name": record[5],
            "attribute_value": record[6],
        }


@dataclasses.dataclass
class ReallocateRetention:
    """
    Represent a complete data retention job, using the `delete` strategy.
    """

    # Database connection URI.
    dburi: str

    # Retention cutoff timestamp.
    cutoff_day: str

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
        sql = read_text("cratedb_rollup.strategy", "reallocate_tasks.sql")
        sql = sql.format(day=self.cutoff_day)

        # Resolve retention policies.
        policy_records = run_sql(self.dburi, sql)
        logger.info(f"Policies: {policy_records}")
        for record in policy_records:
            policy = ReallocateAction.from_record(record)
            yield policy


def run_reallocate_job(dburi: str, cutoff_day: str):
    """
    Invoke data retention using the `reallocate` strategy.
    """

    ret = ReallocateRetention(dburi=dburi, cutoff_day=cutoff_day)
    ret.start()
