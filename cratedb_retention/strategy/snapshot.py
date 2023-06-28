# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
Implements a retention policy by snapshotting expired partitions to a repository

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-building-a-data-retention-policy-using-external-snapshot-repositories/1001

Prerequisites
-------------
In CrateDB, tables for storing retention policies need to be created once manually.
See the file setup/schema.sql in this repository.
"""
import dataclasses
import logging

from cratedb_retention.model import GenericAction, GenericRetention

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SnapshotAction(GenericAction):
    """
    Manage metadata representing a data retention operation on a single table.
    """

    schema: str
    table: str
    table_fqn: str
    column: str
    value: str
    target_repository_name: str

    def to_sql(self):
        """
        Render as SQL statement.
        """
        # FIXME: S608 Possible SQL injection vector through string-based query construction
        sql = f"""
            CREATE SNAPSHOT {self.target_repository_name}."{self.schema}.{self.table}-{self.value}"
            TABLE {self.table_fqn} PARTITION ({self.column} = {self.value})
            WITH ("wait_for_completion" = true);
        """  # noqa: S608
        sql2 = f"""
        DELETE FROM {self.schema}.{self.table} WHERE {self.column} = {self.value};
        """  # noqa: S608
        return [sql, sql2]

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
            "target_repository_name": record[5],
        }


@dataclasses.dataclass
class SnapshotRetention(GenericRetention):
    """
    Represent a complete data retention job, using the `snapshot` strategy.
    """

    _tasks_sql = "snapshot_tasks.sql"
    _action_class = SnapshotAction
