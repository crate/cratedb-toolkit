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

from cratedb_toolkit.retention.model import RetentionTask

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SnapshotRetentionTask(RetentionTask):
    """
    Represent a data retention task, using the `snapshot` strategy.
    """

    def to_sql(self):
        """
        Render as SQL statement.
        """
        # FIXME: S608 Possible SQL injection vector through string-based query construction
        sql = f"""
        CREATE SNAPSHOT "{self.target_repository_name}"."{self.table_schema}.{self.table_name}-{self.partition_value}"
        TABLE {self.table_fullname} PARTITION ({self.partition_column} = {self.partition_value})
        WITH ("wait_for_completion" = true);
        """  # noqa: S608
        sql2 = f"""
        DELETE FROM {self.table_fullname} WHERE {self.partition_column} = {self.partition_value};
        """  # noqa: S608
        return [sql, sql2]
