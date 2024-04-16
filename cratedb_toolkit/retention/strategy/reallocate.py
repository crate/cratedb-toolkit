# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
A retention policy implementation which reallocates data to "warm" partitions.

It is derived from a corresponding tutorial based on Apache Airflow.
https://community.crate.io/t/cratedb-and-apache-airflow-building-a-hot-cold-storage-data-retention-policy/934

Prerequisites
-------------
- CrateDB 5.2.0 or later
- Tables for storing retention policies need to be created once manually in
  CrateDB. See the file setup/schema.sql in this repository.
"""

import dataclasses
import logging

from cratedb_toolkit.retention.model import RetentionTask

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ReallocateRetentionTask(RetentionTask):
    """
    Represent a data retention task, using the `reallocate` strategy.
    """

    def to_sql(self):
        """
        Render as SQL statement.
        """
        # FIXME: S608 Possible SQL injection vector through string-based query construction
        sql = f"""
        ALTER TABLE {self.table_fullname} PARTITION ({self.partition_column} = {self.partition_value})
        SET ("routing.allocation.require.{self.reallocation_attribute_name}" = '{self.reallocation_attribute_value}');
        """  # noqa: S608
        return sql


class ReallocateRetentionJob:
    """
    Represent a data retention job, using the `reallocate` strategy.
    """

    SQL = """
WITH partitions AS (
  {policy_dql}
),
partition_allocations AS (
  SELECT DISTINCT s.schema_name AS table_schema,
                  s.table_name,
                  s.partition_ident,
                  n.attributes
  FROM sys.shards s
  JOIN sys.nodes n ON s.node['id'] = n.id
)
SELECT *
FROM partitions p
JOIN partition_allocations a ON a.table_schema = p.table_schema
  AND a.table_name = p.table_name
  AND p.partition_ident = a.partition_ident
  AND attributes['{reallocation_attribute_name}'] <> '{reallocation_attribute_value}'
    """
