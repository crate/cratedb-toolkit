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

from cratedb_retention.model import GenericRetention, RetentionPolicy

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ReallocateAction(RetentionPolicy):
    """
    Manage metadata representing a data retention operation on a single table.
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


@dataclasses.dataclass
class ReallocateRetention(GenericRetention):
    """
    Represent a complete data retention job, using the `reallocate` strategy.
    """

    _tasks_sql = "reallocate_tasks.sql"
    _action_class = ReallocateAction
