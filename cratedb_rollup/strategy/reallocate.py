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

from cratedb_rollup.model import GenericAction, GenericRetention

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ReallocateAction(GenericAction):
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
class ReallocateRetention(GenericRetention):
    """
    Represent a complete data retention job, using the `reallocate` strategy.
    """

    _tasks_sql = "reallocate_tasks.sql"
    _action_class = ReallocateAction
