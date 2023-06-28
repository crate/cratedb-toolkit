# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
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

from cratedb_rollup.model import GenericAction, GenericRetention

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DeleteAction(GenericAction):
    """
    Manage metadata representing a data retention operation on a single table.
    """

    table_fqn: str
    column: str
    value: int

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
class DeleteRetention(GenericRetention):
    """
    Represent a complete data retention job, using the `delete` strategy.
    """

    _tasks_sql = "delete_tasks.sql"
    _action_class = DeleteAction
