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

from cratedb_toolkit.retention.model import RetentionTask

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DeleteRetentionTask(RetentionTask):
    """
    Represent a data retention task, using the `delete` strategy.
    """

    def to_sql(self):
        """
        Render as SQL statement.
        """
        # FIXME: S608 Possible SQL injection vector through string-based query construction
        sql = f"""
            DELETE FROM {self.table_fullname}
            WHERE {self.partition_column} = {self.partition_value};
        """  # noqa: S608
        return sql
