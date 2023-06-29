# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
Example program demonstrating how to create and retrieve data
retention policy records.

It initializes the data retention and expiry subsystem, and creates
and deletes a few data retention policies.

The program obtains a single positional argument from the command line,
the database URI, in SQLAlchemy-compatible string format.
"""
import logging

from cratedb_retention.model import DatabaseAddress, JobSettings
from cratedb_retention.setup.schema import setup_schema
from cratedb_retention.store import RetentionPolicyStore
from cratedb_retention.util.cli import boot_with_dburi
from cratedb_retention.util.data import jd
from cratedb_retention.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


class EditExample:
    def __init__(self, dburi):
        self.dburi = dburi
        self.db = DatabaseAdapter(dburi=self.dburi)

        # Create `JobSettings` instance.
        # It is the single source of truth about configuration and runtime settings.
        self.settings = JobSettings(database=DatabaseAddress.from_string(self.dburi))
        self.settings.policy_table.schema = "examples"

        self.store = RetentionPolicyStore(settings=self.settings)

    def setup(self):
        """
        Create the SQL DDL schema.
        """
        # Run SQL DDL statement.
        setup_schema(settings=self.settings)

    def list_policies(self):
        """
        Query and return all retention policies from database.
        """
        return self.store.get_records()

    def add_policy_basic(self):
        """
        Add a basic retention policy.
        """
        # TODO: All values are currently hardcoded.
        sql = f"""
        -- A policy using the DELETE strategy.
        INSERT INTO {self.settings.policy_table.fullname}
          (strategy, table_schema, table_name, partition_column, retention_period)
        VALUES
          ('delete', 'doc', 'raw_metrics', 'ts_day', 1);
        """
        self.db.run_sql(sql, ignore="DuplicateKeyException")

    def add_policy_tags(self):
        """
        Add a retention policy using tags.
        """
        # TODO: All values are currently hardcoded.
        sql = f"""
        -- A policy using the DELETE strategy, with tags.
        INSERT INTO {self.settings.policy_table.fullname}
          (strategy, tags, table_schema, table_name, partition_column, retention_period)
        VALUES
          ('snapshot', {{foo='true', bar='true'}}, 'doc', 'raw_metrics', 'ts_day', 90);
        """
        self.db.run_sql(sql, ignore="DuplicateKeyException")

    def delete_policy_by_tag(self):
        """
        Delete the policy created by the previous function.
        """
        sql = f"DELETE FROM {self.settings.policy_table.fullname} WHERE tags['foo'] IS NOT NULL;"  # noqa: S608
        self.db.run_sql(sql)


def main(dburi: str):
    """
    Create different data retention policies, and list them.
    """

    logger.info("Running `EditExample` application")
    ctx = EditExample(dburi=dburi)
    ctx.setup()

    logger.info("Create two policies, and list them")
    ctx.add_policy_basic()
    ctx.add_policy_tags()
    jd(ctx.list_policies())

    logger.info("Delete one policy again, and list all remaining")
    ctx.delete_policy_by_tag()
    jd(ctx.list_policies())


if __name__ == "__main__":
    """
    The program obtains a single positional argument from the command line,
    the database URI, in SQLAlchemy-compatible string format.
    """
    dburi = boot_with_dburi()
    main(dburi)
