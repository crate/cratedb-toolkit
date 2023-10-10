# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
About
=====

Example program demonstrating how to manipulate retention policy records.

It initializes the data retention and expiry subsystem, and creates and
deletes a few data retention policies.

The program obtains a single positional argument from the command line,
the database URI, in SQLAlchemy-compatible string format. By default,
the program connects to a CrateDB instance on localhost.

Synopsis
========
::

    # Install package
    pip install cratedb-toolkit

    # General.
    python examples/retention_edit.py crate://<USERNAME>:<PASSWORD>@<HOSTNAME>:4200?ssl=true

    # Default.
    python examples/retention_edit.py crate://localhost:4200

"""
import logging
import os

from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.retention.model import JobSettings, RetentionPolicy, RetentionStrategy
from cratedb_toolkit.retention.setup.schema import setup_schema
from cratedb_toolkit.retention.store import RetentionPolicyStore
from cratedb_toolkit.util import DatabaseAdapter, boot_with_dburi, jd

logger = logging.getLogger(__name__)


class EditExample:
    """
    An example program demonstrating retention policy editing.
    """

    def __init__(self, dburi):
        # Set up a generic database adapter.
        self.db = DatabaseAdapter(dburi=dburi)

        # Configure retention policy store to use the `examples` schema.
        self.settings = JobSettings(database=DatabaseAddress.from_string(dburi))
        if "PYTEST_CURRENT_TEST" not in os.environ:
            self.settings.policy_table.schema = "examples"

        # Set up adapter to retention policy store.
        self.setup()
        self.store = RetentionPolicyStore(settings=self.settings)

    def cleanup(self):
        """
        Drop retention policy table.
        """
        self.db.run_sql(f"DROP TABLE IF EXISTS {self.settings.policy_table.fullname};")

    def setup(self):
        """
        Create the SQL DDL schema.
        """
        setup_schema(settings=self.settings)

    def main(self):
        """
        Run a scenario of creating and deleting retention policies.
        """

        logger.info("Creating two policies, and list them")

        # Add a retention policy.
        policy = RetentionPolicy(
            strategy=RetentionStrategy.DELETE,
            table_schema="doc",
            table_name="foo",
            partition_column="ts_day",
            retention_period=1,
        )
        identifier = self.store.create(policy, ignore="DuplicateKeyException")

        # Add a retention policy using tags.
        policy = RetentionPolicy(
            strategy=RetentionStrategy.SNAPSHOT,
            tags=["foo", "bar"],
            table_schema="doc",
            table_name="bar",
            partition_column="ts_day",
            retention_period=90,
        )
        self.store.create(policy, ignore="DuplicateKeyException")

        logger.info("Listing policies")
        jd(self.store.retrieve())

        # Delete the policies created by the previous functions.
        logger.info("Deleting policies again")
        self.store.delete(identifier)
        self.store.delete_by_all_tags(["foo", "bar"])

        logger.info("Listing all remaining policies should equal an empty list")
        jd(self.store.retrieve())


def main(dburi: str):
    """
    Create different data retention policies, and list them.
    """

    logger.info("Running example application")
    example = EditExample(dburi=dburi)
    example.main()
    example.cleanup()


if __name__ == "__main__":
    """
    The program obtains a single positional argument from the command line,
    the database URI, in SQLAlchemy-compatible string format.
    """
    dburi = boot_with_dburi()
    main(dburi)
