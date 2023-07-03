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
    pip install cratedb-retention

    # General.
    python examples/edit.py crate://<USERNAME>:<PASSWORD>@<HOSTNAME>:4200?ssl=true

    # Default.
    python examples/edit.py crate://localhost:4200

"""
import logging
import os

from cratedb_retention.model import DatabaseAddress, JobSettings, RetentionPolicy, RetentionStrategy
from cratedb_retention.setup.schema import setup_schema
from cratedb_retention.store import RetentionPolicyStore
from cratedb_retention.util.cli import boot_with_dburi
from cratedb_retention.util.data import jd
from cratedb_retention.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


class EditExample:
    """
    An example program demonstrating retention policy editing.
    """

    def __init__(self, dburi):
        self.dburi = dburi

        # Set up a generic database adapter.
        self.db = DatabaseAdapter(dburi=self.dburi)

        # Configure retention policy store to use the `examples` schema.
        self.settings = JobSettings(database=DatabaseAddress.from_string(self.dburi))
        if "PYTEST_CURRENT_TEST" not in os.environ:
            self.settings.policy_table.schema = "examples"

        # Set up adapter to retention policy store.
        self.store = RetentionPolicyStore(settings=self.settings)

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
            table_name="raw_metrics",
            partition_column="ts_day",
            retention_period=1,
        )
        identifier = self.store.create(policy, ignore="DuplicateKeyException")

        # Add a retention policy using tags.
        policy = RetentionPolicy(
            strategy=RetentionStrategy.SNAPSHOT,
            tags=["foo", "bar"],
            table_schema="doc",
            table_name="raw_metrics",
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
    example.setup()
    example.main()


if __name__ == "__main__":
    """
    The program obtains a single positional argument from the command line,
    the database URI, in SQLAlchemy-compatible string format.
    """
    dburi = boot_with_dburi()
    main(dburi)
