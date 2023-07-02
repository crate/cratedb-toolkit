# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
About
=====

Example program demonstrating how to create retention policy records,
and invoke retention tasks.

It initializes the data retention and expiry subsystem, and creates
a data retention policy. After that, it invokes the corresponding
data retention job.

The program obtains a single positional argument from the command line,
the database URI, in SQLAlchemy-compatible string format. By default,
the program connects to a CrateDB instance on localhost.

Synopsis
========
::

    # General.
    python examples/basic.py crate://<USERNAME>:<PASSWORD>@<HOSTNAME>:4200?ssl=true

    # Default.
    python examples/basic.py crate://localhost:4200

"""
import logging
import os
import sys

from cratedb_retention.core import RetentionJob
from cratedb_retention.model import DatabaseAddress, JobSettings, RetentionPolicy, RetentionStrategy
from cratedb_retention.setup.schema import setup_schema
from cratedb_retention.store import RetentionPolicyStore
from cratedb_retention.util.cli import boot_with_dburi
from cratedb_retention.util.common import setup_logging
from cratedb_retention.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


class BasicExample:
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

    def create(self):
        """
        Run a scenario of creating and deleting retention policies.
        """

        logger.info("Creating a policy")

        # Add a basic retention policy.
        policy = RetentionPolicy(
            strategy=RetentionStrategy.DELETE,
            table_schema="doc",
            table_name="raw_metrics",
            partition_column="ts_day",
            retention_period=1,
        )
        self.store.create(policy, ignore="DuplicateKeyException")

    def invoke(self, strategy: str, cutoff_day: str):
        """
        How to invoke a data retention job.
        """
        # Create `JobSettings` instance.
        # It is the single source of truth about configuration and runtime settings.
        settings = JobSettings(
            database=self.settings.database,
            strategy=RetentionStrategy(strategy.upper()),
            cutoff_day=cutoff_day,
        )

        # Invoke the data retention job.
        job = RetentionJob(settings=settings)
        job.start()


def main(dburi: str):
    """
    Create and run data retention policy.
    """

    logger.info("Running example application")
    example = BasicExample(dburi=dburi)
    example.setup()
    example.create()
    example.invoke(strategy="delete", cutoff_day="2023-06-27")


if __name__ == "__main__":
    """
    The program obtains a single positional argument from the command line,
    the database URI, in SQLAlchemy-compatible string format.
    """
    dburi = boot_with_dburi()
    main(dburi)


if __name__ == "__main__":
    """
    The program obtains a single positional argument from the command line,
    the database URI, in SQLAlchemy-compatible string format.
    """
    setup_logging()
    try:
        dburi = sys.argv[1]
    except IndexError:
        dburi = "crate://localhost/"
    main(dburi)
