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

    # Install package
    pip install cratedb-retention

    # General.
    python examples/full.py crate://<USERNAME>:<PASSWORD>@<HOSTNAME>:4200?ssl=true

    # Default.
    python examples/full.py crate://localhost:4200

"""
import logging
import os

from cratedb_retention.core import RetentionJob
from cratedb_retention.model import DatabaseAddress, JobSettings, RetentionPolicy, RetentionStrategy
from cratedb_retention.setup.schema import setup_schema
from cratedb_retention.store import RetentionPolicyStore
from cratedb_retention.util.cli import boot_with_dburi
from cratedb_retention.util.database import DatabaseAdapter, run_sql

logger = logging.getLogger(__name__)


class FullExample:
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

    def cleanup(self):
        """
        Drop retention policy table and data table.
        """
        run_sql(self.dburi, f"DROP TABLE IF EXISTS {self.settings.policy_table.fullname};")
        run_sql(self.dburi, 'DROP TABLE IF EXISTS "examples"."raw_metrics";')

    def setup_retention(self):
        """
        Create the SQL DDL schema for the retention policy table, and insert a single record.
        """
        setup_schema(settings=self.settings)

        logger.info("Creating a policy")

        # Add a retention policy.
        policy = RetentionPolicy(
            strategy=RetentionStrategy.DELETE,
            table_schema="examples",
            table_name="raw_metrics",
            partition_column="ts_day",
            retention_period=1,
        )

        store = RetentionPolicyStore(settings=self.settings)
        store.create(policy, ignore="DuplicateKeyException")

        run_sql(self.dburi, f"REFRESH TABLE {self.settings.policy_table.fullname};")

    def setup_data(self):
        """
        Provision and populate data table.
        """
        ddl = """
            CREATE TABLE "examples"."raw_metrics" (
               "variable" TEXT,
               "timestamp" TIMESTAMP WITH TIME ZONE,
               "ts_day" TIMESTAMP GENERATED ALWAYS AS date_trunc('day', "timestamp"),
               "value" REAL,
               "quality" INTEGER,
               PRIMARY KEY ("variable", "timestamp", "ts_day")
            )
            PARTITIONED BY ("ts_day")
            WITH ("routing.allocation.require.storage" = 'hot')
            ;
        """

        dml = """
            INSERT INTO "examples"."raw_metrics"
                (variable, timestamp, value, quality)
            SELECT
                'temperature' AS variable,
                generate_series AS timestamp,
                RANDOM()*100 AS value,
                0 AS quality
            FROM generate_series('2023-06-01', '2023-06-30', '2 days'::INTERVAL);
        """

        run_sql(self.dburi, ddl)
        run_sql(self.dburi, dml)
        run_sql(self.dburi, 'REFRESH TABLE "examples"."raw_metrics";')

    def invoke(self, strategy: str, cutoff_day: str):
        """
        Invoke a data retention job.
        """

        # Create `JobSettings` instance, and configure it like the settings defined in the constructor.
        settings = JobSettings(
            database=self.settings.database,
            policy_table=self.settings.policy_table,
            strategy=RetentionStrategy(strategy.upper()),
            cutoff_day=cutoff_day,
        )

        # Invoke the data retention job.
        job = RetentionJob(settings=settings)
        job.start()

        run_sql(self.dburi, 'REFRESH TABLE "examples"."raw_metrics";')


def main(dburi: str):
    """
    Create and run data retention policy.
    """

    logger.info("Running example application")

    # Set up all the jazz.
    logger.info("Provisioning database")
    example = FullExample(dburi=dburi)
    example.cleanup()
    example.setup_retention()
    example.setup_data()

    logger.info("Invoking data retention/expiry")

    # Report about number of records in table before data retention.
    count = example.db.count_records("examples.raw_metrics")
    logger.info(f"Database table `examples.raw_metrics` contains {count} records")

    # Invoke data retention/expiry.
    example.invoke(strategy="delete", cutoff_day="2023-06-15")

    # Report about number of records in table after data retention.
    count = example.db.count_records("examples.raw_metrics")
    logger.info(f"Database table `examples.raw_metrics` contains {count} records")


if __name__ == "__main__":
    """
    The program obtains a single positional argument from the command line,
    the database URI, in SQLAlchemy-compatible string format.
    """
    dburi = boot_with_dburi()
    main(dburi)
