# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
About
=====

Example program demonstrating how to create retention policy records,
and invoke retention tasks.

Specifically, this example demonstrates how to use a given cutoff date
when invoking the data retention job, in order to retire all data before
that date and the configured retention duration time.

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
    pip install cratedb-toolkit

    # General.
    python examples/retention_retire_cutoff.py crate://<USERNAME>:<PASSWORD>@<HOSTNAME>:4200?ssl=true

    # Default.
    python examples/retention_retire_cutoff.py crate://localhost:4200

"""
import logging
import os

from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.retention.core import RetentionJob
from cratedb_toolkit.retention.model import JobSettings, RetentionPolicy, RetentionStrategy
from cratedb_toolkit.retention.setup.schema import setup_schema
from cratedb_toolkit.retention.store import RetentionPolicyStore
from cratedb_toolkit.util import DatabaseAdapter, boot_with_dburi

logger = logging.getLogger(__name__)


class RetireCutoffExample:
    """
    An example program demonstrating data retention with given cutoff date.
    """

    def __init__(self, dburi):
        # Set up a generic database adapter.
        self.db = DatabaseAdapter(dburi=dburi)

        # Configure retention policy store to use the `examples` schema.
        self.settings = JobSettings(database=DatabaseAddress.from_string(dburi))
        if "PYTEST_CURRENT_TEST" not in os.environ:
            self.settings.policy_table.schema = "examples"

    def cleanup(self):
        """
        Drop retention policy table and data table.
        """
        self.db.run_sql(f"DROP TABLE IF EXISTS {self.settings.policy_table.fullname};")
        self.db.run_sql('DROP TABLE IF EXISTS "examples"."raw_metrics";')

    def setup(self):
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

        self.db.run_sql(f"REFRESH TABLE {self.settings.policy_table.fullname};")

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
            CLUSTERED INTO 1 SHARDS
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

        self.db.run_sql(ddl)
        self.db.run_sql(dml)
        self.db.run_sql('REFRESH TABLE "examples"."raw_metrics";')

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

        self.db.run_sql('REFRESH TABLE "examples"."raw_metrics";')


def main(dburi: str):
    """
    Create and run data retention policy.
    """

    logger.info("Running example application")

    # Set up all the jazz.
    logger.info("Provisioning database")
    example = RetireCutoffExample(dburi=dburi)
    example.cleanup()
    example.setup()
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

    example.cleanup()


if __name__ == "__main__":
    """
    The program obtains a single positional argument from the command line,
    the database URI, in SQLAlchemy-compatible string format.
    """
    dburi = boot_with_dburi()
    main(dburi)
