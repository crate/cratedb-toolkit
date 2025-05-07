# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
About
=====

Example program demonstrating how to create and maintain a basic variant
of materialized views.

It initializes the materialized view subsystem, inserts a bunch of data,
and creates a materialized view on it. After that, it inserts more data,
and refreshes the materialized view.

The program obtains a single positional argument from the command line,
the database URI, in SQLAlchemy-compatible string format. By default,
the program connects to a CrateDB instance on localhost.

Synopsis
========
::

    # Install package
    pip install cratedb-toolkit

    # General.
    python examples/materialized_view.py crate://<USERNAME>:<PASSWORD>@<HOSTNAME>:4200?ssl=true

    # Default.
    python examples/materialized_view.py crate://localhost:4200

"""

import logging
from textwrap import dedent

from cratedb_toolkit.materialized.core import MaterializedViewManager
from cratedb_toolkit.materialized.model import MaterializedView, MaterializedViewSettings
from cratedb_toolkit.materialized.schema import setup_schema
from cratedb_toolkit.model import DatabaseAddress, TableAddress
from cratedb_toolkit.util.cli import boot_with_dburi
from cratedb_toolkit.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


class MaterializedViewExample:
    """
    An example program demonstrating basic materialized views.
    """

    def __init__(self, dburi):
        # Set up a generic database adapter.
        self.db = DatabaseAdapter(dburi=dburi)

        # Configure store API to use the `examples` schema.
        self.settings = MaterializedViewSettings(
            database=DatabaseAddress.from_string(dburi),
            materialized_table=TableAddress(schema="examples", table="materialized_view"),
        )

        # Drop all tables used within this example.
        self.cleanup()

        # Create the SQL DDL schema for the materialized views management table.
        # TODO: Refactor to `MaterializedViewManager`.
        setup_schema(settings=self.settings)

        # Provide manager instance.
        self.manager = MaterializedViewManager(settings=self.settings)

    def cleanup(self):
        """
        Drop materialized view management table and data tables.
        """
        self.db.run_sql(f"DROP TABLE IF EXISTS {self.settings.materialized_table.fullname};")
        self.db.run_sql('DROP TABLE IF EXISTS "examples"."raw_metrics";')
        self.db.run_sql('DROP TABLE IF EXISTS "examples"."raw_metrics_view";')
        self.db.run_sql('DROP TABLE IF EXISTS "examples"."raw_metrics_view-staging";')

    def setup(self):
        """
        Create materialized view record in management table.
        """

        logger.info("Creating materialized view")

        # Add a record.
        mview = MaterializedView(
            table_schema="examples",
            table_name="raw_metrics_view",
            sql=dedent(
                """
            SELECT variable, MIN(value), MAX(value), AVG(value)
            FROM "examples"."raw_metrics"
            GROUP BY variable
            """
            ).strip(),
        )
        self.manager.store.create(mview, ignore="DuplicateKeyException")

        self.db.run_sql(f"REFRESH TABLE {self.settings.materialized_table.fullname};")

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
        self.db.run_sql(ddl)

        dml = """
            INSERT INTO "examples"."raw_metrics"
                (variable, timestamp, value, quality)
            SELECT
                'temperature' AS variable,
                generate_series AS timestamp,
                12 + RANDOM()*40 AS value,
                0 AS quality
            FROM generate_series('2023-06-01', '2023-06-30', '2 days'::INTERVAL);
        """
        self.db.run_sql(dml)

        dml = """
            INSERT INTO "examples"."raw_metrics"
                (variable, timestamp, value, quality)
            SELECT
                'humidity' AS variable,
                generate_series AS timestamp,
                40 + RANDOM()*50 AS value,
                0 AS quality
            FROM generate_series('2023-06-01', '2023-06-30', '2 days'::INTERVAL);
        """
        self.db.run_sql(dml)

        self.db.run_sql('REFRESH TABLE "examples"."raw_metrics";')

    def refresh(self):
        """
        Refresh materialized view.
        """
        self.manager.refresh(name="examples.raw_metrics_view")


def main(dburi: str):
    """
    Create and refresh emulated materialized views.
    """

    logger.info("Running example application")

    # Set up all the jazz.
    logger.info("Provisioning database")
    example = MaterializedViewExample(dburi=dburi)
    example.setup()
    example.setup_data()

    logger.info("Invoking materialized view refresh")

    # Invoke materialized view refresh.
    example.refresh()

    # Report about the number of records in table after first materialized view refresh.
    count = example.db.count_records("examples.raw_metrics_view")
    logger.info(f"Database table `examples.raw_metrics_view` contains {count} records")

    # Insert more data.
    dml = """
        INSERT INTO "examples"."raw_metrics"
            (variable, timestamp, value, quality)
        SELECT
            'battery' AS variable,
            generate_series AS timestamp,
            0 + RANDOM()*100 AS value,
            0 AS quality
        FROM generate_series('2023-06-01', '2023-06-30', '2 days'::INTERVAL);
    """
    example.db.run_sql(dml)
    example.db.run_sql('REFRESH TABLE "examples"."raw_metrics";')

    # Invoke materialized view refresh.
    example.refresh()

    # Report about the number of records in table after second materialized view refresh.
    count = example.db.count_records("examples.raw_metrics_view")
    logger.info(f"Database table `examples.raw_metrics_view` contains {count} records")

    # example.cleanup()


if __name__ == "__main__":
    """
    The program obtains a single positional argument from the command line,
    the database URI, in SQLAlchemy-compatible string format.
    """
    dburi = boot_with_dburi()
    main(dburi)
