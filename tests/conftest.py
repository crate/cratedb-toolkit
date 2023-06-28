# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.

import pytest
import sqlalchemy as sa

from cratedb_rollup.model import Settings
from cratedb_rollup.setup.schema import setup_schema
from cratedb_rollup.util.common import setup_logging
from cratedb_rollup.util.database import run_sql
from tests.testcontainers.cratedb import CrateDBContainer

# Use another schema for storing the retention policy table,
# so that it does not accidentally touch a production system.
TESTDRIVE_EXT_SCHEMA = "testdrive-ext"

RESET_TABLES = [
    f'"{TESTDRIVE_EXT_SCHEMA}"."retention_policies"',
    '"doc"."raw_metrics"',
    '"doc"."sensor_readings"',
    '"doc"."testdrive"',
]


class CrateDBFixture:
    def __init__(self):
        self.cratedb = None
        self.setup()

    def setup(self):
        # TODO: Make image name configurable.
        self.cratedb = CrateDBContainer("crate/crate:nightly")
        self.cratedb.start()

    def finalize(self):
        self.cratedb.stop()

    def reset(self):
        database_url = self.cratedb.get_connection_url()
        sa_engine = sa.create_engine(database_url)
        with sa_engine.connect() as conn:
            # TODO: Make list of tables configurable.
            for reset_table in RESET_TABLES:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {reset_table};")

    def get_connection_url(self, *args, **kwargs):
        return self.cratedb.get_connection_url(*args, **kwargs)

    def get_connection(self):
        database_url = self.cratedb.get_connection_url()
        sa_engine = sa.create_engine(database_url)
        with sa_engine.connect() as conn:
            return conn

    def execute(self, sql: str):
        conn = self.get_connection()
        return conn.execute(sa.text(sql))


@pytest.fixture(scope="function")
def cratedb():
    fixture = CrateDBFixture()
    yield fixture
    fixture.finalize()


@pytest.fixture(scope="function")
def provision_database(cratedb):
    """
    Populate `retention_policies` table, and data tables.
    """
    cratedb.reset()

    database_url = cratedb.get_connection_url()

    settings = Settings(dburi=database_url)
    settings.policy_table.schema = TESTDRIVE_EXT_SCHEMA
    setup_schema(settings=settings)

    ddls = [
        """
        CREATE TABLE "doc"."raw_metrics" (
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
        """,
        """
        CREATE TABLE "doc"."sensor_readings" (
           time TIMESTAMP WITH TIME ZONE NOT NULL,
           time_month TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS DATE_TRUNC('month', "time"),
           sensor_id TEXT NOT NULL,
           battery_level DOUBLE PRECISION,
           battery_status TEXT,
           battery_temperature DOUBLE PRECISION
        )
        PARTITIONED BY (time_month);
        """,
    ]
    for sql in ddls:
        run_sql(database_url, sql)

    data = [
        """
        INSERT INTO "doc"."raw_metrics"
            (variable, timestamp, value, quality)
        VALUES
            ('temperature', '2023-06-27T12:00:00', 42.42, 0);
        """,
        """
        INSERT INTO "doc"."raw_metrics"
            (variable, timestamp, value, quality)
        VALUES
            ('water-flow', NOW() - '5 months'::INTERVAL, 12, 1);
        """,
        """
        INSERT INTO "doc"."sensor_readings"
            (time, sensor_id, battery_level, battery_status, battery_temperature)
        VALUES
            (NOW() - '6 years'::INTERVAL, 'batt01', 98.99, 'FULL', 42.42);
        """,
        """
        INSERT INTO "doc"."sensor_readings"
            (time, sensor_id, battery_level, battery_status, battery_temperature)
        VALUES
            (NOW() - '5 years'::INTERVAL, 'batt01', 83.82, 'ALMOST FULL', 18.42);
        """,
    ]
    for sql in data:
        run_sql(database_url, sql)

    rules = [
        f"""
        -- Provision retention policy rule for the DELETE strategy.
        INSERT INTO {settings.policy_table.fullname} (
          table_schema, table_name, partition_column, retention_period, strategy)
        VALUES ('doc', 'raw_metrics', 'ts_day', 1, 'delete');
        """,  # noqa: S608
        f"""
        -- Provision retention policy rule for the REALLOCATE strategy.
        INSERT INTO {settings.policy_table.fullname}
        VALUES ('doc', 'raw_metrics', 'ts_day', 60, 'storage', 'cold', NULL, 'reallocate');
        """,  # noqa: S608
        f"""
        -- Provision retention policy rule for the SNAPSHOT strategy.
        INSERT INTO {settings.policy_table.fullname}
          (table_schema, table_name, partition_column, retention_period, target_repository_name, strategy)
        VALUES ('doc', 'sensor_readings', 'time_month', 365, 'export_cold', 'snapshot');
        """,  # noqa: S608
    ]
    for sql in rules:
        run_sql(database_url, sql)

    # Synchronize data.
    run_sql(database_url, 'REFRESH TABLE "doc"."raw_metrics";')
    run_sql(database_url, 'REFRESH TABLE "doc"."sensor_readings";')
    run_sql(database_url, f"REFRESH TABLE {settings.policy_table.fullname};")


setup_logging()
