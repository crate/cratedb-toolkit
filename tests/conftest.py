# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.

import pytest
import sqlalchemy as sa

from cratedb_rollup.setup.schema import setup_schema
from cratedb_rollup.util.common import setup_logging
from cratedb_rollup.util.database import run_sql
from tests.testcontainers.cratedb import CrateDBContainer

RESET_TABLES = ["testdrive", "retention_policies", "raw_metrics", "sensor_readings"]


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
    Populate `raw_metrics` and `retention_policies` tables.
    """
    cratedb.reset()

    database_url = cratedb.get_connection_url()

    setup_schema(database_url)
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
        CREATE TABLE doc.sensor_readings (
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
        INSERT INTO doc.raw_metrics
            (variable, timestamp, value, quality)
        VALUES
            ('temperature', '2023-06-27T12:00:00', 42.42, 0);
        """,
        """
        INSERT INTO doc.raw_metrics
            (variable, timestamp, value, quality)
        VALUES
            ('water-flow', NOW() - '5 months'::INTERVAL, 12, 1);
        """,
        """
        INSERT INTO doc.sensor_readings
            (time, sensor_id, battery_level, battery_status, battery_temperature)
        VALUES
            (NOW() - '6 years'::INTERVAL, 'batt01', 98.99, 'FULL', 42.42);
        """,
        """
        INSERT INTO doc.sensor_readings
            (time, sensor_id, battery_level, battery_status, battery_temperature)
        VALUES
            (NOW() - '5 years'::INTERVAL, 'batt01', 83.82, 'ALMOST FULL', 18.42);
        """,
    ]
    for sql in data:
        run_sql(database_url, sql)

    rules = [
        """
        -- Provision retention policy rule for the DELETE strategy.
        INSERT INTO retention_policies (
          table_schema, table_name, partition_column, retention_period, strategy)
        VALUES ('doc', 'raw_metrics', 'ts_day', 1, 'delete');
        """,
        """
        -- Provision retention policy rule for the REALLOCATE strategy.
        INSERT INTO retention_policies
        VALUES ('doc', 'raw_metrics', 'ts_day', 60, 'storage', 'cold', NULL, 'reallocate');
        """,
        """
        -- Provision retention policy rule for the SNAPSHOT strategy.
        INSERT INTO retention_policies
          (table_schema, table_name, partition_column, retention_period, target_repository_name, strategy)
        VALUES ('doc', 'sensor_readings', 'time_month', 365, 'export_cold', 'snapshot');
        """,
    ]
    for sql in rules:
        run_sql(database_url, sql)

    # Synchronize data.
    run_sql(database_url, 'REFRESH TABLE "doc"."raw_metrics";')
    run_sql(database_url, 'REFRESH TABLE "doc"."retention_policies";')


setup_logging()
