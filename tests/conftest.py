# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import pytest

from cratedb_retention.model import DatabaseAddress, JobSettings, RetentionPolicy, RetentionStrategy
from cratedb_retention.setup.schema import setup_schema
from cratedb_retention.store import RetentionPolicyStore
from cratedb_retention.util.common import setup_logging
from cratedb_retention.util.database import DatabaseAdapter, run_sql
from tests.testcontainers.azurite import ExtendedAzuriteContainer
from tests.testcontainers.cratedb import CrateDBContainer
from tests.testcontainers.minio import ExtendedMinioContainer

# Use different schemas both for storing the retention policy table, and
# the test data, so that they do not accidentally touch the default `doc`
# schema of CrateDB.
TESTDRIVE_EXT_SCHEMA = "testdrive-ext"
TESTDRIVE_DATA_SCHEMA = "testdrive-data"

RESET_TABLES = [
    f'"{TESTDRIVE_EXT_SCHEMA}"."retention_policy"',
    f'"{TESTDRIVE_DATA_SCHEMA}"."raw_metrics"',
    f'"{TESTDRIVE_DATA_SCHEMA}"."sensor_readings"',
    f'"{TESTDRIVE_DATA_SCHEMA}"."testdrive"',
    f'"{TESTDRIVE_DATA_SCHEMA}"."foobar"',
]


class CrateDBFixture:
    def __init__(self):
        self.cratedb = None
        self.database: DatabaseAdapter = None
        self.setup()

    def setup(self):
        # TODO: Make image name configurable.
        self.cratedb = CrateDBContainer("crate/crate:nightly")
        self.cratedb.start()
        self.database = DatabaseAdapter(dburi=self.get_connection_url())

    def finalize(self):
        self.cratedb.stop()

    def reset(self):
        # TODO: Make list of tables configurable.
        for reset_table in RESET_TABLES:
            self.database.connection.exec_driver_sql(f"DROP TABLE IF EXISTS {reset_table};")

    def get_connection_url(self, *args, **kwargs):
        return self.cratedb.get_connection_url(*args, **kwargs)


@pytest.fixture(scope="session", autouse=True)
def configure_database_schema(session_mocker):
    """
    Configure the machinery to use another schema for storing the retention
    policy table, so that it does not accidentally touch a production system.

    If not configured otherwise, the test suite currently uses `testdrive-ext`.
    """
    session_mocker.patch("os.environ", {"CRATEDB_EXT_SCHEMA": TESTDRIVE_EXT_SCHEMA})


@pytest.fixture(scope="function")
def cratedb():
    db = CrateDBFixture()
    db.reset()
    yield db
    db.finalize()


@pytest.fixture(scope="session")
def minio():
    """
    For testing the "SNAPSHOT" strategy against an Amazon Web Services S3 object storage API,
    provide a MinIO service to the test suite.

    - https://en.wikipedia.org/wiki/Object_storage
    - https://en.wikipedia.org/wiki/Amazon_S3
    - https://github.com/minio/minio
    - https://crate.io/docs/crate/reference/en/latest/sql/statements/create-repository.html
    """
    with ExtendedMinioContainer() as minio:
        yield minio


@pytest.fixture(scope="session")
def azurite():
    """
    For testing the "SNAPSHOT" strategy against a Microsoft Azure Blob Storage object storage API,
    provide an Azurite service to the test suite.

    - https://en.wikipedia.org/wiki/Object_storage
    - https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite
    - https://learn.microsoft.com/en-us/azure/storage/blobs/use-azurite-to-run-automated-tests
    - https://github.com/azure/azurite
    - https://crate.io/docs/crate/reference/en/latest/sql/statements/create-repository.html
    """
    with ExtendedAzuriteContainer() as azurite:
        yield azurite


@pytest.fixture()
def database(cratedb, settings):
    """
    Provide a client database adapter, which is connected to the test database instance.
    """
    yield DatabaseAdapter(dburi=settings.database.dburi)


@pytest.fixture()
def store(database, settings):
    """
    Provide a client database adapter, which is connected to the test database instance.
    The retention policy database table schema has been established.
    """
    setup_schema(settings=settings)
    rps = RetentionPolicyStore(settings=settings)
    yield rps


@pytest.fixture()
def settings(cratedb):
    """
    Provide configuration and runtime settings object, parameterized for the test suite.
    """
    database_url = cratedb.get_connection_url()
    job_settings = JobSettings(database=DatabaseAddress.from_string(database_url))
    job_settings.policy_table.schema = TESTDRIVE_EXT_SCHEMA
    return job_settings


@pytest.fixture(scope="function")
def policies(cratedb, settings, store):
    """
    Populate the retention policy table.
    """
    database_url = cratedb.get_connection_url()
    rules = [
        # Retention policy rule for the DELETE strategy.
        RetentionPolicy(
            strategy=RetentionStrategy.DELETE,
            table_schema=TESTDRIVE_DATA_SCHEMA,
            table_name="raw_metrics",
            partition_column="ts_day",
            retention_period=1,
        ),
        # Retention policy rule for the DELETE strategy, using tags.
        RetentionPolicy(
            strategy=RetentionStrategy.DELETE,
            tags={"foo", "bar"},
            table_schema=TESTDRIVE_DATA_SCHEMA,
            table_name="sensor_readings",
            partition_column="time_month",
            retention_period=1,
        ),
    ]
    for rule in rules:
        store.create(rule, ignore="DuplicateKeyException")

    # Synchronize data.
    run_sql(database_url, f"REFRESH TABLE {settings.policy_table.fullname};")


@pytest.fixture(scope="function")
def raw_metrics(cratedb, settings, store):
    """
    Populate the `raw_metrics` table.
    """

    tablename_full = f'"{TESTDRIVE_DATA_SCHEMA}"."raw_metrics"'

    database_url = cratedb.get_connection_url()
    ddl = f"""
        CREATE TABLE {tablename_full} (
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

    dml = f"""
        INSERT INTO {tablename_full}
            (variable, timestamp, value, quality)
        SELECT
            'temperature' AS variable,
            generate_series AS timestamp,
            RANDOM()*100 AS value,
            0 AS quality
        FROM generate_series('2023-06-01', '2023-06-30', '5 days'::INTERVAL);
    """

    run_sql(database_url, ddl)
    run_sql(database_url, dml)
    run_sql(database_url, f"REFRESH TABLE {tablename_full};")

    return tablename_full


@pytest.fixture(scope="function")
def sensor_readings(cratedb, settings, store):
    """
    Populate the `sensor_readings` table.
    """

    tablename_full = f'"{TESTDRIVE_DATA_SCHEMA}"."sensor_readings"'

    database_url = cratedb.get_connection_url()
    ddl = f"""
        CREATE TABLE {tablename_full} (
           time TIMESTAMP WITH TIME ZONE NOT NULL,
           time_month TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS DATE_TRUNC('month', "time"),
           sensor_id TEXT NOT NULL,
           battery_level DOUBLE PRECISION,
           battery_status TEXT,
           battery_temperature DOUBLE PRECISION
        )
        PARTITIONED BY (time_month);
    """

    dml = f"""
        INSERT INTO {tablename_full}
            (time, sensor_id, battery_level, battery_status, battery_temperature)
        SELECT
            generate_series AS time,
            'batt01' AS sensor_id,
            RANDOM()*100 AS battery_level,
            'FULL' AS battery_status,
            RANDOM()*100 AS battery_temperature
        FROM generate_series(
            '2023-05-01'::TIMESTAMPTZ,
            '2023-06-30'::TIMESTAMPTZ,
            '7 days'::INTERVAL
        );
    """

    run_sql(database_url, ddl)
    run_sql(database_url, dml)
    run_sql(database_url, f"REFRESH TABLE {tablename_full};")

    return tablename_full


@pytest.fixture(scope="function")
def raw_metrics_reallocate_policy(store):
    """
    Populate the retention policy table.
    """
    # Retention policy rule for the REALLOCATE strategy.
    rule = RetentionPolicy(
        strategy=RetentionStrategy.REALLOCATE,
        table_schema=TESTDRIVE_DATA_SCHEMA,
        table_name="raw_metrics",
        partition_column="ts_day",
        retention_period=60,
        reallocation_attribute_name="storage",
        reallocation_attribute_value="warm",
    )
    store.create(rule, ignore="DuplicateKeyException")


@pytest.fixture(scope="function")
def sensor_readings_snapshot_policy(store):
    """
    Populate the retention policy table.
    """
    # Retention policy rule for the SNAPSHOT strategy.
    rule = RetentionPolicy(
        strategy=RetentionStrategy.SNAPSHOT,
        table_schema=TESTDRIVE_DATA_SCHEMA,
        table_name="sensor_readings",
        partition_column="time_month",
        retention_period=365,
        target_repository_name="export_cold",
    )
    store.create(rule, ignore="DuplicateKeyException")


setup_logging()
