# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import pytest

from cratedb_toolkit.testing.testcontainers.azurite import ExtendedAzuriteContainer
from cratedb_toolkit.testing.testcontainers.cratedb import CrateDBContainer
from cratedb_toolkit.testing.testcontainers.minio import ExtendedMinioContainer
from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.util.common import setup_logging

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


@pytest.fixture(scope="session")
def cratedb_service():
    db = CrateDBFixture()
    db.reset()
    yield db
    db.finalize()


@pytest.fixture(scope="function")
def cratedb(cratedb_service):
    cratedb_service.reset()
    yield cratedb_service


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


setup_logging()
