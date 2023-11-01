# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import pytest

from cratedb_toolkit.testing.testcontainers.cratedb import CrateDBContainer
from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.util.common import setup_logging

# Use different schemas for storing the subsystem database tables, and the
# test/example data, so that they do not accidentally touch the default `doc`
# schema.
TESTDRIVE_EXT_SCHEMA = "testdrive-ext"
TESTDRIVE_DATA_SCHEMA = "testdrive-data"

RESET_TABLES = [
    f'"{TESTDRIVE_EXT_SCHEMA}"."retention_policy"',
    f'"{TESTDRIVE_DATA_SCHEMA}"."raw_metrics"',
    f'"{TESTDRIVE_DATA_SCHEMA}"."sensor_readings"',
    f'"{TESTDRIVE_DATA_SCHEMA}"."testdrive"',
    f'"{TESTDRIVE_DATA_SCHEMA}"."foobar"',
    f'"{TESTDRIVE_DATA_SCHEMA}"."foobar_unique_single"',
    f'"{TESTDRIVE_DATA_SCHEMA}"."foobar_unique_composite"',
]


class CrateDBFixture:
    """
    A little helper wrapping Testcontainer's `CrateDBContainer` and
    CrateDB Toolkit's `DatabaseAdapter`, agnostic of the test framework.
    """

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
    Configure the machinery to use a different schema for storing subsystem database
    tables, so that they do not accidentally touch the production system.

    If not configured otherwise, the test suite currently uses `testdrive-ext`.
    """
    session_mocker.patch("os.environ", {"CRATEDB_EXT_SCHEMA": TESTDRIVE_EXT_SCHEMA})


@pytest.fixture(scope="session")
def cratedb_service():
    """
    Provide a CrateDB service instance to the test suite.
    """
    db = CrateDBFixture()
    db.reset()
    yield db
    db.finalize()


@pytest.fixture(scope="function")
def cratedb(cratedb_service):
    """
    Provide a fresh canvas to each test case invocation, by resetting database content.
    """
    cratedb_service.reset()
    yield cratedb_service


setup_logging()
