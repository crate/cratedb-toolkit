import pytest

from cratedb_toolkit.util.database import DatabaseAdapter
from tests.conftest import TESTDRIVE_DATA_SCHEMA


@pytest.fixture
def database(cratedb):
    """
    Provide a client database adapter, which is connected to the test database instance.
    """
    database_url = cratedb.get_connection_url() + "?schema=" + TESTDRIVE_DATA_SCHEMA
    yield DatabaseAdapter(dburi=database_url)
