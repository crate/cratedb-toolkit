import pytest

from cratedb_toolkit.materialized.model import MaterializedViewSettings
from cratedb_toolkit.materialized.schema import setup_schema
from cratedb_toolkit.materialized.store import MaterializedViewStore
from cratedb_toolkit.model import DatabaseAddress, TableAddress
from cratedb_toolkit.util.database import DatabaseAdapter
from tests.conftest import TESTDRIVE_EXT_SCHEMA


@pytest.fixture()
def settings(cratedb) -> MaterializedViewSettings:
    """
    Provide the configuration and runtime settings objects, parameterized for the test suite.
    """
    database_url = cratedb.get_connection_url()
    settings = MaterializedViewSettings(
        database=DatabaseAddress.from_string(database_url),
        materialized_table=TableAddress(schema=TESTDRIVE_EXT_SCHEMA, table="materialized_view"),
    )
    return settings


@pytest.fixture()
def database(cratedb, settings):
    """
    Provide a client database adapter, which is connected to the test database instance.
    """
    yield DatabaseAdapter(dburi=settings.database.dburi, echo=True)


@pytest.fixture()
def store(database, settings):
    """
    Provide a MaterializedViewStore instance, connected to the test database instance.
    The materialized view management table schema has been established.
    """
    setup_schema(settings=settings)
    store = MaterializedViewStore(settings=settings)
    yield store
