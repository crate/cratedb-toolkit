import re

import pytest
from sqlalchemy.exc import ProgrammingError

from cratedb_toolkit.materialized.core import MaterializedViewManager
from cratedb_toolkit.materialized.model import MaterializedView
from tests.conftest import TESTDRIVE_DATA_SCHEMA


@pytest.fixture
def mview(database, store) -> MaterializedView:
    item = MaterializedView(
        table_schema=TESTDRIVE_DATA_SCHEMA,
        table_name="foobar",
        sql=f'SELECT * FROM "{TESTDRIVE_DATA_SCHEMA}"."raw_metrics"',  # noqa: S608
        id=None,
    )
    store.create(item, ignore="DuplicateKeyException")
    return item


def test_materialized_undefined(settings, database, store):
    mvm = MaterializedViewManager(settings=settings)
    with pytest.raises(KeyError) as ex:
        mvm.refresh("unknown.unknown")
    ex.match("Synthetic materialized table definition does not exist: unknown.unknown")


def test_materialized_missing_table(settings, database, store, mview):
    database.run_sql(f'CREATE TABLE "{TESTDRIVE_DATA_SCHEMA}"."foobar" AS SELECT 1;')

    mvm = MaterializedViewManager(settings=settings)
    with pytest.raises(ProgrammingError) as ex:
        mvm.refresh(f"{TESTDRIVE_DATA_SCHEMA}.foobar")
    ex.match(re.escape("RelationUnknown[Relation 'testdrive-data.raw_metrics' unknown]"))


def test_materialized_success(settings, database, store, mview):
    # Prepare a source table.
    database.run_sql(f'CREATE TABLE "{TESTDRIVE_DATA_SCHEMA}"."raw_metrics" AS SELECT 42;')
    database.run_sql(f'REFRESH TABLE "{TESTDRIVE_DATA_SCHEMA}"."raw_metrics"')

    # Invoke the materialized view refresh.
    mvm = MaterializedViewManager(settings=settings)
    mvm.refresh(f"{TESTDRIVE_DATA_SCHEMA}.foobar")

    # Verify the outcome.
    results = database.run_sql(f'SELECT * FROM "{TESTDRIVE_DATA_SCHEMA}"."foobar"')  # noqa: S608
    assert results == [(42,)]
