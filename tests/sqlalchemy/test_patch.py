import datetime

import pytest
import sqlalchemy as sa
from crate.client.http import json_dumps

from tests.conftest import TESTDRIVE_DATA_SCHEMA


def test_inspector_vanilla(database, needs_sqlalchemy2):
    """
    Vanilla SQLAlchemy Inspector tests.
    """
    tablename = f'"{TESTDRIVE_DATA_SCHEMA}"."foobar"'
    inspector: sa.Inspector = sa.inspect(database.engine)
    database.run_sql(f"CREATE TABLE {tablename} AS SELECT 1")

    assert inspector.has_schema(TESTDRIVE_DATA_SCHEMA) is True

    table_names = inspector.get_table_names(schema=TESTDRIVE_DATA_SCHEMA)
    assert "foobar" in table_names

    view_names = inspector.get_view_names(schema=TESTDRIVE_DATA_SCHEMA)
    assert view_names == []

    indexes = inspector.get_indexes(tablename)
    assert indexes == []


def test_inspector_patched(database, needs_sqlalchemy2):
    """
    Patched SQLAlchemy Inspector tests.

    Both MLflow and LangChain invoke `get_table_names()` without a `schema` argument.
    This verifies that it still works, when it properly has been assigned to
    the `?schema=` connection string URL parameter.
    """
    tablename = f'"{TESTDRIVE_DATA_SCHEMA}"."foobar"'
    inspector: sa.Inspector = sa.inspect(database.engine)
    database.run_sql(f"CREATE TABLE {tablename} AS SELECT 1")
    assert inspector.has_schema(TESTDRIVE_DATA_SCHEMA) is True

    table_names = inspector.get_table_names()
    assert "foobar" in table_names


def test_json_encoder_date():
    """
    Verify the extended JSON encoder also accepts Python's `date` types.

    TODO: Move to different test file, as this no longer requires
          monkeypatching after using orjson for marshalling.
    """
    data = {"date": datetime.date(2024, 6, 4)}
    encoded = json_dumps(data)
    assert encoded == b'{"date":1717459200000}'


def test_json_encoder_numpy():
    """
    Verify the extended JSON encoder also accepts NumPy types.

    TODO: Move to different test file, as this no longer requires
          monkeypatching after using orjson for marshalling.
    """
    np = pytest.importorskip("numpy")

    data = {"scalar-int": np.float32(42.42).astype(int), "scalar-float": np.float32(42.42), "ndarray": np.ndarray([1])}
    encoded = json_dumps(data)
    assert encoded == b"""{"scalar-int":42,"scalar-float":42.42,"ndarray":[2.08e-322]}"""
