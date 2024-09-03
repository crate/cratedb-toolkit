import os
from unittest import mock
from uuid import UUID

import dateutil
import pytest
import sqlparse
from click.testing import CliRunner
from pueblo.testing.dataframe import DataFrameFactory

from cratedb_toolkit.cli import cli
from tests.conftest import check_sqlalchemy2

pytestmark = pytest.mark.mongodb

bson = pytest.importorskip("bson", reason="Skipping tests because bson is not installed")
pymongo = pytest.importorskip("pymongo", reason="Skipping tests because pymongo is not installed")
pytest.importorskip("rich", reason="Skipping tests because rich is not installed")


@pytest.fixture(scope="module", autouse=True)
def check_prerequisites():
    """
    This subsystem needs SQLAlchemy 2.x.
    """
    check_sqlalchemy2()


def test_version():
    """
    CLI test: Invoke `migr8 --version`.
    """
    exitcode = os.system("migr8 --version")  # noqa: S605,S607
    assert exitcode == 0


DATETIME = dateutil.parser.parse("2020-06-19T15:03:53.727Z")

DOCUMENT_IN = {
    "id": bson.Binary.from_uuid(UUID("d575540f-759d-4653-a4c4-4a9e410f1aa1")),
    "value": {
        "name": "foobar",
        "active": True,
        "created": DATETIME,
        "timestamp": bson.datetime_ms.DatetimeMS(1455141600000),
        "list_date": [DATETIME, DATETIME],
        "list_empty": [],
        "list_float": [42.42, 43.43],
        "list_integer": [42, 43],
        "list_object_symmetric": [{"foo": "bar"}, {"baz": "qux"}],
        "list_object_varying_string": [{"value": 42}, {"value": "qux"}],
        # TODO: Improve decoding of inner items.
        "list_object_varying_date": [{"value": DATETIME}, {"value": "qux"}],
        "list_string": ["foo", "bar"],
    },
}
DOCUMENT_OUT = {
    "oid": mock.ANY,
    "data": {
        "_id": mock.ANY,
        "id": "d575540f-759d-4653-a4c4-4a9e410f1aa1",
        "value": {
            "name": "foobar",
            "active": True,
            "created": 1592579033000,
            "timestamp": 1455141600000,
            "list_date": [1592579033000, 1592579033000],
            "list_empty": [],
            "list_float": [42.42, 43.43],
            "list_integer": [42, 43],
            "list_object_symmetric": [{"foo": "bar"}, {"baz": "qux"}],
            "list_object_varying_string": [{"value": "42"}, {"value": "qux"}],
            # TODO: Improve decoding of inner items.
            "list_object_varying_date": [{"value": "{'$date': '2020-06-19T15:03:53.727Z'}"}, {"value": "qux"}],
            "list_string": ["foo", "bar"],
        },
    },
}
DOCUMENT_DDL = """
CREATE TABLE IF NOT EXISTS "testdrive"."demo" (
   "oid" TEXT,
   "data" OBJECT(DYNAMIC)""".lstrip()


def test_mongodb_load_table_basic(caplog, cratedb, mongodb):
    """
    CLI test: Invoke `ctk load table` for MongoDB.
    """
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"
    mongodb_url = f"{mongodb.get_connection_url()}/testdrive/demo"

    # Create sample dataset with a few records worth of data.
    dff = DataFrameFactory(rows=42)
    df = dff.make("dateindex")

    # Populate source database.
    client: pymongo.MongoClient = mongodb.get_connection_client()
    testdrive = client.get_database("testdrive")
    demo = testdrive.create_collection("demo")
    demo.insert_many(df.to_dict("records"))

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb_url})
    result = runner.invoke(
        cli,
        args=f"load table {mongodb_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 42


def test_mongodb_load_table_real(caplog, cratedb, mongodb):
    """
    CLI test: Invoke `ctk load table` for MongoDB.
    """
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"
    mongodb_url = f"{mongodb.get_connection_url()}/testdrive/demo"

    # Populate source database.
    client: pymongo.MongoClient = mongodb.get_connection_client()
    testdrive = client.get_database("testdrive")
    demo = testdrive.create_collection("demo")
    demo.insert_many([DOCUMENT_IN])

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb_url})
    result = runner.invoke(
        cli,
        args=f"load table {mongodb_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify metadata in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 1

    # Verify content in target database.
    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo", records=True)
    assert results[0] == DOCUMENT_OUT

    # Verify schema in target database.
    results = cratedb.database.run_sql("SHOW CREATE TABLE testdrive.demo")
    sql = results[0][0]
    sql = sqlparse.format(sql)
    assert sql.startswith(DOCUMENT_DDL)
