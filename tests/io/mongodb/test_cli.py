import os
from unittest import mock
from uuid import UUID

import dateutil
import pytest
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


DOCUMENT_IN = {
    "id": bson.Binary.from_uuid(UUID("d575540f-759d-4653-a4c4-4a9e410f1aa1")),
    "value": {
        "name": "foobar",
        "active": True,
        "created": dateutil.parser.parse("2020-06-19T15:03:53.727Z"),
        "timestamp": bson.datetime_ms.DatetimeMS(1455141600000),
    },
}
DOCUMENT_OUT = {
    "__id": mock.ANY,
    "id": "d575540f-759d-4653-a4c4-4a9e410f1aa1",
    "value": {
        "name": "foobar",
        "active": True,
        "created": 1592579033000,
        "timestamp": 1455141600000,
    },
}
DOCUMENT_DDL = """
CREATE TABLE IF NOT EXISTS "testdrive"."demo" (
   "__id" TEXT,
   "id" TEXT,
   "value" OBJECT(DYNAMIC) AS (
      "name" TEXT,
      "active" BOOLEAN,
      "created" TIMESTAMP WITH TIME ZONE,
      "timestamp" TIMESTAMP WITH TIME ZONE
   )
)""".lstrip()


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
    assert DOCUMENT_DDL in results[0][0]
