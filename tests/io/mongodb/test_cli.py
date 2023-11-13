import os

import pytest
from click.testing import CliRunner
from pueblo.testing.dataframe import DataFrameFactory

from cratedb_toolkit.cli import cli

pytestmark = pytest.mark.mongodb

pymongo = pytest.importorskip("pymongo", reason="Skipping tests because pymongo is not installed")
pytest.importorskip("rich", reason="Skipping tests because rich is not installed")


def test_version():
    """
    CLI test: Invoke `migr8 --version`.
    """
    exitcode = os.system("migr8 --version")  # noqa: S605,S607
    assert exitcode == 0


def test_mongodb_load_table(caplog, cratedb, mongodb):
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
