from pathlib import Path

import pytest
from sqlalchemy import TIMESTAMP

from tests.conftest import check_sqlalchemy2

pytestmark = pytest.mark.mongodb

pymongo = pytest.importorskip("pymongo", reason="Skipping tests because pymongo is not installed")
pytest.importorskip("bsonjs", reason="Skipping tests because bsonjs is not installed")
pytest.importorskip("rich", reason="Skipping tests because rich is not installed")

from cratedb_toolkit.io.mongodb.api import mongodb_copy  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def check_prerequisites():
    """
    This subsystem needs SQLAlchemy 2.x.
    """
    check_sqlalchemy2()


def test_mongodb_copy_transform_timestamp(caplog, cratedb, mongodb):
    """
    Verify MongoDB -> CrateDB data transfer with transformation.
    """
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"
    mongodb_url = f"{mongodb.get_connection_url()}/testdrive/demo"

    # Populate source database.
    client: pymongo.MongoClient = mongodb.get_connection_client()
    testdrive = client.get_database("testdrive")
    demo = testdrive.create_collection("demo")
    demo.insert_one({"device": "Hotzenplotz", "temperature": 42.42, "timestamp": 1563051934000})

    # Run transfer command.
    mongodb_copy(
        mongodb_url,
        cratedb_url,
        transformation=Path("examples/zyp/zyp-int64-to-timestamp.yaml"),
    )

    # Verify data in target database.
    cratedb.database.refresh_table("testdrive.demo")
    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo;", records=True)
    assert results[0]["timestamp"] == 1563051934000

    # Verify schema in target database.
    columns = cratedb.database.describe_table_columns("testdrive.demo")
    timestamp_type = columns[3]["type"]
    assert isinstance(timestamp_type, TIMESTAMP)
