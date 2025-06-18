from pathlib import Path
from unittest import mock

import pymongo
import pytest

from cratedb_toolkit.io.mongodb.api import mongodb_copy
from tests.conftest import check_sqlalchemy2

pytestmark = pytest.mark.mongodb


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
        transformation=Path("examples/tikray/tikray-int64-to-timestamp.yaml"),
    )

    # Verify data in target database.
    cratedb.database.refresh_table("testdrive.demo")
    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo;", records=True)
    assert results[0]["data"]["timestamp"] == 1563051934000

    # Verify schema in target database.
    type_result = cratedb.database.run_sql(
        "SELECT pg_typeof(data['timestamp']) AS type FROM testdrive.demo;", records=True
    )
    timestamp_type = type_result[0]["type"]
    assert timestamp_type == "bigint"

    # FIXME: Only works with a defined schema.
    # assert timestamp_type == "TIMESTAMP WITH TIME ZONE"  # noqa: ERA001


def test_mongodb_copy_treatment_all(caplog, cratedb, mongodb):
    """
    Verify MongoDB -> CrateDB data transfer with Tikray Treatment transformation.
    """

    data_in = {
        "ignore_toplevel": 42,
        "value": {
            "id": 42,
            "date": {"$date": "2015-09-23T10:32:42.33Z"},
            "ignore_nested": 42,
        },
        "to_list": 42,
        "to_string": 42,
        "to_dict_scalar": 42,
        "to_dict_list": [{"user": 42}],
    }

    data_out = {
        "oid": mock.ANY,
        "data": {
            "_id": mock.ANY,
            "value": {
                "date": 1443004362000,
                "id": 42,
            },
            "to_list": [42],
            "to_string": "42",
            "to_dict_scalar": {"id": 42},
            "to_dict_list": [{"user": {"id": 42}}],
        },
    }

    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"
    mongodb_url = f"{mongodb.get_connection_url()}/testdrive/demo"

    # Populate source database.
    client: pymongo.MongoClient = mongodb.get_connection_client()
    testdrive = client.get_database("testdrive")
    demo = testdrive.create_collection("demo")
    demo.insert_one(data_in)

    # Run transfer command.
    mongodb_copy(
        mongodb_url,
        cratedb_url,
        transformation=Path("examples/tikray/tikray-treatment-all.yaml"),
    )

    # Verify data in target database.
    cratedb.database.refresh_table("testdrive.demo")
    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo;", records=True)
    assert results[0] == data_out
    return
    assert results[0]["data"]["timestamp"] == 1563051934000

    # Verify schema in target database.
    type_result = cratedb.database.run_sql(
        "SELECT pg_typeof(data['timestamp']) AS type FROM testdrive.demo;", records=True
    )
    timestamp_type = type_result[0]["type"]
    assert timestamp_type == "bigint"

    # FIXME: Only works with a defined schema.
    # assert timestamp_type == "TIMESTAMP WITH TIME ZONE"  # noqa: ERA001
