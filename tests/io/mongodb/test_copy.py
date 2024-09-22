import json
from copy import deepcopy
from pathlib import Path
from unittest import mock

import pymongo
import pytest
from zyp import CollectionTransformation, MokshaTransformation
from zyp.model.collection import CollectionAddress
from zyp.model.project import TransformationProject

from cratedb_toolkit.io.mongodb.api import mongodb_copy
from tests.conftest import check_sqlalchemy2

pytestmark = pytest.mark.mongodb


@pytest.fixture(scope="module", autouse=True)
def check_prerequisites():
    """
    This subsystem needs SQLAlchemy 2.x.
    """
    check_sqlalchemy2()


def test_mongodb_copy_server_database(caplog, cratedb, mongodb):
    """
    Verify MongoDB -> CrateDB data transfer for all collections in a database.
    """

    # Reset two database tables.
    cratedb.database.run_sql('DROP TABLE IF EXISTS testdrive."demo1";')
    cratedb.database.run_sql('DROP TABLE IF EXISTS testdrive."demo2";')

    # Define source and target URLs.
    mongodb_url = f"{mongodb.get_connection_url()}/testdrive"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    # Define data.
    data_in = {"device": "Hotzenplotz", "temperature": 42.42, "timestamp": 1563051934000}
    data_out = deepcopy(data_in)
    data_out.update({"_id": mock.ANY})

    # Populate source database.
    client: pymongo.MongoClient = mongodb.get_connection_client()
    testdrive = client.get_database("testdrive")
    demo1 = testdrive.create_collection("demo1")
    demo1.insert_one(data_in)
    demo2 = testdrive.create_collection("demo2")
    demo2.insert_one(data_in)

    # Run transfer command.
    mongodb_copy(
        mongodb_url,
        cratedb_url,
    )

    # Verify data in target database.
    cratedb.database.refresh_table("testdrive.demo1")
    cratedb.database.refresh_table("testdrive.demo2")
    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo1;", records=True)
    assert results[0]["data"] == data_out
    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo2;", records=True)
    assert results[0]["data"] == data_out


def test_mongodb_copy_server_collection_with_filter(caplog, cratedb, mongodb):
    """
    Verify MongoDB -> CrateDB data transfer for a specific collection, with filtering.
    """

    # Define source and target URLs.
    filter_expression = json.dumps({"timestamp": {"$gt": 1563051934050}})
    mongodb_url = f"{mongodb.get_connection_url()}/testdrive/demo?filter={filter_expression}"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Define data.
    data_in = [
        {"device": "Hotzenplotz", "temperature": 42.42, "timestamp": 1563051934000},
        {"device": "Hotzenplotz", "temperature": 42.42, "timestamp": 1563051934100},
    ]
    data_out = deepcopy(data_in)
    data_out[0].update({"_id": mock.ANY})
    data_out[1].update({"_id": mock.ANY})

    # Populate source database.
    client: pymongo.MongoClient = mongodb.get_connection_client()
    testdrive = client.get_database("testdrive")
    demo = testdrive.create_collection("demo")
    demo.insert_many(data_in)

    # Run transfer command.
    mongodb_copy(
        mongodb_url,
        cratedb_url,
    )

    # Verify data in target database.
    cratedb.database.refresh_table("testdrive.demo")
    assert cratedb.database.count_records("testdrive.demo") == 1
    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo;", records=True)
    assert results[0]["data"] == data_out[1]


def test_mongodb_copy_filesystem_folder_absolute(caplog, cratedb, mongodb):
    """
    Verify MongoDB -> CrateDB data transfer for all files in a folder, with relative addressing.
    """

    # Reset two database tables.
    cratedb.database.run_sql('DROP TABLE IF EXISTS testdrive."books-canonical";')
    cratedb.database.run_sql('DROP TABLE IF EXISTS testdrive."books-relaxed";')

    # Define source and target URLs.
    path = Path("./tests/io/mongodb/*.ndjson").absolute()
    fs_resource = f"file+bson://{path}"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    # Run transfer command.
    mongodb_copy(
        fs_resource,
        cratedb_url,
    )

    # Verify data in target database.
    cratedb.database.refresh_table("testdrive.books-canonical")
    cratedb.database.refresh_table("testdrive.books-relaxed")

    assert cratedb.database.count_records("testdrive.books-canonical") == 4
    assert cratedb.database.count_records("testdrive.books-relaxed") == 4


def test_mongodb_copy_filesystem_folder_relative(caplog, cratedb, mongodb):
    """
    Verify MongoDB -> CrateDB data transfer for all files in a folder, with relative addressing.
    """

    # Reset two database tables.
    cratedb.database.run_sql('DROP TABLE IF EXISTS testdrive."books-canonical";')
    cratedb.database.run_sql('DROP TABLE IF EXISTS testdrive."books-relaxed";')

    # Define source and target URLs.
    fs_resource = "file+bson:./tests/io/mongodb/*.ndjson"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    # Run transfer command.
    mongodb_copy(
        fs_resource,
        cratedb_url,
    )

    # Verify data in target database.
    cratedb.database.refresh_table("testdrive.books-canonical")
    cratedb.database.refresh_table("testdrive.books-relaxed")

    assert cratedb.database.count_records("testdrive.books-canonical") == 4
    assert cratedb.database.count_records("testdrive.books-relaxed") == 4


def test_mongodb_copy_filesystem_json_relaxed_success(caplog, cratedb):
    """
    Verify MongoDB Extended JSON -> CrateDB data transfer.
    """

    # Define source and target URLs.
    json_resource = "file+bson:./tests/io/mongodb/books-relaxed.ndjson"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Run transfer command.
    mongodb_copy(json_resource, cratedb_url)

    # Verify metadata in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 4

    # Verify content in target database.
    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo WHERE data['_id'] = 1;", records=True)
    assert results[0]["data"]["authors"] == [
        "W. Frank Ableson",
        "Charlie Collins",
        "Robi Sen",
    ]

    # Verify schema in target database.
    type_result = cratedb.database.run_sql(
        "SELECT pg_typeof(data['publishedDate']) AS type FROM testdrive.demo;", records=True
    )
    timestamp_type = type_result[0]["type"]
    assert timestamp_type == "bigint"


def test_mongodb_copy_filesystem_json_relaxed_warning(caplog, cratedb):
    """
    Verify MongoDB Extended JSON -> CrateDB data transfer, which should omit a warning on an invalid record.
    """

    # Define source and target URLs.
    json_resource = "file+bson:./tests/io/mongodb/mixed.ndjson"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Run transfer command.
    mongodb_copy(json_resource, cratedb_url)

    # Verify metadata in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 2

    assert "Dynamic nested arrays are not supported" in caplog.text


def test_mongodb_copy_filesystem_json_canonical(caplog, cratedb):
    """
    Verify MongoDB Extended JSON -> CrateDB data transfer.
    """

    # Define source and target URLs.
    json_resource = "file+bson:./tests/io/mongodb/books-canonical.ndjson"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Run transfer command.
    mongodb_copy(json_resource, cratedb_url)

    # Verify metadata in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 4

    # Verify content in target database.
    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo WHERE data['_id'] = 1;", records=True)
    assert results[0]["data"]["authors"] == [
        "W. Frank Ableson",
        "Charlie Collins",
        "Robi Sen",
    ]

    # Verify schema in target database.
    type_result = cratedb.database.run_sql(
        "SELECT pg_typeof(data['publishedDate']) AS type FROM testdrive.demo;", records=True
    )
    timestamp_type = type_result[0]["type"]
    assert timestamp_type == "bigint"


def test_mongodb_copy_filesystem_bson(caplog, cratedb):
    """
    Verify MongoDB BSON -> CrateDB data transfer.
    """

    # Define source and target URLs.
    json_resource = "file+bson:./tests/io/mongodb/books.bson.gz"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Run transfer command.
    mongodb_copy(json_resource, cratedb_url)

    # Verify metadata in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 4

    # Verify content in target database.
    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo WHERE data['_id'] = 1;", records=True)
    assert results[0]["data"]["authors"] == [
        "W. Frank Ableson",
        "Charlie Collins",
        "Robi Sen",
    ]

    # Verify schema in target database.
    type_result = cratedb.database.run_sql(
        "SELECT pg_typeof(data['publishedDate']) AS type FROM testdrive.demo;", records=True
    )
    timestamp_type = type_result[0]["type"]
    assert timestamp_type == "bigint"


def test_mongodb_copy_http_json_relaxed(caplog, cratedb):
    """
    Verify MongoDB Extended JSON -> CrateDB data transfer, when source file is on HTTP.
    """

    # Define source and target URLs.
    json_resource = "https+bson://github.com/ozlerhakan/mongodb-json-files/raw/master/datasets/books.json"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Run transfer command.
    transformation = TransformationProject().add(
        CollectionTransformation(
            address=CollectionAddress(container="datasets", name="books"),
            pre=MokshaTransformation().jq(".[] |= (._id |= tostring)"),
        )
    )
    mongodb_copy(json_resource, cratedb_url, transformation=transformation)

    # Verify metadata in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 431

    # Verify content in target database.
    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo WHERE data['_id'] = 1;", records=True)
    assert results[0]["data"]["authors"] == [
        "W. Frank Ableson",
        "Charlie Collins",
        "Robi Sen",
    ]
