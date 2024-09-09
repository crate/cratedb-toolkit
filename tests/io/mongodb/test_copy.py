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


def test_mongodb_copy_filesystem_json_relaxed(caplog, cratedb):
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

    # FIXME: Why does the "canonical format" yield worse results?
    assert timestamp_type == "text"


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
