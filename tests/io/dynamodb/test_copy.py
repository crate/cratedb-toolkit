import pytest

from cratedb_toolkit.io.dynamodb.copy import DynamoDBFullLoad

pytestmark = pytest.mark.dynamodb


RECORD = {
    "Id": {"N": "101"},
}


def test_dynamodb_copy_success(caplog, cratedb, dynamodb, dynamodb_test_manager):
    """
    Verify `DynamoDBFullLoad` works as expected.
    """

    # Define source and target URLs.
    dynamodb_url = f"{dynamodb.get_connection_url()}/demo?region=us-east-1"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Populate source database with data.
    dynamodb_test_manager.load_records(table_name="demo", records=[RECORD])

    # Run transfer command.
    table_loader = DynamoDBFullLoad(dynamodb_url=dynamodb_url, cratedb_url=cratedb_url)
    table_loader.start()

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 1

    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo;", records=True)  # noqa: S608
    assert results[0]["data"] == {"Id": 101.0}
