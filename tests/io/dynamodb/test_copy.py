import pytest

from cratedb_toolkit.io.dynamodb.copy import DynamoDBFullLoad

pytestmark = pytest.mark.dynamodb


def test_dynamodb_copy_basic_success(caplog, cratedb, dynamodb, dynamodb_test_manager):
    """
    Verify a basic `DynamoDBFullLoad` works as expected.
    """

    data_in = {
        "Id": {"N": "101"},
    }
    data_out = {
        "Id": 101.0,
    }

    # Define source and target URLs.
    dynamodb_url = f"{dynamodb.get_connection_url_dynamodb()}/demo?region=us-east-1"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Populate source database with data.
    dynamodb_test_manager.load_records(table_name="demo", records=[data_in])

    # Run transfer command.
    table_loader = DynamoDBFullLoad(dynamodb_url=dynamodb_url, cratedb_url=cratedb_url)
    table_loader.start()

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 1

    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo;", records=True)  # noqa: S608
    assert results[0]["data"] == data_out


def test_dynamodb_copy_basic_warning(caplog, cratedb, dynamodb, dynamodb_test_manager):
    """
    Verify a basic `DynamoDBFullLoad` works as expected, this time omitting a warning on an invalid record.
    """

    data_in = [
        {"Id": {"N": "1"}, "name": {"S": "Foo"}},
        {"Id": {"N": "2"}, "name": {"S": "Bar"}, "nested_array": {"L": [{"L": [{"N": "1"}, {"N": "2"}]}]}},
        {"Id": {"N": "3"}, "name": {"S": "Baz"}},
    ]
    data_out = [
        {"data": {"Id": 1, "name": "Foo"}, "aux": {}},
        {"data": {"Id": 3, "name": "Baz"}, "aux": {}},
    ]

    # Define source and target URLs.
    dynamodb_url = f"{dynamodb.get_connection_url_dynamodb()}/demo?region=us-east-1"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Populate source database with data.
    dynamodb_test_manager.load_records(table_name="demo", records=data_in)

    # Run transfer command.
    table_loader = DynamoDBFullLoad(dynamodb_url=dynamodb_url, cratedb_url=cratedb_url)
    table_loader.start()

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 2

    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo ORDER BY data['Id'];", records=True)  # noqa: S608
    assert results == data_out

    assert "Dynamic nested arrays are not supported" in caplog.text
