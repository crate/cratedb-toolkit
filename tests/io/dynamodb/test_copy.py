from unittest.mock import Mock

import pytest

from cratedb_toolkit.io.dynamodb.copy import DynamoDBFullLoad

pytestmark = pytest.mark.dynamodb


def test_dynamodb_copy_basic_success(caplog, cratedb, dynamodb, dynamodb_test_manager):
    """
    Verify a basic `DynamoDBFullLoad` works as expected.
    """

    data_in = {
        "Id": {"N": "101"},
        "Name": {"S": "Hotzenplotz"},
    }
    record_out = {
        "pk": {
            "Id": 101.0,
        },
        "data": {
            "Name": "Hotzenplotz",
        },
        "aux": {},
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
    assert results[0] == record_out


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
        {"pk": {"Id": 1}, "data": {"name": "Foo"}, "aux": {}},
        {"pk": {"Id": 2}, "data": {"name": "Bar", "nested_array": [[1.0, 2.0]]}, "aux": {}},
        {"pk": {"Id": 3}, "data": {"name": "Baz"}, "aux": {}},
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
    assert cratedb.database.count_records("testdrive.demo") == 3

    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo ORDER BY pk['Id'];", records=True)  # noqa: S608
    assert results == data_out


def test_dynamodb_copy_verify_batch_size(caplog, cratedb, dynamodb, dynamodb_test_manager):
    """
    Verify that the `batch-size` parameter on the DynamoDB URL works as expected.
    """

    ddb_document = {
        "Id": {"N": "101"},
        "Name": {"S": "Hotzenplotz"},
    }

    # Define source and target URLs.
    dynamodb_url = f"{dynamodb.get_connection_url_dynamodb()}/demo?region=us-east-1&batch-size=1234"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Define transfer.
    table_loader = DynamoDBFullLoad(dynamodb_url=dynamodb_url, cratedb_url=cratedb_url)

    # Define a mock for the `.scan()` method.
    mock_scan = Mock(return_value={"Items": [ddb_document], "Count": 1})
    table_loader.dynamodb_adapter.dynamodb_client.scan = mock_scan

    # Invoke transfer.
    table_loader.start()

    # Verify invocation of `.scan()` command.
    mock_scan.assert_called_with(TableName="demo", ConsistentRead=False, Limit=1234)
