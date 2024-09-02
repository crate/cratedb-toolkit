import pytest
from botocore.exceptions import ParamValidationError
from yarl import URL

from cratedb_toolkit.io.dynamodb.adapter import DynamoDBAdapter

pytestmark = pytest.mark.dynamodb


RECORD = {
    "Id": {"N": "101"},
}


def test_adapter_scan_success(dynamodb):
    dynamodb_url = f"{dynamodb.get_connection_url()}/demo?region=us-east-1"
    adapter = DynamoDBAdapter(URL(dynamodb_url))
    adapter.scan("foo")


def test_adapter_scan_failure_consistent_read(dynamodb):
    """
    Check supplying invalid parameters to `DynamoDBAdapter` fails as expected.
    """
    dynamodb_url = f"{dynamodb.get_connection_url()}/demo?region=us-east-1"
    adapter = DynamoDBAdapter(URL(dynamodb_url))

    with pytest.raises(ParamValidationError) as ex:
        next(adapter.scan("demo", consistent_read=-42, on_error="raise"))
    assert ex.match("Parameter validation failed:\nInvalid type for parameter ConsistentRead, value: -42.*")


def test_adapter_scan_failure_page_size(dynamodb):
    """
    Check supplying invalid parameters to `DynamoDBAdapter` fails as expected.
    """
    dynamodb_url = f"{dynamodb.get_connection_url()}/demo?region=us-east-1"
    adapter = DynamoDBAdapter(URL(dynamodb_url))

    with pytest.raises(ParamValidationError) as ex:
        next(adapter.scan("demo", page_size=-1, on_error="raise"))
    assert ex.match("Parameter validation failed:\nInvalid value for parameter Limit, value: -1, valid min value: 1")
