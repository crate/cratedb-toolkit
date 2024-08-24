import base64
import json
import os
import sys

import pytest

pytestmark = pytest.mark.kinesis

pytest.importorskip("commons_codec", reason="Only works with commons-codec installed")

from commons_codec.transform.dynamodb import DynamoDBCDCTranslator  # noqa: E402

DYNAMODB_CDC_INSERT_NESTED = {
    "awsRegion": "us-east-1",
    "eventID": "b581c2dc-9d97-44ed-94f7-cb77e4fdb740",
    "eventName": "INSERT",
    "userIdentity": None,
    "recordFormat": "application/json",
    "tableName": "table-testdrive-nested",
    "dynamodb": {
        "ApproximateCreationDateTime": 1720800199717446,
        "Keys": {"id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"}},
        "NewImage": {
            "id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"},
            "data": {"M": {"temperature": {"N": "42.42"}, "humidity": {"N": "84.84"}}},
            "meta": {"M": {"timestamp": {"S": "2024-07-12T01:17:42"}, "device": {"S": "foo"}}},
            "string_set": {"SS": ["location_1"]},
            "number_set": {"NS": [1, 2, 3, 0.34]},
            "binary_set": {"BS": ["U3Vubnk="]},
            "somemap": {
                "M": {
                    "test": {"N": 1},
                    "test2": {"N": 2},
                }
            },
        },
        "SizeBytes": 156,
        "ApproximateCreationDateTimePrecision": "MICROSECOND",
    },
    "eventSource": "aws:dynamodb",
}

DYNAMODB_CDC_MODIFY_NESTED = {
    "awsRegion": "us-east-1",
    "eventID": "24757579-ebfd-480a-956d-a1287d2ef707",
    "eventName": "MODIFY",
    "userIdentity": None,
    "recordFormat": "application/json",
    "tableName": "foo",
    "dynamodb": {
        "ApproximateCreationDateTime": 1720742302233719,
        "Keys": {"id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"}},
        "NewImage": {
            "id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"},
            "device": {"M": {"id": {"S": "bar"}, "serial": {"N": 12345}}},
            "tags": {"L": [{"S": "foo"}, {"S": "bar"}]},
            "empty_map": {"M": {}},
            "empty_list": {"L": []},
            "timestamp": {"S": "2024-07-12T01:17:42"},
            "string_set": {"SS": ["location_1"]},
            "number_set": {"NS": [1, 2, 3, 0.34]},
            "binary_set": {"BS": ["U3Vubnk="]},
            "somemap": {
                "M": {
                    "test": {"N": 1},
                    "test2": {"N": 2},
                }
            },
            "list_of_objects": {"L": [{"M": {"foo": {"S": "bar"}}}, {"M": {"baz": {"S": "qux"}}}]},
        },
        "OldImage": {
            "NOTE": "This event does not match the INSERT record",
            "humidity": {"N": "84.84"},
            "temperature": {"N": "42.42"},
            "location": {"S": "Sydney"},
            "timestamp": {"S": "2024-07-12T01:17:42"},
            "device": {"M": {"id": {"S": "bar"}, "serial": {"N": 12345}}},
        },
        "SizeBytes": 161,
        "ApproximateCreationDateTimePrecision": "MICROSECOND",
    },
    "eventSource": "aws:dynamodb",
}


@pytest.fixture
def reset_handler():
    try:
        del sys.modules["cratedb_toolkit.io.processor.kinesis_lambda"]
    except KeyError:
        pass


def test_processor_kinesis_dms_no_records(reset_handler, mocker, caplog):
    """
    Roughly verify that the unified Lambda handler works with AWS DMS.
    """

    # Configure environment variables.
    handler_environment = {
        "MESSAGE_FORMAT": "dms",
    }
    mocker.patch.dict(os.environ, handler_environment)

    from cratedb_toolkit.io.processor.kinesis_lambda import handler

    event = {"Records": []}
    handler(event, None)

    assert "Successfully processed 0 records" in caplog.messages


def test_processor_kinesis_dynamodb_insert_update(cratedb, reset_handler, mocker, caplog):
    """
    Roughly verify that the unified Lambda handler works with AWS DynamoDB.
    """

    # Define target table name.
    table_name = '"testdrive"."demo"'

    # Create target table.
    cratedb.database.run_sql(DynamoDBCDCTranslator(table_name=table_name).sql_ddl)

    # Configure Lambda processor per environment variables.
    handler_environment = {
        "CRATEDB_SQLALCHEMY_URL": cratedb.get_connection_url(),
        "MESSAGE_FORMAT": "dynamodb",
        "CRATEDB_TABLE": table_name,
    }
    mocker.patch.dict(os.environ, handler_environment)

    from cratedb_toolkit.io.processor.kinesis_lambda import handler

    # Define two CDC events: INSERT and UPDATE.
    # They have to be conveyed separately because CrateDB needs a
    # `REFRESH TABLE` operation between them.
    event = {
        "Records": [
            wrap_kinesis(DYNAMODB_CDC_INSERT_NESTED),
            wrap_kinesis(DYNAMODB_CDC_MODIFY_NESTED),
        ]
    }

    # Run transfer command.
    handler(event, None)

    # Verify outcome of processor, per validating log output.
    assert "Successfully processed 2 records" in caplog.messages

    # Verify data in target database.
    assert cratedb.database.count_records(table_name) == 1
    results = cratedb.database.run_sql(f"SELECT * FROM {table_name}", records=True)  # noqa: S608
    assert results[0]["data"]["list_of_objects"] == [{"foo": "bar"}, {"baz": "qux"}]


def wrap_kinesis(data):
    """
    Wrap a CDC event into a Kinesis message, to satisfy the interface of the Lambda processor.
    """
    return {
        "eventID": "shardId-000000000006:49590338271490256608559692538361571095921575989136588898",
        "kinesis": {
            "sequenceNumber": "49590338271490256608559692538361571095921575989136588898",
            "data": base64.b64encode(json.dumps(data).encode("utf-8")),
        },
    }
