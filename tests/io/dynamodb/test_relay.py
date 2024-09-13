import threading
import time

import botocore
import pytest

from cratedb_toolkit.io.kinesis.relay import KinesisRelay
from tests.io.test_processor import DYNAMODB_CDC_INSERT_NESTED, DYNAMODB_CDC_MODIFY_NESTED, wrap_kinesis

pytestmark = pytest.mark.kinesis

pytest.importorskip("commons_codec", reason="Only works with commons-codec installed")
pytest.importorskip("kinesis", reason="Only works with async-kinesis installed")

from commons_codec.transform.dynamodb import DynamoDBCDCTranslator  # noqa: E402


def test_kinesis_earliest_dynamodb_cdc_insert_update(caplog, cratedb, dynamodb):
    """
    Roughly verify that the AWS DynamoDB CDC processing through Kinesis works as expected.

    This test case consumes the Kinesis Stream from the "earliest" point, i.e. from the beginning.
    No option is configured, because `start=earliest` is the default mode.
    """

    # Define source and target URLs.
    kinesis_url = (
        f"{dynamodb.get_connection_url_kinesis_dynamodb_cdc()}/demo"
        f"?region=us-east-1&create=true&buffer-time=0.01&idle-sleep=0.01"
    )
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Define target table name.
    table_name = '"testdrive"."demo"'

    # Create target table.
    cratedb.database.run_sql(DynamoDBCDCTranslator(table_name=table_name).sql_ddl)

    # Define two CDC events: INSERT and UPDATE.
    events = [
        wrap_kinesis(DYNAMODB_CDC_INSERT_NESTED),
        wrap_kinesis(DYNAMODB_CDC_MODIFY_NESTED),
    ]

    # Initialize table loader.
    table_loader = KinesisRelay(kinesis_url=kinesis_url, cratedb_url=cratedb_url)

    # Delete stream for blank canvas.
    try:
        table_loader.kinesis_adapter.kinesis_client.delete_stream(StreamName="demo", EnforceConsumerDeletion=True)
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    # LocalStack needs a while when deleting the Stream.
    # FIXME: Can this be made more efficient?
    time.sleep(0.5)

    # Populate source database with data.
    for event in events:
        table_loader.kinesis_adapter.produce(event)

    # Run transfer command, consuming once not forever.
    table_loader.start(once=True)

    # Verify data in target database, more specifically that both events have been processed well.
    assert cratedb.database.count_records(table_name) == 1
    results = cratedb.database.run_sql(f"SELECT * FROM {table_name}", records=True)  # noqa: S608
    assert results[0]["data"]["list_of_objects"] == [{"foo": "bar"}, {"baz": "qux"}]


def test_kinesis_latest_dynamodb_cdc_insert_update(caplog, cratedb, dynamodb):
    """
    Roughly verify that the AWS DynamoDB CDC processing through Kinesis works as expected.

    This test case consumes the Kinesis Stream from the "latest" point, i.e. from "now".
    """

    # Define source and target URLs.
    kinesis_url = (
        f"{dynamodb.get_connection_url_kinesis_dynamodb_cdc()}/demo"
        f"?region=us-east-1&create=true&buffer-time=0.01&idle-sleep=0.01&start=latest"
    )
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Define target table name.
    table_name = '"testdrive"."demo"'

    # Create target table.
    cratedb.database.run_sql(DynamoDBCDCTranslator(table_name=table_name).sql_ddl)

    # Define two CDC events: INSERT and UPDATE.
    events = [
        wrap_kinesis(DYNAMODB_CDC_INSERT_NESTED),
        wrap_kinesis(DYNAMODB_CDC_MODIFY_NESTED),
    ]

    # Initialize table loader.
    table_loader = KinesisRelay(kinesis_url=kinesis_url, cratedb_url=cratedb_url)

    # Delete stream for blank canvas.
    try:
        table_loader.kinesis_adapter.kinesis_client.delete_stream(StreamName="demo")
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    # LocalStack needs a while when deleting the Stream.
    # FIXME: Can this be made more efficient instead of waiting multiple times to orchestrate this sequence?
    time.sleep(0.5)

    # Start event processor / stream consumer in separate thread, consuming forever.
    thread = threading.Thread(target=table_loader.start)
    thread.start()
    time.sleep(1)

    # Populate source database with data.
    for event in events:
        table_loader.kinesis_adapter.produce(event)

    # Stop stream consumer.
    table_loader.stop()
    thread.join()

    # Verify data in target database, more specifically that both events have been processed well.
    assert cratedb.database.refresh_table(table_name) is True
    assert cratedb.database.count_records(table_name) == 1
    results = cratedb.database.run_sql(f"SELECT * FROM {table_name}", records=True)  # noqa: S608
    assert results[0]["data"]["list_of_objects"] == [{"foo": "bar"}, {"baz": "qux"}]
