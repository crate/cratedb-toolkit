import pytest

from cratedb_toolkit.io.kinesis.relay import KinesisRelay
from tests.io.kinesis.data import DMS_CDC_CREATE_TABLE, DMS_CDC_INSERT_BASIC
from tests.io.test_processor import wrap_kinesis

pytestmark = pytest.mark.kinesis

pytest.importorskip("commons_codec", reason="Only works with commons-codec installed")


def test_kinesis_earliest_dms_cdc_ddl_dml(caplog, cratedb, kinesis):
    """
    Roughly verify that the AWS DynamoDB CDC processing through Kinesis works as expected.

    This test case consumes the Kinesis Stream from the "earliest" point, i.e. from the beginning.
    No option is configured, because `start=earliest` is the default mode.
    """

    # Define source and target URLs.
    kinesis_url = (
        f"{kinesis.get_connection_url_kinesis()}?region=us-east-1&create=true&buffer-time=0.01&idle-sleep=0.01"
    )
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    # Define two CDC events: INSERT and UPDATE.
    events = [
        wrap_kinesis(DMS_CDC_CREATE_TABLE),
        wrap_kinesis(DMS_CDC_INSERT_BASIC),
    ]

    # Initialize table loader.
    table_loader = KinesisRelay(kinesis_url=kinesis_url, cratedb_url=cratedb_url)

    # Populate the Kinesis stream with data.
    for event in events:
        table_loader.kinesis_adapter.produce(event)

    # Run transfer command, consuming once not forever.
    table_loader.start(once=True)

    # Verify data in the target database, more specifically that both events have been processed well.
    table_name = '"testdrive"."demo"'
    cratedb.database.refresh_table(table_name)
    assert cratedb.database.count_records(table_name) == 1
    results = cratedb.database.run_sql(f"SELECT * FROM {table_name}", records=True)  # noqa: S608
    assert results[0]["data"] == {"name": "Test", "age": 4, "attributes": None}
