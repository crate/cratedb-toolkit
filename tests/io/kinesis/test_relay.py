from pathlib import Path
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

from cratedb_toolkit.io.kinesis.model import RecipeDefinition
from cratedb_toolkit.io.kinesis.relay import KinesisRelay
from tests.io.kinesis.data import DMS_CDC_CREATE_TABLE, DMS_CDC_INSERT_BASIC
from tests.io.test_awslambda import wrap_kinesis

pytestmark = pytest.mark.kinesis

pytest.importorskip("commons_codec", reason="Only works with commons-codec installed")


def test_kinesis_earliest_dms_cdc_ddl_dml_universal(caplog, cratedb, kinesis):
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

    # Define transformation file.
    transformation_file = Path("./examples/cdc/aws/dms-load-schema-universal.yaml")

    # Initialize table loader.
    recipe = RecipeDefinition.from_yaml(transformation_file.read_text())
    table_loader = KinesisRelay(kinesis_url=kinesis_url, cratedb_url=cratedb_url, recipe=recipe)

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


def test_kinesis_relay_write_failure_propagates(caplog, cratedb, kinesis):
    """
    Verify that database write failures (OperationalError, ProgrammingError) propagate
    instead of being silently swallowed.
    """

    # Define source and target URLs.
    kinesis_url = (
        f"{kinesis.get_connection_url_kinesis()}?region=us-east-1&create=true&buffer-time=0.01&idle-sleep=0.01"
    )
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    # Define CDC events: CREATE TABLE + INSERT.
    events = [
        wrap_kinesis(DMS_CDC_CREATE_TABLE),
        wrap_kinesis(DMS_CDC_INSERT_BASIC),
    ]

    # Define transformation file.
    transformation_file = Path("./examples/cdc/aws/dms-load-schema-universal.yaml")

    # Initialize table loader.
    recipe = RecipeDefinition.from_yaml(transformation_file.read_text())
    table_loader = KinesisRelay(kinesis_url=kinesis_url, cratedb_url=cratedb_url, recipe=recipe)

    # Populate the Kinesis stream with data.
    for event in events:
        table_loader.kinesis_adapter.produce(event)

    # Patch connection.execute to raise OperationalError on the INSERT (not the DDL).
    original_execute = sa.Connection.execute

    def failing_execute(self, statement, *args, **kwargs):
        stmt_text = statement.text if hasattr(statement, "text") else str(statement)
        if "INSERT" in stmt_text.upper() or "UPSERT" in stmt_text.upper():
            raise OperationalError("test", {}, Exception("connection lost"))
        return original_execute(self, statement, *args, **kwargs)

    # Verify that the write failure propagates instead of being swallowed.
    with patch.object(sa.Connection, "execute", failing_execute):
        with pytest.raises(OperationalError, match="connection lost"):
            table_loader.start(once=True)

    # Verify that stop() cleaned up the connection.
    assert not hasattr(table_loader, "connection"), "connection should be cleaned up after start() failure"


def test_kinesis_relay_corrupt_record_skipped(caplog, cratedb, kinesis):
    """
    Verify that a record the translator cannot process is logged and skipped,
    not raised to the caller.
    """

    # Define source and target URLs.
    kinesis_url = (
        f"{kinesis.get_connection_url_kinesis()}?region=us-east-1&create=true&buffer-time=0.01&idle-sleep=0.01"
    )
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    # Define CDC events: CREATE TABLE, a corrupt record, then a valid INSERT.
    corrupt_record = {"garbage_key": "garbage_value"}
    events = [
        wrap_kinesis(DMS_CDC_CREATE_TABLE),
        wrap_kinesis(corrupt_record),
        wrap_kinesis(DMS_CDC_INSERT_BASIC),
    ]

    # Define transformation file.
    transformation_file = Path("./examples/cdc/aws/dms-load-schema-universal.yaml")

    # Initialize table loader.
    recipe = RecipeDefinition.from_yaml(transformation_file.read_text())
    table_loader = KinesisRelay(kinesis_url=kinesis_url, cratedb_url=cratedb_url, recipe=recipe)

    # Populate the Kinesis stream with data.
    for event in events:
        table_loader.kinesis_adapter.produce(event)

    # Run transfer command, consuming once not forever. Should complete without raising.
    table_loader.start(once=True)

    # Verify the valid INSERT was still processed despite the corrupt record.
    table_name = '"testdrive"."demo"'
    cratedb.database.refresh_table(table_name)
    assert cratedb.database.count_records(table_name) == 1

    # Verify the corrupt record was logged.
    assert "Translating CDC event to SQL failed" in caplog.text
