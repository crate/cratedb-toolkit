from pathlib import Path

import pytest
from click.testing import CliRunner

from cratedb_toolkit.cli import cli
from tests.io.kinesis.data import DMS_CDC_CREATE_TABLE, DMS_CDC_INSERT_BASIC

pytestmark = pytest.mark.kinesis


def test_kinesis_dms_stream(caplog, cratedb, kinesis, kinesis_test_manager):
    """
    CLI test: Invoke `ctk load table` for DMS over Kinesis from a stream.
    """

    # Define source and target URLs.
    kinesis_url = f"{kinesis.get_connection_url_kinesis()}?region=us-east-1&create=true&once=true"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    # Populate source stream with sample dataset.
    # Define two CDC events: CREATE TABLE and INSERT.
    events = [
        DMS_CDC_CREATE_TABLE,
        DMS_CDC_INSERT_BASIC,
    ]
    kinesis_test_manager.load_events(events)

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb_url})
    result = runner.invoke(
        cli,
        args=f"load table {kinesis_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 1


def test_kinesis_dms_file(caplog, cratedb):
    """
    CLI test: Invoke `ctk load table` for DMS over Kinesis from a dump file.
    """

    stream_dump_file = Path("./examples/cdc/aws/postgresql-pglogical-dms-kinesis.ndjson")

    # Define source and target URLs.
    kinesis_url = f"kinesis+dms://{stream_dump_file.absolute()}"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb_url})
    result = runner.invoke(
        cli,
        args=f"load table {kinesis_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    assert cratedb.database.table_exists("dms.awsdms_history") is True
    assert cratedb.database.table_exists("dms.awsdms_status") is True
    assert cratedb.database.table_exists("testdrive.diamonds") is True
    assert cratedb.database.table_exists("testdrive.functional_alltypes") is True
    assert cratedb.database.table_exists("testdrive.win") is True

    assert cratedb.database.refresh_table("testdrive.diamonds") is True
    assert cratedb.database.refresh_table("testdrive.functional_alltypes") is True
    assert cratedb.database.refresh_table("testdrive.win") is True

    assert cratedb.database.count_records("testdrive.diamonds") == 5
    assert cratedb.database.count_records("testdrive.functional_alltypes") == 5
    assert cratedb.database.count_records("testdrive.win") == 5
