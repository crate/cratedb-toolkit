import json
from pathlib import Path

import orjsonl
import pytest
from click.testing import CliRunner

from cratedb_toolkit.cli import cli
from tests.io.kinesis.data import DMS_CDC_CREATE_TABLE, DMS_CDC_INSERT_BASIC

pytestmark = pytest.mark.kinesis


def test_kinesis_dms_stream_universal(caplog, cratedb, kinesis, kinesis_test_manager):
    """
    CLI test: Invoke `ctk load table` for DMS over Kinesis from a stream, using universal mapping strategy.
    """

    # Define transformation file.
    transformation_file = Path("./examples/cdc/aws/dms-load-schema-universal.yaml")

    # Define source and target URLs.
    kinesis_url = (
        f"{kinesis.get_connection_url_kinesis()}?region=us-east-1"
        f"&buffer-time=0.01&idle-sleep=0.01&create=true&once=true"
    )
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
        args=f"load table {kinesis_url} --transformation='{transformation_file}'",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 1
    data = cratedb.database.run_sql('SELECT * FROM "testdrive".demo', records=True)
    assert data == [{"pk": {"id": 393}, "data": {"name": "Test", "attributes": None, "age": 4}, "aux": {}}]


def test_kinesis_dms_stream_direct(caplog, cratedb, kinesis, kinesis_test_manager):
    """
    CLI test: Invoke `ctk load table` for DMS over Kinesis from a stream, using direct mapping strategy.
    """

    # Define transformation file.
    transformation_file = Path("./examples/cdc/aws/dms-load-schema-direct.yaml")

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
        args=f"load table {kinesis_url} --transformation='{transformation_file}'",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 1
    data = cratedb.database.run_sql('SELECT * FROM "testdrive".demo', records=True)
    assert data == [{"id": 393, "name": "Test", "attributes": None, "age": 4}]


def test_kinesis_dms_file_universal(caplog, cratedb):
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


def test_kinesis_dms_load_without_ddl_universal(caplog, tmp_path, cratedb):
    """
    Validate DMS over Kinesis with a pre-defined target schema, using the universal mapping.

    While this variant doesn't need a DDL, it needs to be supplied with
    primary key and column type information manually.
    """

    # Supply SQL DDL manually.
    cratedb.database.run_sql(
        'CREATE TABLE "testdrive-data".foobar '
        "(pk OBJECT(STRICT) AS (rowid BIGINT PRIMARY KEY), "
        "data OBJECT(DYNAMIC), "
        "aux OBJECT(IGNORED))",
        records=True,
    )

    # Define transformation file.
    transformation_file = Path("./examples/cdc/aws/dms-load-schema-universal-noddl.yaml")

    # Write a single event as a dump file to disk.
    dms_load_event = json.loads(Path("./examples/cdc/aws/dms-data-load.json").read_text())
    stream_dump_file = tmp_path / "dms-load.ndjson"
    orjsonl.save(stream_dump_file, [dms_load_event])

    # Define source and target URLs.
    kinesis_url = f"kinesis+dms://{stream_dump_file.absolute()}"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive-data"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb_url})
    result = runner.invoke(
        cli,
        args=f"load table {kinesis_url} --transformation='{transformation_file}'",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive-data.foobar") is True
    assert cratedb.database.refresh_table("testdrive-data.foobar") is True
    assert cratedb.database.count_records("testdrive-data.foobar") == 1
    data = cratedb.database.run_sql('SELECT * FROM "testdrive-data".foobar', records=True)
    assert data == [{"pk": {"rowid": 1}, "data": {"resource": {"temperature": 42.42}}, "aux": {}}]


def test_kinesis_dms_load_without_ddl_direct(caplog, tmp_path, cratedb):
    """
    Validate DMS over Kinesis with a pre-defined target schema, using the direct mapping.

    While this variant doesn't need a DDL, it needs to be supplied with
    primary key and column type information manually.
    """

    # Supply SQL DDL manually.
    cratedb.database.run_sql(
        'CREATE TABLE "testdrive-data".foobar (rowid BIGINT PRIMARY KEY, resource OBJECT(DYNAMIC))',
        records=True,
    )

    # Define transformation file.
    transformation_file = Path("./examples/cdc/aws/dms-load-schema-direct-noddl.yaml")

    # Write a single event as a dump file to disk.
    dms_load_event = json.loads(Path("./examples/cdc/aws/dms-data-load.json").read_text())
    stream_dump_file = tmp_path / "dms-load.ndjson"
    orjsonl.save(stream_dump_file, [dms_load_event])

    # Define source and target URLs.
    kinesis_url = f"kinesis+dms://{stream_dump_file.absolute()}"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive-data"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb_url})
    result = runner.invoke(
        cli,
        args=f"load table {kinesis_url} --transformation='{transformation_file}'",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive-data.foobar") is True
    assert cratedb.database.refresh_table("testdrive-data.foobar") is True
    assert cratedb.database.count_records("testdrive-data.foobar") == 1
    data = cratedb.database.run_sql('SELECT * FROM "testdrive-data".foobar', records=True)
    assert data == [{"rowid": 1, "resource": {"temperature": 42.42}}]
