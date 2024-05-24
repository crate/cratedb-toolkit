import json
import os.path
import re
import shutil
import sys
import tarfile

import tests

if sys.version_info < (3, 9):
    from importlib_resources import files
else:
    from importlib.resources import files
from pathlib import Path

from click.testing import CliRunner

from cratedb_toolkit.cfr.cli import cli


def filenames(path: Path):
    return sorted([item.name for item in path.iterdir()])


def test_cfr_cli_export_success(cratedb, tmp_path, caplog):
    """
    Verify `ctk cfr sys-export` works.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi, "CFR_TARGET": str(tmp_path)})
    result = runner.invoke(
        cli,
        args="--debug sys-export",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify log output.
    assert "Exporting system tables to" in caplog.text
    assert re.search(r"Successfully exported \d+ system tables", caplog.text), "Log message missing"

    # Verify outcome.
    path = Path(json.loads(result.output)["path"])
    assert filenames(path) == ["data", "schema"]

    schema_files = filenames(path / "schema")
    data_files = filenames(path / "data")

    assert len(schema_files) >= 19
    assert len(data_files) >= 10


def test_cfr_cli_export_to_archive_file(cratedb, tmp_path, caplog):
    """
    Verify `ctk cfr sys-export some-file.tgz` works.
    """

    target = os.path.join(tmp_path, "cluster-data.tgz")

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi, "CFR_TARGET": str(tmp_path)})
    result = runner.invoke(
        cli,
        args=f"--debug sys-export {target}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify log output.
    assert "Exporting system tables to" in caplog.text
    assert re.search(r"Successfully exported \d+ system tables", caplog.text), "Log message missing"

    # Verify outcome.
    path = Path(json.loads(result.output)["path"])
    assert "cluster-data.tgz" in path.name

    data_files = []
    schema_files = []
    with tarfile.open(path, "r") as tar:
        name_list = tar.getnames()
        for name in name_list:
            if "data" in name:
                data_files.append(name)
            elif "schema" in name:
                schema_files.append(name)

    assert len(schema_files) >= 19
    assert len(data_files) >= 10


def test_cfr_cli_export_failure(cratedb, tmp_path, caplog):
    """
    Verify `ctk cfr sys-export` failure.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": "crate://foo.bar/", "CFR_TARGET": str(tmp_path)})
    result = runner.invoke(
        cli,
        args="--debug sys-export",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    # Verify log output.
    assert "Failed to establish a new connection" in caplog.text or "Failed to resolve" in caplog.text
    assert result.output == ""


def test_cfr_cli_import_success(cratedb, tmp_path, caplog):
    """
    Verify `ctk cfr sys-import` works.
    """

    # Blank database canvas.
    imported_system_tables = [
        "sys-allocations",
        "sys-checks",
        "sys-cluster",
        "sys-health",
        "sys-jobs",
        "sys-jobs_log",
        "sys-jobs_metrics",
        "sys-node_checks",
        "sys-nodes",
        "sys-operations",
        "sys-operations_log",
        "sys-privileges",
        "sys-repositories",
        "sys-roles",
        "sys-segments",
        "sys-shards",
        "sys-snapshot_restore",
        "sys-snapshots",
        "sys-users",
    ]
    cratedb.reset(imported_system_tables)

    # Provision filesystem to look like a fake `sys-export` trace.
    assets_path = files(tests.cfr) / "assets"
    sys_operations_schema = assets_path / "sys-operations.sql"
    sys_operations_data = assets_path / "sys-operations.jsonl"
    schema_path = tmp_path / "schema"
    data_path = tmp_path / "data"
    schema_path.mkdir()
    data_path.mkdir()
    shutil.copy(sys_operations_schema, schema_path)
    shutil.copy(sys_operations_data, data_path)

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi, "CFR_SOURCE": str(tmp_path)})
    result = runner.invoke(
        cli,
        args="--debug sys-import",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify log output.
    assert "Importing system tables from" in caplog.text
    assert re.search(r"Successfully imported \d+ system tables", caplog.text), "Log message missing"

    # Verify outcome.
    results = cratedb.database.run_sql("SHOW TABLES", records=True)
    assert results == [{"table_name": "sys-operations"}]

    cratedb.database.run_sql('REFRESH TABLE "sys-operations"')
    assert cratedb.database.count_records("sys-operations") == 1


def test_cfr_cli_import_failure(cratedb, tmp_path, caplog):
    """
    Verify `ctk cfr sys-import` failure.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": "crate://foo.bar/", "CFR_SOURCE": str(tmp_path)})
    result = runner.invoke(
        cli,
        args="--debug sys-import",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    # Verify log output.
    assert "Failed to establish a new connection" in caplog.text or "Failed to resolve" in caplog.text
    assert result.output == ""
