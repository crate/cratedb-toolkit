# ruff: noqa: E402
import json
import os.path
import re
import shutil
import tarfile
from importlib.resources import files
from pathlib import Path

import pytest

import tests.cfr

pymongo = pytest.importorskip("polars", reason="Skipping tests because polars is not installed")

from click.testing import CliRunner

import tests
from cratedb_toolkit.cfr.cli import cli
from tests.conftest import TESTDRIVE_DATA_SCHEMA

pytestmark = pytest.mark.cfr


def filenames(path: Path):
    return sorted([item.name for item in path.iterdir()])


def linecount(path: Path) -> int:
    with path.open("r", encoding="utf-8") as f:
        return sum(1 for _ in f)


EXPORT_SUMMARY_PATTERN = r"Successfully exported \d+ tables from sys, information_schema"


def test_cfr_sys_export_success(cratedb, click_kwargs, tmp_path, caplog):
    """
    Verify `ctk cfr sys-export` works.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi, "CFR_TARGET": str(tmp_path)}, **click_kwargs)
    result = runner.invoke(
        cli,
        args="--debug sys-export",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Verify log output.
    assert "Exporting system tables to" in caplog.text
    assert re.search(EXPORT_SUMMARY_PATTERN, caplog.text), "Log message missing"

    # Verify the outcome. The reported path is the bundle root, so everything
    # the bundle carries is reachable from it.
    path = Path(json.loads(result.stdout)["path"])
    assert filenames(path) == ["ddl", "information_schema", "manifest.json", "sys"]
    assert filenames(path / "sys") == ["data", "schema"]

    schema_files = filenames(path / "sys" / "schema")
    data_files = filenames(path / "sys" / "data")

    assert len(schema_files) >= 19
    assert len(data_files) >= 10


def test_cfr_sys_export_to_archive_file(cratedb, click_kwargs, tmp_path, caplog):
    """
    Verify `ctk cfr sys-export some-file.tgz` works.
    """

    target = os.path.join(tmp_path, "cluster-data.tgz")

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi, "CFR_TARGET": str(tmp_path)}, **click_kwargs)
    result = runner.invoke(
        cli,
        args=f"--debug sys-export {target}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Verify log output.
    assert "Exporting system tables to" in caplog.text
    assert re.search(EXPORT_SUMMARY_PATTERN, caplog.text), "Log message missing"

    # Verify the outcome.
    path = Path(json.loads(result.stdout)["path"])
    assert "cluster-data.tgz" in path.name

    # Classify by the `sys/` subtree's own directories, not by substring match.
    data_files = []
    schema_files = []
    with tarfile.open(path, "r") as tar:
        for name in tar.getnames():
            if "/sys/data/" in name:
                data_files.append(name)
            elif "/sys/schema/" in name:
                schema_files.append(name)

    assert len(schema_files) >= 19
    assert len(data_files) >= 10


def test_cfr_sys_export_failure(cratedb, click_kwargs, tmp_path, caplog):
    """
    Verify `ctk cfr sys-export` failure.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": "crate://foo.bar/", "CFR_TARGET": str(tmp_path)}, **click_kwargs)
    result = runner.invoke(
        cli,
        args="--debug sys-export",
        catch_exceptions=False,
    )
    assert result.exit_code == 1, result.output

    # Verify log output.
    assert "Failed to establish a new connection" in caplog.text or "Failed to resolve" in caplog.text
    assert result.output == ""


def test_cfr_sys_export_ensure_table_name_is_quoted(cratedb, click_kwargs, tmp_path, caplog):
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi, "CFR_TARGET": str(tmp_path)}, **click_kwargs)
    result = runner.invoke(
        cli,
        args="--debug sys-export",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    path = Path(json.loads(result.stdout)["path"])
    sys_cluster_table_schema = path / "sys" / "schema" / "sys-cluster.sql"
    with open(sys_cluster_table_schema, "r") as f:
        content = f.read()
        assert '"sys-cluster"' in content, "Table name missing or not quoted"


def test_cfr_sys_import_success(cratedb, tmp_path, caplog):
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
    shutil.copy(str(sys_operations_schema), schema_path)
    shutil.copy(str(sys_operations_data), data_path)

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi, "CFR_SOURCE": str(tmp_path)})
    result = runner.invoke(
        cli,
        args="--debug sys-import",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Verify log output.
    assert "Importing system tables from" in caplog.text
    assert re.search(r"Successfully imported \d+ system tables", caplog.text), "Log message missing"

    # Verify the outcome.
    results = cratedb.database.run_sql("SHOW TABLES", records=True)
    assert {"table_name": "sys-operations"} in results

    cratedb.database.run_sql('REFRESH TABLE "sys-operations"')
    assert cratedb.database.count_records("sys-operations") == 1


def test_cfr_sys_import_restores_every_exported_row(cratedb, click_kwargs, tmp_path, caplog):
    """
    Every row a bundle carries is readable from the restored table after `ctk cfr sys-import`.
    """

    adapter = cratedb.database
    probe = adapter.quote_relation_name(f"{TESTDRIVE_DATA_SCHEMA}.segment_source")

    # Spread the cluster over several Lucene segments, so `sys.segments` contributes
    # more than one row to the bundle. CrateDB reports a rejected row of a single-row
    # bulk request as an HTTP error, but a rejected row of a multi-row request as a
    # per-row result underneath an HTTP 200, and only the latter can pass unnoticed.
    adapter.run_sql(f"CREATE TABLE {probe} (id INT, name TEXT) CLUSTERED INTO 2 SHARDS")
    for identifier in range(3):
        adapter.run_sql(f"INSERT INTO {probe} (id, name) VALUES ({identifier}, 'probe')")  # noqa: S608
        adapter.run_sql(f"REFRESH TABLE {probe}")

    # Capture the cluster into a bundle.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": adapter.dburi, "CFR_TARGET": str(tmp_path)}, **click_kwargs)
    result = runner.invoke(cli, args="--debug sys-export", catch_exceptions=False)
    assert result.exit_code == 0, result.output

    bundle = Path(json.loads(result.stdout)["path"]) / "sys"
    exported = {item.stem: linecount(item) for item in sorted((bundle / "data").glob("*.jsonl"))}
    assert exported["sys-segments"] > 1, "Cluster did not hold enough segments to exercise the bulk path"

    # Restore the bundle onto a blank canvas.
    cratedb.reset(tables=list(exported))
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": adapter.dburi, "CFR_SOURCE": str(bundle)}, **click_kwargs)
    result = runner.invoke(cli, args="--debug sys-import", catch_exceptions=False)
    assert result.exit_code == 0, result.output

    restored = {}
    for tablename in exported:
        adapter.run_sql(f"REFRESH TABLE {adapter.quote_relation_name(tablename)}")
        restored[tablename] = adapter.count_records(tablename)

    assert restored == exported


def test_cfr_sys_import_reports_rejected_rows(cratedb, click_kwargs, tmp_path, caplog):
    """
    A table whose rows the cluster refuses is not reported as imported.

    A bundle carries the column definitions its exporter rendered, so a restore can
    meet a definition that cannot hold the data stored next to it. `sys.segments`
    is one such case: its `attributes` keys are named after Lucene codec settings
    and carry dots, which CrateDB forbids in the sub-columns of a dynamic object.
    """

    cratedb.reset(tables=["sys-segments"])

    assets_path = files(tests.cfr) / "assets"
    schema_path = tmp_path / "schema"
    data_path = tmp_path / "data"
    schema_path.mkdir()
    data_path.mkdir()
    shutil.copy(str(assets_path / "sys-segments.sql"), schema_path)
    shutil.copy(str(assets_path / "sys-segments.jsonl"), data_path)

    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi, "CFR_SOURCE": str(tmp_path)}, **click_kwargs)
    result = runner.invoke(cli, args="--debug sys-import", catch_exceptions=False)

    assert result.exit_code == 1
    assert "sys-segments" in caplog.text
    assert not re.search(r"Successfully imported [1-9]\d* system tables", caplog.text)


def test_cfr_sys_import_restores_oversized_cluster_state(cratedb, click_kwargs, tmp_path):
    """
    A restored `sys-cluster` holds a state longer than Lucene's maximum term length.

    The encoded cluster state grows with the number of nodes, tables and shards, so how
    far past 32766 bytes it reaches is a property of whichever cluster is under test.
    The length is supplied here instead, so the contract holds on any of them.
    """

    adapter = cratedb.database

    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": adapter.dburi, "CFR_TARGET": str(tmp_path)}, **click_kwargs)
    result = runner.invoke(cli, args="--debug sys-export", catch_exceptions=False)
    assert result.exit_code == 0, result.output

    bundle = Path(json.loads(result.stdout)["path"]) / "sys"
    if "state" not in (bundle / "schema" / "sys-cluster.sql").read_text():
        pytest.skip("This CrateDB version does not report a cluster state")

    cratedb.reset(tables=[item.stem for item in (bundle / "data").glob("*.jsonl")])
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": adapter.dburi, "CFR_SOURCE": str(bundle)}, **click_kwargs)
    result = runner.invoke(cli, args="--debug sys-import", catch_exceptions=False)
    assert result.exit_code == 0, result.output

    oversized = "s" * 40_000
    adapter.run_sql('INSERT INTO "sys-cluster" (state) VALUES (:state)', {"state": oversized})
    adapter.run_sql('REFRESH TABLE "sys-cluster"')
    records = adapter.run_sql(
        'SELECT state FROM "sys-cluster" WHERE length(state) = :length',
        {"length": len(oversized)},
        records=True,
    )
    assert [record["state"] for record in records] == [oversized]


def test_cfr_sys_import_failure(cratedb, click_kwargs, tmp_path, caplog):
    """
    Verify `ctk cfr sys-import` failure.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": "crate://foo.bar/", "CFR_SOURCE": str(tmp_path)}, **click_kwargs)
    result = runner.invoke(
        cli,
        args="--debug sys-import",
        catch_exceptions=False,
    )
    assert result.exit_code == 1, result.output

    # Verify log output.
    assert "Failed to establish a new connection" in caplog.text or "Failed to resolve" in caplog.text
    assert result.output == ""
