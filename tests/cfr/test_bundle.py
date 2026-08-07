# Copyright (c) 2021-2026, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
Tests for what `ctk cfr sys-export` collects beyond the raw `sys.*` tables.

These assert against what the cluster itself reports, rather than absolute table
counts, so they do not become version-specific.
"""

import json
import tarfile
from pathlib import Path

import pytest

pytest.importorskip("polars", reason="Skipping tests because polars is not installed")

from click.testing import CliRunner  # noqa: E402

from cratedb_toolkit.cfr.cli import cli  # noqa: E402
from cratedb_toolkit.cfr.systable import SystemTableExporter  # noqa: E402

pytestmark = pytest.mark.cfr


def run_export(cratedb, click_kwargs, target):
    """
    Invoke `ctk cfr sys-export`, returning the Click result.
    """
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi, "CFR_TARGET": str(target)}, **click_kwargs)
    return runner.invoke(cli, args=f"--debug sys-export {target}", catch_exceptions=False)


def export_bundle(cratedb, click_kwargs, tmp_path):
    """
    Produce a bundle as a directory tree, returning (bundle_path, manifest).
    """
    result = run_export(cratedb, click_kwargs, tmp_path)
    assert result.exit_code == 0, result.output
    bundle_path = Path(json.loads(result.stdout)["path"])
    manifest = json.loads((bundle_path / "manifest.json").read_text())
    return bundle_path, manifest


def test_reported_path_is_the_bundle_root(cratedb, click_kwargs, tmp_path):
    """
    The path printed on stdout is the directory a recipient should look at.

    Reporting the `sys/` subtree instead would point past `manifest.json`,
    `information_schema/`, and `ddl/`.
    """
    result = run_export(cratedb, click_kwargs, tmp_path)
    assert result.exit_code == 0, result.output

    path = Path(json.loads(result.stdout)["path"])
    for entry in ["manifest.json", "sys", "information_schema", "ddl"]:
        assert (path / entry).exists(), f"not reachable from the reported path: {entry}"


def test_unrepresentable_schema_does_not_cost_the_data(cratedb, click_kwargs, tmp_path):
    """
    A table whose schema cannot be represented still has its data exported.
    """
    bundle_path, manifest = export_bundle(cratedb, click_kwargs, tmp_path)

    assert (bundle_path / "sys" / "data" / "sys-summits.jsonl").exists(), "summits data was dropped"

    # If the schema could not be generated, the bundle says so.
    if not (bundle_path / "sys" / "schema" / "sys-summits.sql").exists():
        failures = {item["table"] for item in manifest["schema_failures"]}
        assert "summits" in failures
        assert manifest["schema_failures"][0]["reason"]


def test_information_schema_is_exported(cratedb, click_kwargs, tmp_path):
    """
    `information_schema` is exported alongside `sys`, under its own prefix.
    """
    bundle_path, manifest = export_bundle(cratedb, click_kwargs, tmp_path)
    assert "information_schema" in manifest["schemas_exported"]

    schema_files = {item.name for item in (bundle_path / "information_schema" / "schema").iterdir()}
    assert schema_files, "no information_schema schema files written"
    assert all(name.startswith("is-") for name in schema_files)

    expected = {
        row["table_name"]
        for row in cratedb.database.run_sql(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'information_schema'",
            records=True,
        )
    }
    assert expected == {name[len("is-") : -len(".sql")] for name in schema_files}


def test_table_definitions_preserve_every_clause(cfr_validation_schema, cratedb, click_kwargs, tmp_path):
    """
    A captured definition is complete enough to rebuild the table.

    Deriving DDL from metadata tables by hand loses exactly these clauses, which
    is why the definition comes from the cluster's own `SHOW CREATE TABLE`.
    """
    bundle_path, manifest = export_bundle(cratedb, click_kwargs, tmp_path)
    definition = (bundle_path / "ddl" / "tables" / "doc.cfr_complex.sql").read_text()

    for clause in [
        "GEO_POINT",
        "FLOAT_VECTOR(4)",
        "OBJECT(DYNAMIC)",
        "ARRAY(TEXT)",
        "GENERATED ALWAYS AS",
        "INDEX USING FULLTEXT",
        "analyzer = 'english'",
        'PRIMARY KEY ("id", "month")',
        "CLUSTERED INTO 3 SHARDS",
        'PARTITIONED BY ("month")',
        "best_compression",
        "refresh_interval",
    ]:
        assert clause in definition, f"missing from captured definition: {clause}"

    assert manifest["definitions_captured"]["tables"] >= 2


def test_view_definitions_are_captured(cfr_validation_schema, cratedb, click_kwargs, tmp_path):
    """
    Views come from `information_schema.views`, because `SHOW CREATE TABLE`
    refuses them, and are not mistaken for tables.
    """
    bundle_path, manifest = export_bundle(cratedb, click_kwargs, tmp_path)

    view_file = bundle_path / "ddl" / "views" / "doc.cfr_view.sql"
    assert view_file.exists()
    assert "cfr_simple" in view_file.read_text()
    assert manifest["definitions_captured"]["views"] >= 1

    # A view must never be written as a table definition.
    assert not (bundle_path / "ddl" / "tables" / "doc.cfr_view.sql").exists()


def test_definitions_cover_all_non_system_schemas(cfr_validation_schema, cratedb, click_kwargs, tmp_path):
    """
    Capture is not limited to the default `doc` schema.
    """
    bundle_path, _ = export_bundle(cratedb, click_kwargs, tmp_path)
    captured = {item.name for item in (bundle_path / "ddl" / "tables").iterdir()}

    assert "doc.cfr_simple.sql" in captured
    assert "testdrive-cfr.cfr_other.sql" in captured
    for name in captured:
        assert not name.startswith(("sys.", "information_schema.", "pg_catalog.", "blob."))


def test_archive_top_level_entry_is_meaningful(cratedb, click_kwargs, tmp_path):
    """
    An archive unpacks into a name derived from cluster and timestamp, rather
    than into the random name of the temporary staging directory.
    """
    target = tmp_path / "bundle.tgz"
    assert run_export(cratedb, click_kwargs, target).exit_code == 0

    with tarfile.open(target) as tar:
        names = tar.getnames()

    top_level = {name.split("/")[0] for name in names}
    assert len(top_level) == 1, f"archive must have a single root, got {top_level}"
    root = top_level.pop()
    assert not root.startswith("tmp"), f"archive rooted at a temporary directory name: {root}"

    second_level = {name.split("/")[1] for name in names if len(name.split("/")) > 1}
    assert {"sys", "information_schema", "ddl", "manifest.json"}.issubset(second_level)


def test_sys_subtree_still_round_trips(cratedb, click_kwargs, tmp_path):
    """
    A bundle's `sys/` subtree imports back with no change to the importer.

    This is the guardrail on the layout: `manifest.json`, `information_schema/`,
    and `ddl/` all sit *above* `sys/` precisely so that this keeps working.
    """
    result = run_export(cratedb, click_kwargs, tmp_path)
    sys_path = Path(json.loads(result.stdout)["path"]) / "sys"

    assert sorted(item.name for item in sys_path.iterdir()) == ["data", "schema"]

    target_schema = "testdrive-roundtrip"
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi}, **click_kwargs)
    result = runner.invoke(
        cli,
        args=f"--cluster-url={cratedb.database.dburi}?schema={target_schema} sys-import {sys_path}",
        catch_exceptions=False,
    )
    try:
        assert result.exit_code == 0, result.output
        restored = cratedb.database.run_sql(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{target_schema}'",  # noqa: S608
            records=True,
        )
        assert restored, "no tables were restored"
    finally:
        cratedb.database.run_sql(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')


def test_data_failures_are_recorded_in_the_manifest(cratedb, click_kwargs, tmp_path, monkeypatch):
    """
    A table whose data could not be read is named in the manifest, not just logged.

    The recipient of a bundle never sees the exporter's log output, so a bundle
    where reads failed must not be indistinguishable from a complete one — the
    data *is* the diagnostic payload.
    """
    original_read_table = SystemTableExporter.read_table

    def failing_read_table(self, tablename, schema=None):
        if tablename == "summits":
            raise PermissionError("simulated read failure")
        return original_read_table(self, tablename=tablename, schema=schema)

    monkeypatch.setattr(SystemTableExporter, "read_table", failing_read_table)
    bundle_path, manifest = export_bundle(cratedb, click_kwargs, tmp_path)

    failures = {(item["schema"], item["table"]): item["reason"] for item in manifest["data_failures"]}
    assert ("sys", "summits") in failures
    assert "PermissionError" in failures[("sys", "summits")]
    assert not (bundle_path / "sys" / "data" / "sys-summits.jsonl").exists()


def test_information_schema_subtree_imports(cratedb, click_kwargs, tmp_path):
    """
    `sys-import` reads any per-schema subtree, not only `sys/`.

    Filenames carry their schema's prefix, so the importer derives table names
    from them rather than assuming `sys-`.
    """
    bundle_path, _ = export_bundle(cratedb, click_kwargs, tmp_path)
    is_path = bundle_path / "information_schema"

    target_schema = "testdrive-is-import"
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi}, **click_kwargs)
    result = runner.invoke(
        cli,
        args=f"--cluster-url={cratedb.database.dburi}?schema={target_schema} sys-import {is_path}",
        catch_exceptions=False,
    )
    try:
        assert result.exit_code == 0, result.output
        restored = {
            row["table_name"]
            for row in cratedb.database.run_sql(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{target_schema}'",  # noqa: S608
                records=True,
            )
        }
        assert restored, "no tables were restored"
        assert all(name.startswith("is-") for name in restored), restored
    finally:
        cratedb.database.run_sql(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')


def test_scrub_is_not_silently_ignored(cratedb, click_kwargs, tmp_path, caplog):
    """
    `--scrub` is accepted by the command group but does not apply to raw cluster
    data. Accepting it without saying so would misrepresent the bundle.
    """
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi, "CFR_TARGET": str(tmp_path)}, **click_kwargs)
    result = runner.invoke(cli, args=f"--debug --scrub sys-export {tmp_path}", catch_exceptions=False)

    assert result.exit_code == 0, result.output
    assert "`--scrub` has no effect on `sys-export`" in caplog.text
