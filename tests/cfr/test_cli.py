import json
import re
from pathlib import Path

from click.testing import CliRunner

from cratedb_toolkit.cfr.cli import cli


def filenames(path: Path):
    return sorted([item.name for item in path.iterdir()])


def test_cfr_cli_export(cratedb, tmp_path, caplog):
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
    data_files = filenames(path / "schema")

    assert len(schema_files) >= 19
    assert len(data_files) >= 19
