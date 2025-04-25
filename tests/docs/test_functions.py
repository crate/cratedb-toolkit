import json
from pathlib import Path

from click.testing import CliRunner

from cratedb_toolkit.docs.cli import cli


def test_functions_json(tmp_path: Path):
    """
    Verify `ctk docs functions`.
    """

    output_path = tmp_path / "cratedb-functions.json"

    # Invoke command.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args=f"functions --format=json --output={output_path}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify the outcome.
    data = json.loads(output_path.read_text())
    assert "substr('string' FROM 'pattern')" in data["functions"]


def test_functions_markdown(tmp_path: Path):
    """
    Verify `ctk docs functions`.
    """

    output_path = tmp_path / "cratedb-functions.md"

    # Invoke command.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args=f"functions --format=markdown --output={output_path}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify the outcome.
    data = output_path.read_text()
    assert "substr('string' FROM 'pattern')" in data
