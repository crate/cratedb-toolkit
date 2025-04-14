import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from cratedb_toolkit.docs.cli import cli


def test_settings_json(tmp_path: Path):
    """
    Verify `ctk docs settings`.
    """

    output_path = tmp_path / "cratedb-settings.json"

    # Invoke command.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args=f"settings --format=json --output={output_path}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify the outcome.
    data = json.loads(output_path.read_text())
    assert "whether or not to collect statistical information" in data["settings"]["stats.enabled"]["purpose"]


def test_settings_markdown(tmp_path: Path):
    """
    Verify `ctk docs settings`.
    """

    output_path = tmp_path / "cratedb-settings.md"

    # Invoke command.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args=f"settings --format=markdown --output={output_path}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify the outcome.
    data = output_path.read_text()
    assert "| bulk.request_timeout | 1m | Default: 1m" in data
    assert "| udc.url | https://udc.crate.io" in data


def test_settings_sql(tmp_path: Path):
    """
    Verify `ctk docs settings`.
    """

    output_path = tmp_path / "cratedb-settings.sql"

    # Invoke command.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args=f"settings --format=sql --output={output_path}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify the outcome.
    data = output_path.read_text()
    assert """SET GLOBAL PERSISTENT "bulk.request_timeout" = '1m';""" in data
    assert "-- Total statements:" in data


def test_settings_yaml(tmp_path: Path):
    """
    Verify `ctk docs settings`.
    """

    output_path = tmp_path / "cratedb-settings.yaml"

    # Invoke command.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args=f"settings --format=yaml --output={output_path}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify the outcome.
    data = yaml.safe_load(output_path.read_text())
    assert "whether or not to collect statistical information" in data["settings"]["stats.enabled"]["purpose"]


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
