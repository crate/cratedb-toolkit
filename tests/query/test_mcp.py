import json
import os

import pytest
import yaml
from click.testing import CliRunner

pytest.importorskip("mcp")

from cratedb_toolkit.query.mcp.cli import cli


def test_list():
    """
    Verify `ctk query mcp list`.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args="list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data == [
        "cratedb-mcp",
        "dbhub",
        "mcp-alchemy",
        "mcp-dbutils",
        "pg-mcp",
        "postgres-basic",
        "postgres-mcp (Postgres Pro)",
        "quarkus",
    ]


def test_inquire_markdown():
    """
    Verify `ctk query mcp inquire --format=markdown`.
    """
    import markdown_it

    runner = CliRunner()

    result = runner.invoke(
        cli,
        args="--server-name=postgres-basic inquire --format=markdown",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    html = markdown_it.MarkdownIt().render(result.output)
    assert "## postgres-basic" in result.output
    assert "<h2>postgres-basic</h2>" in html
    assert "This page was generated automatically" in html


@pytest.mark.skipif("GITHUB_ACTION" in os.environ, reason="Test fails on GHA. Don't know why.")
def test_inquire_json():
    """
    Verify `ctk query mcp inquire --format=json`.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args="--server-name=postgres-basic inquire --format=json",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    response = json.loads(result.output)
    assert "postgres-basic" in response["data"]
    assert response["data"]["postgres-basic"]["capabilities"]["tools"][0]["name"] == "query"
    assert "This page was generated automatically" in response["meta"]["notes"]


@pytest.mark.skipif("GITHUB_ACTION" in os.environ, reason="Test fails on GHA. Don't know why.")
def test_inquire_yaml():
    """
    Verify `ctk query mcp inquire --format=yaml`.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args="--server-name=postgres-basic inquire --format=yaml",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    response = yaml.safe_load(result.output)
    assert "postgres-basic" in response["data"]
    assert response["data"]["postgres-basic"]["capabilities"]["tools"][0]["name"] == "query"
    assert "This page was generated automatically" in response["meta"]["notes"]


def test_launch(mocker):
    """
    Verify `ctk query mcp launch`.
    """
    runner = CliRunner()

    # Disable the actual launcher code, to not block the progress of the test case.
    mocker.patch("cratedb_toolkit.query.mcp.model.McpServer._start_dummy")

    result = runner.invoke(
        cli,
        args="--server-name=postgres-basic launch",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
