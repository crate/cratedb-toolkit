import json
import os

import pytest
from click.testing import CliRunner

from cratedb_toolkit.shell.cli import cli
from tests.conftest import TESTDRIVE_DATA_SCHEMA


def test_shell_standalone(cratedb):
    """
    Verify the successful incantation of `ctk shell` against a self-installed CrateDB.
    """
    runner = CliRunner()

    database_url = cratedb.get_connection_url() + "?schema=" + TESTDRIVE_DATA_SCHEMA

    result = runner.invoke(
        cli,
        args=f"--cluster-url='{database_url}' --command 'SELECT 42 AS answer;' --format=json",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == [{"answer": 42}]


def test_shell_managed_jwt(mocker, cloud_cluster_name):
    """
    Verify the successful incantation of `ctk shell` against CrateDB Cloud, using JWT authentication.
    """

    settings = {
        "CRATEDB_CLOUD_API_KEY": os.environ.get("TEST_CRATEDB_CLOUD_API_KEY"),
        "CRATEDB_CLOUD_API_SECRET": os.environ.get("TEST_CRATEDB_CLOUD_API_SECRET"),
    }

    if any(setting is None for setting in settings.values()):
        raise pytest.skip("Missing environment variables for headless mode with croud")

    # Synthesize a valid environment.
    mocker.patch.dict("os.environ", settings)

    runner = CliRunner()

    result = runner.invoke(
        cli,
        args=f"--cluster-name={cloud_cluster_name} --command 'SELECT 42 AS answer;' --format=json",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == [{"answer": 42}]


def test_shell_managed_username_password(mocker, cloud_cluster_name):
    """
    Verify the successful incantation of `ctk shell` against CrateDB Cloud, using username/password authentication.
    """

    settings = {
        "CRATEDB_CLOUD_API_KEY": os.environ.get("TEST_CRATEDB_CLOUD_API_KEY"),
        "CRATEDB_CLOUD_API_SECRET": os.environ.get("TEST_CRATEDB_CLOUD_API_SECRET"),
        "CRATEDB_USERNAME": os.environ.get("TEST_CRATEDB_USERNAME"),
        "CRATEDB_PASSWORD": os.environ.get("TEST_CRATEDB_PASSWORD"),
    }

    if any(setting is None for setting in settings.values()):
        raise pytest.skip("Missing environment variables for headless mode with croud")

    # Synthesize a valid environment.
    mocker.patch.dict("os.environ", settings)

    runner = CliRunner()

    result = runner.invoke(
        cli,
        args=f"--cluster-name={cloud_cluster_name} --command 'SELECT 42 AS answer;' --format=json",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == [{"answer": 42}]


def test_shell_no_address():
    """
    Verify `ctk shell` fails when invoked without a database address.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="--command 'SELECT 42 AS answer;' --format=json",
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "Error: Missing database address" in result.output
