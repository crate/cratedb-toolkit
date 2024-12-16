import json

import pytest
from click.testing import CliRunner

from cratedb_toolkit.shell.cli import cli
from tests.conftest import TESTDRIVE_DATA_SCHEMA


def test_shell_success(cratedb):
    """
    Verify successful incantation of `ctk shell`.
    """
    runner = CliRunner()

    database_url = cratedb.get_connection_url() + "?schema=" + TESTDRIVE_DATA_SCHEMA

    result = runner.invoke(
        cli,
        args=f"--cratedb-sqlalchemy-url='{database_url}' --command 'SELECT 42 AS answer;' --format=json",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == [{"answer": 42}]


def test_shell_failure_no_address():
    """
    Verify `ctk shell` fails when invoked without database address.
    """
    runner = CliRunner()

    with pytest.raises(ValueError) as ex:
        runner.invoke(
            cli,
            args="--command 'SELECT 42 AS answer;' --format=json",
            catch_exceptions=False,
        )
    assert ex.match("Unknown database address")
