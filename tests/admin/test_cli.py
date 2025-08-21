import pytest
from click.testing import CliRunner

from cratedb_toolkit.admin.xmover.cli import main as cli


@pytest.mark.parametrize(
    "subcommand",
    [
        "analyze",
        "check-balance",
        "explain-error",
        "find-candidates",
        "monitor-recovery",
        "recommend",
        "test-connection",
        "zone-analysis",
        "shard-distribution",
    ],
)
def test_xmover_all(cratedb, subcommand):
    """
    CLI test: Invoke `xmover <subcommand>`.
    """
    http_url = cratedb.get_http_url()
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args=subcommand,
        env={"CRATE_CONNECTION_STRING": http_url},
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_xmover_validate_move_success(cratedb):
    """
    CLI test: Invoke `xmover validate-move`.
    """
    http_url = cratedb.get_http_url()
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args=["validate-move", "doc.demo", "1", "42", "84"],
        env={"CRATE_CONNECTION_STRING": http_url},
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "Source node '42' not found in cluster" in result.output


def test_xmover_validate_move_failure(cratedb):
    """
    CLI test: Invoke `xmover validate-move`.
    """
    http_url = cratedb.get_http_url()
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args=["validate-move"],
        env={"CRATE_CONNECTION_STRING": http_url},
        catch_exceptions=False,
    )
    assert result.exit_code == 2
    assert "Error: Missing argument 'SCHEMA_TABLE'." in result.output
