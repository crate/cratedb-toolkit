import pytest
from click import ClickException
from click.testing import CliRunner

from cratedb_toolkit.cluster.cli import cli, handle_command_errors
from cratedb_toolkit.exception import CroudException


def test_exception_handling_croud(caplog):
    with pytest.raises(CroudException):
        with handle_command_errors("test operation"):
            raise CroudException("test error")
    assert "Failed to test operation" in caplog.text
    assert "test error" in caplog.text


def test_exception_handling_click():
    with pytest.raises(ClickException):
        with handle_command_errors("test operation"):
            raise ClickException("click error")


def test_exception_handling_generic_to_system_exit(caplog):
    with pytest.raises(SystemExit) as excinfo:
        with handle_command_errors("test operation"):
            raise ValueError("value error")
    assert excinfo.value.code == 1
    assert "Unexpected error on operation: test operation" in caplog.text


def test_managed_cluster_info_default(cloud_environment):
    """
    Verify `ctk cluster info` on a managed cluster works when a valid environment is provided.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="info",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert '"name": "testcluster"' in result.output
    assert '"product_name": "crfree"' in result.output


def test_managed_cluster_health_default(cloud_environment):
    """
    Verify `ctk cluster health` on a managed cluster works.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="health",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert '"cluster-name": "testcluster"' in result.output
    assert '"status": "GREEN"' in result.output


def test_managed_cluster_ping_default(cloud_environment):
    """
    Verify `ctk cluster ping` on a managed cluster works.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="ping",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert '"cluster-name": "testcluster"' in result.output
    assert '"cloud": true' in result.output
    assert '"database": true' in result.output


def test_managed_cluster_info_unknown(cloud_environment):
    """
    Verify `ctk cluster info --cluster-name=unknown` on a managed cluster fails.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="info --cluster-name=unknown",
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "Cluster not found: unknown" in result.output


def test_standalone_cluster_info_empty():
    """
    Verify `ctk cluster info` on a standalone cluster fails when not configured.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="info",
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "Error: Failed to address cluster" in result.output


def test_standalone_cluster_info_unknown(caplog):
    """
    Verify `ctk cluster info --cluster-name=unknown` on a standalone cluster fails.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="info --cluster-name=unknown",
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "Failed to inquire cluster info" in caplog.text
    assert "401 - Unauthorized" in result.output
