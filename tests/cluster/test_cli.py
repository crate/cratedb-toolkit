from click.testing import CliRunner

from cratedb_toolkit.cluster.cli import cli


def test_managed_cluster_info_empty(cloud_environment):
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
    assert "Error: 401 - Unauthorized" in result.output
