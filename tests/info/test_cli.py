import json

import pytest
from boltons.iterutils import get_path
from click.testing import CliRunner

from cratedb_toolkit.info.cli import cli


@pytest.fixture
def runner_managed(cloud_environment):
    """
    Provide a Click runner for managed CrateDB, connecting per information from environment variables.
    """
    return CliRunner()


@pytest.fixture
def runner_standalone(cratedb):
    """
    Provide a Click runner for standalone CrateDB, connecting per SQLAlchemy URL.
    """
    return CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi})


@pytest.mark.parametrize("runner_factory", ["runner_standalone", "runner_managed"], ids=["standalone", "managed"])
def test_info_cluster(request, runner_factory):
    """
    Verify `ctk info cluster` on both standalone and managed CrateDB.
    """

    # Invoke command.
    runner = request.getfixturevalue(runner_factory)
    result = runner.invoke(
        cli,
        args="cluster",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify the outcome.
    info = json.loads(result.output)
    assert "meta" in info
    assert "data" in info

    system_keys = list(get_path(info, ("data", "system")).keys())
    database_keys = list(get_path(info, ("data", "database")).keys())
    assert system_keys == [
        "remark",
        "application",
        "eco",
        # "libraries",
    ]
    assert "cluster_name" in database_keys
    assert "cluster_nodes_count" in database_keys


@pytest.mark.parametrize("runner_factory", ["runner_standalone", "runner_managed"], ids=["standalone", "managed"])
def test_info_logs(request, runner_factory):
    """
    Verify `ctk info logs` on both standalone and managed CrateDB.
    """

    # Invoke command.
    runner = request.getfixturevalue(runner_factory)
    result = runner.invoke(
        cli,
        args="logs",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify the outcome.
    info = json.loads(result.output)
    assert "meta" in info
    assert "data" in info

    data_keys = list(info["data"].keys())
    assert "user_queries_latest" in data_keys
    assert len(info["data"]["user_queries_latest"]) > 3


@pytest.mark.parametrize("runner_factory", ["runner_standalone", "runner_managed"], ids=["standalone", "managed"])
def test_info_jobs(request, runner_factory):
    """
    Verify `ctk info jobs` on both standalone and managed CrateDB.
    """

    # Invoke command.
    runner = request.getfixturevalue(runner_factory)
    result = runner.invoke(
        cli,
        args="jobs",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify the outcome.
    info = json.loads(result.output)
    assert "meta" in info
    assert "data" in info

    data_keys = list(info["data"].keys())
    assert "by_user" in data_keys
    assert "top100_count" in data_keys
    assert "top100_duration_individual" in data_keys
    assert "top100_duration_total" in data_keys
    assert "performance15min" in data_keys
