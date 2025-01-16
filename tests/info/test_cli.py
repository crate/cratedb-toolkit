import json

from boltons.iterutils import get_path
from click.testing import CliRunner

from cratedb_toolkit.info.cli import cli


def test_info_cluster(cratedb):
    """
    Verify `ctk info cluster`.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})
    result = runner.invoke(
        cli,
        args="cluster",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify outcome.
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


def test_info_logs(cratedb):
    """
    Verify `ctk info logs`.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})
    result = runner.invoke(
        cli,
        args="logs",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify outcome.
    info = json.loads(result.output)
    assert "meta" in info
    assert "data" in info

    data_keys = list(info["data"].keys())
    assert "user_queries_latest" in data_keys
    assert len(info["data"]["user_queries_latest"]) > 3


def test_info_jobs(cratedb):
    """
    Verify `ctk info jobs`.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})
    result = runner.invoke(
        cli,
        args="jobs",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify outcome.
    info = json.loads(result.output)
    assert "meta" in info
    assert "data" in info

    data_keys = list(info["data"].keys())
    assert "by_user" in data_keys
    assert "top100_count" in data_keys
    assert "top100_duration_individual" in data_keys
    assert "top100_duration_total" in data_keys
    assert "performance15min" in data_keys
