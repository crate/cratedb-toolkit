import json

import pytest
from boltons.iterutils import get_path
from click.testing import CliRunner

from cratedb_toolkit.info.cli import cli
from cratedb_toolkit.info.core import JobInfoContainer
from tests.info.test_model import JOB_ELEMENT_NAMES


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
    assert "cluster_health" in database_keys
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

    # Both sections describe the very same elements.
    assert sorted(info["data"]) == JOB_ELEMENT_NAMES
    assert sorted(info["meta"]["elements"]) == JOB_ELEMENT_NAMES

    # The elements which reduce to a single value.
    assert isinstance(info["data"]["history_count"], int)
    assert isinstance(info["data"]["running_count"], int)

    # The elements which return rows.
    assert isinstance(info["data"]["by_user"], list)
    assert isinstance(info["data"]["top100_count"], list)


def test_info_jobs_history(cratedb, runner_standalone):
    """
    Verify `ctk info jobs` reports the statements which have been invoked.
    """

    marker = "ctk-info-jobs-marker"
    cratedb.database.run_sql(f"SELECT '{marker}' AS marker")

    result = runner_standalone.invoke(cli, args="jobs", catch_exceptions=False)
    assert result.exit_code == 0

    data = json.loads(result.output)["data"]

    assert data["history_count"] > 0
    assert any(marker in record["stmt"] for record in data["history"])

    # The query history is reported in chronological order, oldest first.
    timestamps = [record["time"] for record in data["history"]]
    assert timestamps == sorted(timestamps)

    # The query frequency reports the 99th percentile of the query duration, per statement.
    assert sorted(data["top100_count"][0]) == [
        "avg_duration",
        "max_duration",
        "min_duration",
        "p99",
        "stmt",
        "stmt_count",
    ]

    # Durations are reported in milliseconds.
    assert sorted(data["top100_duration_individual"][0]) == ["duration", "stmt"]


@pytest.mark.parametrize("element_name", JOB_ELEMENT_NAMES)
def test_info_jobs_element(cratedb, element_name):
    """
    Verify each element of `ctk info jobs` runs against CrateDB on its own.

    """

    container = JobInfoContainer(adapter=cratedb.database)
    element = container.elements.index[element_name]
    container.evaluate_element(element)


def test_info_serve(mocker):
    """
    Verify `ctk info serve` starts the HTTP service, and hands over its options.
    """

    pytest.importorskip("fastapi")
    start = mocker.patch("cratedb_toolkit.info.http.start")

    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": "crate://localhost:4200/"})
    result = runner.invoke(cli, args="serve --listen 0.0.0.0:8042 --reload", catch_exceptions=False)
    assert result.exit_code == 0

    start.assert_called_once_with("0.0.0.0:8042", reload=True)
