import json

from boltons.iterutils import get_path
from click.testing import CliRunner
from yarl import URL

from cratedb_toolkit.wtf.cli import cli


def test_wtf_cli_info(cratedb):
    """
    Verify `cratedb-wtf info`.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})
    result = runner.invoke(
        cli,
        args="info",
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


def test_wtf_cli_logs(cratedb):
    """
    Verify `cratedb-wtf logs`.
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


def test_wtf_cli_job_info(cratedb):
    """
    Verify `cratedb-wtf job-info`.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})
    result = runner.invoke(
        cli,
        args="job-info",
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


def test_wtf_cli_statistics_collect(cratedb, caplog):
    """
    Verify `cratedb-wtf job-statistics collect`.
    """

    uri = URL(cratedb.database.dburi)

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})
    result = runner.invoke(
        cli,
        args="job-statistics collect --once",
        env={"HOSTNAME": f"{uri.host}:{uri.port}"},
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify outcome: Log output.
    assert "Recording information snapshot" in caplog.messages

    # Verify outcome: Database content.
    # stats.statement_log, stats.last_execution
    results = cratedb.database.run_sql("SHOW TABLES", records=True)
    assert {"table_name": "last_execution"} in results
    assert {"table_name": "statement_log"} in results

    # FIXME: Table is empty. Why?
    cratedb.database.run_sql('REFRESH TABLE "stats"."statement_log"')
    assert cratedb.database.count_records("stats.statement_log") == 0

    cratedb.database.run_sql('REFRESH TABLE "stats"."last_execution"')
    assert cratedb.database.count_records("stats.last_execution") == 1


def test_wtf_cli_statistics_view(cratedb):
    """
    Verify `cratedb-wtf job-statistics view`.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})
    result = runner.invoke(
        cli,
        args="job-statistics view",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify outcome.
    info = json.loads(result.output)
    assert "meta" in info
    assert "data" in info

    data_keys = list(info["data"].keys())
    assert "stats" in data_keys


def test_wtf_cli_record(cratedb, caplog):
    """
    Verify `cratedb-wtf record`.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})
    result = runner.invoke(
        cli,
        args="--debug record --once",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify outcome: Log output.
    assert "Recording information snapshot" in caplog.messages

    # Verify outcome: Database content.
    results = cratedb.database.run_sql("SHOW TABLES", records=True)
    assert {"table_name": "clusterinfo"} in results
    assert {"table_name": "jobinfo"} in results

    cratedb.database.run_sql('REFRESH TABLE "ext"."clusterinfo"')
    assert cratedb.database.count_records("ext.clusterinfo") == 1

    cratedb.database.run_sql('REFRESH TABLE "ext"."jobinfo"')
    assert cratedb.database.count_records("ext.jobinfo") == 1
