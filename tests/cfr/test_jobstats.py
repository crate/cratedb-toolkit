# ruff: noqa: S608
import json
import os
import uuid

import pytest
from click.testing import CliRunner

from cratedb_toolkit.cfr.cli import cli
from tests.conftest import TESTDRIVE_EXT_SCHEMA

pytestmark = pytest.mark.cfr

STATEMENTS_TABLE = f"{TESTDRIVE_EXT_SCHEMA}.jobstats_statements"
LAST_TABLE = f"{TESTDRIVE_EXT_SCHEMA}.jobstats_last"


@pytest.fixture
def runner(cratedb):
    """
    Provide a Click runner which collects into the `testdrive-ext` schema.
    """
    return CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi + f"?schema={TESTDRIVE_EXT_SCHEMA}"})


@pytest.fixture(autouse=True)
def reset_collector_state():
    """
    Discard collector state, which lives in module-global variables, between test cases.
    """
    from cratedb_toolkit.cfr import jobstats

    jobstats.reset_state()
    yield
    jobstats.reset_state()


def marker_statement(cratedb, label: str) -> str:
    """
    Run a uniquely identifiable statement, so it can be found in the collected statistics.

    The marker gets a unique suffix, because `sys.jobs_log` also holds the statements of
    previous test runs when the database container is reused. The collector skips
    statements which touch `sys.` or `information_schema.`.
    """
    marker = f"{label}-{uuid.uuid4().hex[:8]}"
    cratedb.database.run_sql(f"SELECT '{marker}' AS marker")
    return marker


def collected_statements(cratedb, table: str = STATEMENTS_TABLE):
    """
    Return all statements from the collected statistics.
    """
    cratedb.database.refresh_table(table)
    quoted = cratedb.database.quote_relation_name(table)
    return [record["stmt"] for record in cratedb.database.run_sql(f"SELECT stmt FROM {quoted}", records=True)]


def test_cfr_jobstats_collect_self(cratedb, caplog):
    """
    Verify `ctk cfr jobstats collect` into the same database.
    """

    # Configure database URI.
    dburi = cratedb.database.dburi + f"?schema={TESTDRIVE_EXT_SCHEMA}"

    marker_statement(cratedb, "jobstats-collect-self-marker")

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": dburi})
    result = runner.invoke(
        cli,
        args="jobstats collect --once",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Verify outcome: Log output.
    assert "Recording information snapshot" in caplog.messages

    # Verify outcome: Database content.
    # stats.statement_log, stats.last_execution
    results = cratedb.database.run_sql("SHOW TABLES", records=True)
    assert {"table_name": "jobstats_last"} in results
    assert {"table_name": "jobstats_statements"} in results

    # How many statements are collected depends on the activity on the cluster.
    cratedb.database.refresh_table(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_statements")
    assert cratedb.database.count_records(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_statements") >= 1

    cratedb.database.refresh_table(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_last")
    assert cratedb.database.count_records(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_last") == 1


def test_cfr_jobstats_collect_anonymized(cratedb, caplog):
    """
    Verify `ctk cfr jobstats collect` into the same database, using the `--anonymize` option.

    Without a value, the option uses `decoder_dictionary.json` in the current directory,
    so the command runs on an isolated filesystem here.
    """

    # Configure database URI.
    dburi = cratedb.database.dburi + f"?schema={TESTDRIVE_EXT_SCHEMA}"

    marker = marker_statement(cratedb, "jobstats-anonymized-default")

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": dburi})
    with runner.isolated_filesystem():
        result = runner.invoke(
            cli,
            args="jobstats collect --once --anonymize",
            catch_exceptions=False,
        )
    assert result.exit_code == 0, result.output

    # Verify outcome: Log output.
    assert "Recording information snapshot" in caplog.messages

    # Verify outcome: Database content.
    # stats.statement_log, stats.last_execution
    results = cratedb.database.run_sql("SHOW TABLES", records=True)
    assert {"table_name": "jobstats_last"} in results
    assert {"table_name": "jobstats_statements"} in results

    cratedb.database.refresh_table(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_statements")
    assert cratedb.database.count_records(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_statements") >= 1

    cratedb.database.refresh_table(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_last")
    assert cratedb.database.count_records(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_last") == 1

    # Verify outcome: No statement has been stored in clear text.
    assert not any(marker in stmt for stmt in collected_statements(cratedb))


def test_cfr_jobstats_collect_reportdb(cratedb, caplog):
    """
    Verify `ctk cfr jobstats collect` into a different database.
    """

    schema_reportdb = "testdrive-ext-report"

    # Configure database URI.
    dburi = cratedb.database.dburi + f"?schema={TESTDRIVE_EXT_SCHEMA}"
    dburi_report = cratedb.database.dburi + f"?schema={schema_reportdb}"

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": dburi})
    result = runner.invoke(
        cli,
        args=f"jobstats collect --once --reportdb={dburi_report}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Verify outcome: Log output.
    assert "Recording information snapshot" in caplog.messages

    # Verify outcome: Database content.
    # stats.statement_log, stats.last_execution
    results = cratedb.database.run_sql("SHOW TABLES", records=True)
    assert {"table_name": "jobstats_last"} in results
    assert {"table_name": "jobstats_statements"} in results

    # How many statements are collected depends on the activity on the cluster.
    cratedb.database.refresh_table(f"{schema_reportdb}.jobstats_statements")
    assert cratedb.database.count_records(f"{schema_reportdb}.jobstats_statements") >= 1

    cratedb.database.refresh_table(f"{schema_reportdb}.jobstats_last")
    assert cratedb.database.count_records(f"{schema_reportdb}.jobstats_last") == 1


def test_cfr_jobstats_view(cratedb):
    """
    Verify `ctk cfr jobstats view`.
    """

    # Configure database URI.
    dburi = cratedb.database.dburi + f"?schema={TESTDRIVE_EXT_SCHEMA}"

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": dburi})
    result = runner.invoke(
        cli,
        args="jobstats view",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Verify outcome.
    info = json.loads(result.output)
    assert "meta" in info
    assert "data" in info

    data_keys = list(info["data"].keys())
    assert "stats" in data_keys


def test_cfr_jobstats_collect_records_statements(cratedb, runner):
    """
    Verify `ctk cfr jobstats collect` records statements verbatim, when not anonymizing.
    """

    marker = marker_statement(cratedb, "jobstats-plain")

    result = runner.invoke(cli, args="jobstats collect --once", catch_exceptions=False)
    assert result.exit_code == 0, result.output

    assert any(marker in stmt for stmt in collected_statements(cratedb))


def test_cfr_jobstats_collect_anonymize_with_path(cratedb, runner, tmp_path):
    """
    Verify `ctk cfr jobstats collect --anonymize` accepts a decoder dictionary path.

    Passing a path used to fail with `Got unexpected extra argument`, because the option
    was declared as a boolean flag, while both the help text and the docs advertised a path.
    """

    marker = marker_statement(cratedb, "jobstats-anonymize")
    decoder_dictionary = tmp_path / "decoder_dictionary.json"

    result = runner.invoke(
        cli,
        args=f"jobstats collect --once --anonymize {decoder_dictionary}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Verify outcome: The marker statement has been stored, but not in clear text.
    statements = collected_statements(cratedb)
    assert statements
    assert not any(marker in stmt for stmt in statements)


def test_cfr_jobstats_anonymize_roundtrip(cratedb, runner, tmp_path):
    """
    Verify `collect --anonymize` and `view --deanonymize` are inverse operations.
    """

    marker = marker_statement(cratedb, "jobstats-roundtrip")
    decoder_dictionary = tmp_path / "decoder_dictionary.json"

    result = runner.invoke(
        cli,
        args=f"jobstats collect --once --anonymize {decoder_dictionary}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert not any(marker in stmt for stmt in collected_statements(cratedb))

    result = runner.invoke(
        cli,
        args=f"jobstats view --deanonymize {decoder_dictionary}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Verify outcome: The statement is legible again.
    statements = json.loads(result.output)["data"]["stats"]
    assert any(marker in stmt for stmt in statements)

    # Verify outcome: Each statement is reported once, not once per anonymization state.
    assert len(statements) == len(collected_statements(cratedb))


def test_cfr_jobstats_collect_resumes_from_watermark(cratedb, runner, caplog):
    """
    Verify a second `ctk cfr jobstats collect --once` resumes from the recorded watermark.

    The `jobstats_last` table records how far the collector has come. Without honoring it,
    a restarted collector counts the same jobs again.
    """

    marker = marker_statement(cratedb, "jobstats-watermark")

    # Collect once, and remember the watermark.
    result = runner.invoke(cli, args="jobstats collect --once", catch_exceptions=False)
    assert result.exit_code == 0, result.output

    cratedb.database.refresh_table(LAST_TABLE)
    quoted_last = cratedb.database.quote_relation_name(LAST_TABLE)
    first_watermark = cratedb.database.run_sql(f"SELECT last_execution FROM {quoted_last}", records=True)[0][
        "last_execution"
    ]
    assert first_watermark > 0

    def marker_calls():
        """
        Return the recorded number of calls of the marker statement.

        Match the statement exactly. The verification queries of this test case are
        recorded in `sys.jobs_log`, too, and mention the marker as well.
        """
        cratedb.database.refresh_table(STATEMENTS_TABLE)
        quoted = cratedb.database.quote_relation_name(STATEMENTS_TABLE)
        records = cratedb.database.run_sql(f"SELECT stmt, calls FROM {quoted}", records=True)
        return [record["calls"] for record in records if record["stmt"] == f"SELECT '{marker}' AS marker"]

    assert marker_calls() == [1]

    # Collect again. The marker job ran before the watermark, so it must not be counted twice.
    caplog.clear()
    result = runner.invoke(cli, args="jobstats collect --once", catch_exceptions=False)
    assert result.exit_code == 0, result.output
    assert f"Resuming from recorded watermark: {first_watermark}" in caplog.text

    assert marker_calls() == [1]

    cratedb.database.refresh_table(LAST_TABLE)
    second_watermark = cratedb.database.run_sql(f"SELECT last_execution FROM {quoted_last}", records=True)[0][
        "last_execution"
    ]
    assert second_watermark > first_watermark


def test_cfr_jobstats_view_without_data(cratedb):
    """
    Verify `ctk cfr jobstats view` on a schema without collected statistics.

    The command creates its tables on demand, so it reports an empty result instead of failing.
    """

    schema = "testdrive-ext-empty"
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi + f"?schema={schema}"})
    result = runner.invoke(cli, args="jobstats view", catch_exceptions=False)
    assert result.exit_code == 0, result.output

    assert json.loads(result.output)["data"]["stats"] == {}

    cratedb.reset(tables=[f'"{schema}".jobstats_statements', f'"{schema}".jobstats_last'])


def test_cfr_jobstats_report(cratedb, runner, mocker):
    """
    Verify `ctk cfr jobstats report` reads the schema the statistics were collected into.
    """

    pytest.importorskip("marimo")

    result = runner.invoke(cli, args="jobstats collect --once", catch_exceptions=False)
    assert result.exit_code == 0, result.output

    captured = {}

    def capture():
        captured["cluster_url"] = os.environ.get("CRATEDB_CLUSTER_URL")

    app_run = mocker.patch("cratedb_toolkit.cfr.marimo.app.run", side_effect=capture)

    result = runner.invoke(cli, args="jobstats report", catch_exceptions=False)
    assert result.exit_code == 0, result.output

    app_run.assert_called_once()
    assert TESTDRIVE_EXT_SCHEMA in captured["cluster_url"]


def test_cfr_jobstats_ui(cratedb, runner, mocker):
    """
    Verify `ctk cfr jobstats ui` launches a web server for the collected statistics.
    """

    pytest.importorskip("marimo")
    pytest.importorskip("uvicorn")

    result = runner.invoke(cli, args="jobstats collect --once", catch_exceptions=False)
    assert result.exit_code == 0, result.output

    mocker.patch("marimo.create_asgi_app")
    uvicorn_run = mocker.patch("uvicorn.run")

    result = runner.invoke(cli, args="jobstats ui", catch_exceptions=False)
    assert result.exit_code == 0, result.output

    uvicorn_run.assert_called_once()
    assert uvicorn_run.call_args.kwargs["port"] == 7777


def test_cfr_jobstats_report_without_data(cratedb):
    """
    Verify `ctk cfr jobstats report` reports missing statistics in an actionable way.
    """

    pytest.importorskip("marimo")

    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi + "?schema=testdrive-ext-missing"})
    result = runner.invoke(cli, args="jobstats report")

    assert result.exit_code == 1
    assert isinstance(result.exception, FileNotFoundError)
    assert "testdrive-ext-missing" in str(result.exception)
    assert "ctk cfr jobstats collect" in str(result.exception)
