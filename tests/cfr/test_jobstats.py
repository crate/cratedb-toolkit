import json

from click.testing import CliRunner

from cratedb_toolkit.cfr.cli import cli
from tests.conftest import TESTDRIVE_EXT_SCHEMA


def test_cfr_jobstats_collect(cratedb, caplog):
    """
    Verify `ctk cfr jobstats collect`.
    """

    # Configure database URI.
    dburi = cratedb.database.dburi + f"?schema={TESTDRIVE_EXT_SCHEMA}"

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": dburi})
    result = runner.invoke(
        cli,
        args="jobstats collect --once",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify outcome: Log output.
    assert "Recording information snapshot" in caplog.messages

    # Verify outcome: Database content.
    # stats.statement_log, stats.last_execution
    results = cratedb.database.run_sql("SHOW TABLES", records=True)
    assert {"table_name": "jobstats_last"} in results
    assert {"table_name": "jobstats_statements"} in results

    cratedb.database.refresh_table(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_statements")
    assert cratedb.database.count_records(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_statements") >= 19

    cratedb.database.refresh_table(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_last")
    assert cratedb.database.count_records(f"{TESTDRIVE_EXT_SCHEMA}.jobstats_last") == 1


def test_cfr_jobstats_view(cratedb):
    """
    Verify `ctk cfr jobstats view`.
    """

    # Configure database URI.
    dburi = cratedb.database.dburi + f"?schema={TESTDRIVE_EXT_SCHEMA}"

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": dburi})
    result = runner.invoke(
        cli,
        args="jobstats view",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify outcome.
    info = json.loads(result.output)
    assert "meta" in info
    assert "data" in info

    data_keys = list(info["data"].keys())
    assert "stats" in data_keys
