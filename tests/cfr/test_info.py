from click.testing import CliRunner

from cratedb_toolkit.cfr.cli import cli
from cratedb_toolkit.testing import pytest

pytestmark = pytest.mark.cfr


def test_cfr_info_record(cratedb, caplog):
    """
    Verify `ctk cfr info record`.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": cratedb.database.dburi})
    result = runner.invoke(
        cli,
        args="--debug info record --once",
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
