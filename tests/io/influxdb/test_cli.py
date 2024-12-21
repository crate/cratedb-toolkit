# ruff: noqa: E402
import pytest
from click.testing import CliRunner
from pueblo.testing.dataframe import DataFrameFactory

from cratedb_toolkit.cli import cli

pytestmark = pytest.mark.influxdb

pytest.importorskip("influxio", reason="Skipping InfluxDB tests because 'influxio' package is not installed")
pytest.importorskip(
    "influxdb_client", reason="Skipping InfluxDB tests because 'influxdb-client' package is not installed"
)

from influxio.adapter import InfluxDbApiAdapter


def test_line_protocol_load_table_success(caplog, cratedb, needs_sqlalchemy2):
    """
    CLI test: Invoke `ctk load table` for InfluxDB line protocol (ILP) file.
    """
    ilp_url = "https://github.com/influxdata/influxdb2-sample-data/raw/master/noaa-ndbc-data/latest-observations.lp"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb_url})
    result = runner.invoke(
        cli,
        args=f"load table {ilp_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") >= 500


def test_line_protocol_load_table_wrong_cratedb_url_failure(caplog, cratedb, needs_sqlalchemy2):
    """
    CLI test: Invoke `ctk load table` for InfluxDB line protocol (ILP) file.
    """
    ilp_url = "https://github.com/influxdata/influxdb2-sample-data/raw/master/noaa-ndbc-data/latest-observations.lp"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb_url})
    with pytest.raises(ValueError) as ex:
        runner.invoke(
            cli,
            args=f"load table {ilp_url}",
            catch_exceptions=False,
        )
    assert ex.match("Table name is missing. Please adjust CrateDB database URL.")


def test_influxdb2_load_table(caplog, cratedb, influxdb, needs_sqlalchemy2):
    """
    CLI test: Invoke `ctk load table` for InfluxDB.
    """
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"
    influxdb_url = f"{influxdb.get_connection_url()}/testdrive/demo"

    # Create sample dataset with a few records worth of data.
    dff = DataFrameFactory(rows=42)
    df = dff.make("dateindex")

    # Populate source database.
    adapter = InfluxDbApiAdapter.from_url(influxdb_url)
    adapter.ensure_bucket()
    adapter.write_df(df)

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb_url})
    influxdb_url = influxdb_url.replace("http://", "influxdb2://")
    result = runner.invoke(
        cli,
        args=f"load table {influxdb_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 42
