# ruff: noqa: E402
import pytest

pytest.importorskip("influxio", reason="Skipping InfluxDB tests because 'influxio' package is not installed")
pytest.importorskip(
    "influxdb_client", reason="Skipping InfluxDB tests because 'influxdb-client' package is not installed"
)

from click.testing import CliRunner
from influxio.model import InfluxDbAdapter
from influxio.testdata import DataFrameFactory

from cratedb_toolkit.cli import cli


def test_influxdb2_load_table(caplog, cratedb, influxdb):
    """
    CLI test: Invoke `ctk load table`.
    """
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"
    influxdb_url = f"{influxdb.get_connection_url()}/testdrive/demo"

    # Populate source database with a few records worth of data.
    adapter = InfluxDbAdapter.from_url(influxdb_url)
    adapter.ensure_bucket()
    dff = DataFrameFactory(rows=42)
    df = dff.make("dateindex")
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
