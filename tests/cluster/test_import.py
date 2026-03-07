import os

import pytest
from click.testing import CliRunner

from cratedb_toolkit import ManagedCluster


@pytest.mark.skipif(
    os.getenv("FORCE", "no") != "yes",
    reason="Only works when invoked exclusively, using "
    "`FORCE=yes pytest --no-cov tests/cluster/test_import.py -k test_csv_import_local`. "
    "Otherwise croaks per `AssertionError: ERROR: The following arguments are required: --url`. "
    "We don't know why.",
)
def test_csv_import_local(cloud_environment, dummy_csv, caplog):
    """
    CLI test: Invoke `ctk load table ...` for a CrateDB Cloud Import, from a local file.
    """

    from cratedb_toolkit.cli import cli

    runner = CliRunner()

    resource_url = str(dummy_csv)

    result = runner.invoke(
        cli,
        args=f"load table {resource_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"ERROR: {result.output}"

    assert "Loading data." in caplog.text
    assert "target=TableAddress(schema=None, table='dummy')" in caplog.text
    assert "Import succeeded (status: SUCCEEDED)" in caplog.text

    with ManagedCluster.from_env() as cluster:
        results = cluster.query("SELECT count(*) AS count FROM dummy;")
    assert results[0]["count"] >= 2


def test_parquet_import_remote(cloud_environment, caplog):
    """
    CLI test: Invoke `ctk load table ...` for a CrateDB Cloud Import, from a URL.
    """

    from cratedb_toolkit.cli import cli

    runner = CliRunner()

    resource_url = "https://github.com/daq-tools/skeem/raw/main/tests/testdata/basic.parquet"

    result = runner.invoke(
        cli,
        args=f"load table {resource_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"ERROR: {result.output}"

    assert "Loading data." in caplog.text
    assert "target=TableAddress(schema=None, table='basic')" in caplog.text
    assert "Import succeeded (status: SUCCEEDED)" in caplog.text

    with ManagedCluster.from_env() as cluster:
        results = cluster.query("SELECT count(*) AS count FROM basic;")
    assert results[0]["count"] >= 2
