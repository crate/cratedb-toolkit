import pytest
import responses


@pytest.fixture
def dummy_csv(tmp_path):
    """
    Provide a dummy CSV file to the test cases.
    """
    csvfile = tmp_path / "dummy.csv"
    csvfile.write_text("name,value\ntemperature,42.42\nhumidity,84.84")
    return csvfile


def test_import_csv_pandas(cratedb, dummy_csv):
    """
    Invoke convenience function `import_csv_pandas`, and verify database content.
    """
    result = cratedb.database.import_csv_pandas(filepath=dummy_csv, tablename="foobar")
    assert result is None

    cratedb.database.run_sql("REFRESH TABLE foobar;")
    result = cratedb.database.run_sql("SELECT COUNT(*) FROM foobar;")
    assert result == [(2,)]


def test_import_csv_dask(cratedb, dummy_csv, needs_sqlalchemy2):
    """
    Invoke convenience function `import_csv_dask`, and verify database content.
    """
    result = cratedb.database.import_csv_dask(filepath=dummy_csv, tablename="foobar")
    assert result is None

    cratedb.database.run_sql("REFRESH TABLE foobar;")
    result = cratedb.database.run_sql("SELECT COUNT(*) FROM foobar;")
    assert result == [(2,)]


def test_import_csv_dask_with_progressbar(cratedb, dummy_csv, needs_sqlalchemy2):
    """
    Invoke convenience function `import_csv_dask`, and verify database content.
    This time, use `progress=True` to make Dask display a progress bar.
    However, the code does not verify it.
    """
    result = cratedb.database.import_csv_dask(filepath=dummy_csv, tablename="foobar", progress=True)
    assert result is None

    cratedb.database.run_sql("REFRESH TABLE foobar;")
    result = cratedb.database.run_sql("SELECT COUNT(*) FROM foobar;")
    assert result == [(2,)]


@pytest.mark.skip("Does not work. Why?")
@responses.activate
def test_import_cloud_file(tmp_path, caplog, cloud_cluster_mock):
    """
    CLI test: Invoke `ctk load table ...` for a CrateDB Cloud Import, from a local file.
    """

    from click.testing import CliRunner

    from cratedb_toolkit.cli import cli

    csv_file = tmp_path / "test.csv"
    csv_file.write_text("temperature,humidity\n42.42,84.84\n")

    runner = CliRunner()

    resource_url = str(csv_file)

    result = runner.invoke(
        cli,
        args=f"load table {resource_url}",
        env={"CRATEDB_CLOUD_CLUSTER_ID": "e1e38d92-a650-48f1-8a70-8133f2d5c400"},
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"ERROR: {result.output}"

    assert (
        f"Loading data. "
        f"source=InputOutputResource(url='{resource_url}', format=None, compression=None), "
        f"target=TableAddress(schema=None, table='test')" in caplog.messages
    )

    assert "Import succeeded (status: SUCCEEDED)" in caplog.messages


@responses.activate
def test_import_cloud_url(caplog, cloud_cluster_mock):
    """
    CLI test: Invoke `ctk load table ...` for a CrateDB Cloud Import, from a URL.
    """

    from click.testing import CliRunner

    from cratedb_toolkit.cli import cli

    runner = CliRunner()

    resource_url = "https://github.com/daq-tools/skeem/raw/main/tests/testdata/basic.parquet"

    result = runner.invoke(
        cli,
        args=f"load table {resource_url}",
        env={"CRATEDB_CLOUD_CLUSTER_ID": "e1e38d92-a650-48f1-8a70-8133f2d5c400"},
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    assert (
        f"Loading data. "
        f"source=InputOutputResource(url='{resource_url}', format=None, compression=None), "
        f"target=TableAddress(schema=None, table='basic')" in caplog.messages
    )

    assert "Import succeeded (status: SUCCEEDED)" in caplog.messages
