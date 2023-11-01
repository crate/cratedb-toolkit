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


def test_import_csv_dask(cratedb, dummy_csv):
    """
    Invoke convenience function `import_csv_dask`, and verify database content.
    """
    result = cratedb.database.import_csv_dask(filepath=dummy_csv, tablename="foobar")
    assert result is None

    cratedb.database.run_sql("REFRESH TABLE foobar;")
    result = cratedb.database.run_sql("SELECT COUNT(*) FROM foobar;")
    assert result == [(2,)]


def test_import_csv_dask_with_progressbar(cratedb, dummy_csv):
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


@responses.activate
def test_import_croud_parquet(caplog):
    """
    CLI test: Invoke `ctk load table ...` for a CrateDB Cloud Import.
    """

    responses.add(
        responses.Response(
            method="GET",
            url="https://console.cratedb.cloud/api/v2/clusters/e1e38d92-a650-48f1-8a70-8133f2d5c400/",
            json={"url": "https://testdrive.example.org:4200/"},
        )
    )
    responses.add(
        responses.Response(
            method="POST",
            url="https://console.cratedb.cloud/api/v2/clusters/e1e38d92-a650-48f1-8a70-8133f2d5c400/import-jobs/",
            json={"id": "testdrive-job-id", "status": "REGISTERED"},
        )
    )
    responses.add(
        responses.Response(
            method="GET",
            url="https://console.cratedb.cloud/api/v2/clusters/e1e38d92-a650-48f1-8a70-8133f2d5c400/import-jobs/",
            json=[
                {
                    "id": "testdrive-job-id",
                    "status": "SUCCEEDED",
                    "progress": {"message": "Import succeeded"},
                    "destination": {"table": "basic"},
                }
            ],
        )
    )

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
        f"Loading resource. "
        f"source=CloudIoResource(url='{resource_url}', format=None, compression=None), "
        f"target=CloudIoTarget(schema=None, table=None)" in caplog.messages
    )
    assert "Import succeeded (status: SUCCEEDED)" in caplog.messages
