import os

import pytest
from click.testing import CliRunner


@pytest.fixture
def dummy_csv(tmp_path):
    """
    Provide a dummy CSV file to the test cases.
    """
    csvfile = tmp_path / "dummy.csv"
    csvfile.write_text("name,value\ntemperature,42.42\nhumidity,84.84")
    return csvfile


def test_import_standalone_csv_pandas(cratedb, dummy_csv):
    """
    Invoke convenience function `import_csv_pandas`, and verify database content.
    """
    result = cratedb.database.import_csv_pandas(filepath=dummy_csv, tablename="foobar")
    assert result is None

    cratedb.database.run_sql("REFRESH TABLE foobar;")
    result = cratedb.database.run_sql("SELECT COUNT(*) FROM foobar;")
    assert result == [(2,)]


def test_import_standalone_csv_dask(cratedb, dummy_csv, needs_sqlalchemy2):
    """
    Invoke convenience function `import_csv_dask`, and verify database content.
    """
    pytest.importorskip("dask")
    result = cratedb.database.import_csv_dask(filepath=dummy_csv, tablename="foobar")
    assert result is None

    cratedb.database.run_sql("REFRESH TABLE foobar;")
    result = cratedb.database.run_sql("SELECT COUNT(*) FROM foobar;")
    assert result == [(2,)]


def test_import_standalone_csv_dask_with_progressbar(cratedb, dummy_csv, needs_sqlalchemy2):
    """
    Invoke convenience function `import_csv_dask`, and verify database content.
    This time, use `progress=True` to make Dask display a progress bar.
    However, the code does not verify it.
    """
    pytest.importorskip("dask")
    result = cratedb.database.import_csv_dask(filepath=dummy_csv, tablename="foobar", progress=True)
    assert result is None

    cratedb.database.run_sql("REFRESH TABLE foobar;")
    result = cratedb.database.run_sql("SELECT COUNT(*) FROM foobar;")
    assert result == [(2,)]


@pytest.mark.skipif(
    os.getenv("FORCE", "no") != "yes",
    reason="Only works when invoked exclusively, using "
    "`FORCE=yes pytest --no-cov tests/io/test_import.py -k managed_csv_local`. "
    "Otherwise croaks per `AssertionError: ERROR: The following arguments are required: --url`. "
    "We don't know why.",
)
def test_import_managed_csv_local(cloud_environment, dummy_csv, caplog):
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
    assert "Import succeeded (status: SUCCEEDED)" in caplog.messages

    # TODO: Read back data from database.


def test_import_managed_parquet_remote(cloud_environment, tmp_path, caplog):
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
    assert "Import succeeded (status: SUCCEEDED)" in caplog.messages

    # TODO: Read back data from database.
