import pytest


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
