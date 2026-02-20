from pathlib import Path

import pytest
from click.testing import CliRunner
from pueblo.testing.dataframe import DataFrameFactory

from cratedb_toolkit.cli import cli

pl = pytest.importorskip("polars", reason="Skipping DeltaLake tests because 'polars' package is not installed")
pytest.importorskip("deltalake", reason="Skipping DeltaLake tests because 'deltalake' package is not installed")


@pytest.fixture
def example_deltalake(tmp_path) -> Path:
    """
    A pytest fixture that creates a dummy Apache DeltaLake table for testing.
    """
    dff = DataFrameFactory()
    df = dff.make_mixed()
    pl.from_pandas(df).write_delta(
        target=tmp_path,
        mode="overwrite",
    )
    return tmp_path


def test_load_deltalake_table_filesystem_base_success(cratedb, example_deltalake):
    """
    Verify loading data from a DeltaLake table into CrateDB.
    """

    # Source and target URLs.
    source_url = f"file+deltalake://{example_deltalake}"
    target_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": target_url})
    result = runner.invoke(
        cli,
        args=["load", "table", source_url],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    db = cratedb.database
    assert db.table_exists("testdrive.demo") is True, "Table `testdrive.demo` does not exist"
    assert db.refresh_table("testdrive.demo") is True, "Refreshing table `testdrive.demo` failed"
    assert db.count_records("testdrive.demo") == 5, "Table `testdrive.demo` does not include expected amount of records"


def test_load_deltalake_table_filesystem_version_integer_success(cratedb, example_deltalake):
    """
    Verify loading data from a DeltaLake table into CrateDB, with version number.
    """

    # Source and target URLs.
    source_url = f"file+deltalake://{example_deltalake}?version=0"
    target_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": target_url})
    result = runner.invoke(
        cli,
        args=["load", "table", source_url],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    db = cratedb.database
    assert db.table_exists("testdrive.demo") is True, "Table `testdrive.demo` does not exist"
    assert db.refresh_table("testdrive.demo") is True, "Refreshing table `testdrive.demo` failed"
    assert db.count_records("testdrive.demo") == 5, "Table `testdrive.demo` does not include expected amount of records"


def test_load_deltalake_table_filesystem_version_integer_invalid(cratedb, example_deltalake):
    """
    Verify loading data from a DeltaLake table into CrateDB, with invalid version number.
    """
    from deltalake.exceptions import DeltaError

    # Source and target URLs.
    source_url = f"file+deltalake://{example_deltalake}?version=99"
    target_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": target_url})
    with pytest.raises(DeltaError) as exc_info:
        runner.invoke(
            cli,
            args=["load", "table", source_url],
            catch_exceptions=False,
        )
    assert exc_info.match(
        "Kernel error: Generic delta kernel error: "
        "LogSegment end version 0 not the same as the specified end version 99"
    )


def test_save_deltalake_table_filesystem(cratedb, tmp_path):
    """
    Verify saving data from CrateDB into a DeltaLake table.
    """

    # Source and target URLs.
    source_url = f"{cratedb.get_connection_url()}/sys/summits"
    target_url = f"file+deltalake://{tmp_path}"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": source_url})
    result = runner.invoke(
        cli,
        args=["save", "table", target_url],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in DeltaLake table.
    table = pl.scan_delta(str(tmp_path))
    assert sorted(table.collect_schema().names()) == [
        "classification",
        "coordinates",
        "country",
        "first_ascent",
        "height",
        "mountain",
        "prominence",
        "range",
        "region",
    ]
    assert table.collect().height >= 1600, "DeltaLake table does not include expected amount of records"
