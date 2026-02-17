from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner
from pueblo.testing.dataframe import DataFrameFactory

from cratedb_toolkit.cli import cli

pl = pytest.importorskip("polars", reason="Skipping Iceberg tests because 'polars' package is not installed")

if not hasattr(pd.DataFrame, "to_iceberg"):
    pytest.skip("Older pandas releases do not support Apache Iceberg", allow_module_level=True)


@pytest.fixture
def example_iceberg(tmp_path) -> Path:
    from pyiceberg.catalog import load_catalog  # noqa: E402

    catalog_properties = {
        "uri": f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
        "warehouse": str(tmp_path),
    }
    catalog = load_catalog("default", **catalog_properties)
    catalog.create_namespace_if_not_exists("demo")

    dff = DataFrameFactory()
    df = dff.make_mixed()
    df.to_iceberg(
        "demo.mixed",
        catalog_name="default",
        catalog_properties=catalog_properties,
    )
    table = catalog.load_table("demo.mixed")
    metadata_location = find_iceberg_data_metadata_location(Path(table.location()))

    catalog.close()
    return metadata_location


def find_iceberg_data_metadata_location(table_path: Path) -> Path:
    """
    Resolve path to metadata.json file in Iceberg table.
    This path is needed for `polars.scan_iceberg()`.
    """
    files = sorted((table_path / "metadata").glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No Iceberg metadata JSON found under {table_path / 'metadata'}")
    return files[-1]


def test_load_iceberg_table(cratedb, example_iceberg):
    """
    Verify loading data from an Iceberg table into CrateDB.
    """

    # Source and target URLs.
    source_url = f"file+iceberg://{example_iceberg}"
    target_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": target_url})
    result = runner.invoke(
        cli,
        args=f"load table {source_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    db = cratedb.database
    assert db.table_exists("testdrive.demo") is True, "Table `testdrive.demo` does not exist"
    assert db.refresh_table("testdrive.demo") is True, "Refreshing table `testdrive.demo` failed"
    assert db.count_records("testdrive.demo") == 5, "Table `testdrive.demo` does not include expected amount of records"


def test_save_iceberg_table(cratedb, tmp_path):
    """
    Verify saving data from CrateDB into an Iceberg table.
    """

    # Source and target URLs.
    source_url = f"{cratedb.get_connection_url()}/sys/summits"
    target_url = f"file+iceberg://{tmp_path}/?catalog=default&namespace=sys&table=summits"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": source_url})
    result = runner.invoke(
        cli,
        args=f"save table {target_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in Iceberg table.
    metadata_location = find_iceberg_data_metadata_location(tmp_path / "sys" / "summits")
    table = pl.scan_iceberg(str(metadata_location))
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
    assert table.collect().height >= 1600, "Iceberg table does not include expected amount of records"
