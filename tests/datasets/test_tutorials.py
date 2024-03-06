import pytest
from slugify import slugify

from cratedb_toolkit.datasets import load_dataset
from cratedb_toolkit.datasets.model import Dataset
from cratedb_toolkit.datasets.store import registry
from cratedb_toolkit.util import DatabaseAdapter

datasets = registry.datasets
dataset_names = [dataset.name for dataset in datasets]


@pytest.mark.parametrize("dataset", datasets, ids=dataset_names)
def test_dataset_replace(database: DatabaseAdapter, dataset: Dataset):
    """
    Verify loading dataset into CrateDB table, with "if_exists=replace" option.
    """
    dataset = load_dataset(dataset.name)
    tablename = slugify(dataset.name, separator="_")
    dataset.to_cratedb(dburi=database.dburi, table=tablename, if_exists="replace")


def test_dataset_noop(database: DatabaseAdapter):
    """
    Verify loading dataset into CrateDB table, with "if_exists=noop" option.
    """
    dataset = load_dataset("tutorial/weather-basic")
    tablename = slugify(dataset.name, separator="_")
    dataset.to_cratedb(dburi=database.dburi, table=tablename, if_exists="noop")


def test_dataset_drop(database: DatabaseAdapter):
    """
    Verify loading dataset into CrateDB table, with "drop table" option.
    """
    dataset = load_dataset("tutorial/weather-basic")
    tablename = slugify(dataset.name, separator="_")
    dataset.to_cratedb(dburi=database.dburi, table=tablename, drop=True)
