import os
from pathlib import Path

import pytest
from slugify import slugify

from cratedb_toolkit.datasets import load_dataset
from cratedb_toolkit.datasets.model import Dataset
from cratedb_toolkit.datasets.store import registry
from cratedb_toolkit.util.database import DatabaseAdapter


class Enumerator:
    @property
    def datasets(self):
        return registry.datasets

    @property
    def names(self):
        return [dataset.reference for dataset in self.datasets]


items = Enumerator()


@pytest.mark.parametrize("dataset", items.datasets, ids=items.names)
def test_dataset_replace(database: DatabaseAdapter, dataset: Dataset):
    """
    Verify loading dataset into CrateDB table, with "if_exists=replace" option.
    """

    # Skip Kaggle tests when having no authentication information.
    kaggle_auth_exists = Path("~/.kaggle/kaggle.json").exists() or (
        "KAGGLE_USERNAME" in os.environ and "KAGGLE_KEY" in os.environ
    )
    if dataset.title and "Weather" in dataset.title and not kaggle_auth_exists:
        raise pytest.skip(f"Kaggle dataset can not be tested without authentication: {dataset.reference}")

    dataset = load_dataset(dataset.reference)
    tablename = slugify(dataset.reference, separator="_")
    dataset.dbtable(dburi=database.dburi, table=tablename).load(if_exists="replace")


def test_dataset_noop(database: DatabaseAdapter):
    """
    Verify loading dataset into CrateDB table, with "if_exists=noop" option.
    """
    dataset = load_dataset("tutorial/weather-basic")
    tablename = slugify(dataset.reference, separator="_")
    dataset.dbtable(dburi=database.dburi, table=tablename).load(if_exists="noop")


def test_dataset_drop(database: DatabaseAdapter):
    """
    Verify loading dataset into CrateDB table, with "drop table" option.
    """
    dataset = load_dataset("tutorial/weather-basic")
    tablename = slugify(dataset.reference, separator="_")
    dataset.dbtable(dburi=database.dburi, table=tablename).load(drop=True)
