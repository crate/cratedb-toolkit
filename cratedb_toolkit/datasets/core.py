import cratedb_toolkit.datasets.kaggle  # noqa: F401
import cratedb_toolkit.datasets.tutorial  # noqa: F401

from .model import Dataset
from .store import registry


def load_dataset(name: str) -> Dataset:
    return registry.find(name)
