import dataclasses
import typing as t

import requests
import sqlparse

from cratedb_toolkit.util import DatabaseAdapter


@dataclasses.dataclass
class Dataset:
    """
    Wrapper around a dataset.
    """

    name: str
    data_url: str
    load_url: str

    # Native datasets wrapper handle, e.g. `datasets`
    # https://github.com/huggingface/datasets
    handle: t.Optional[t.Any] = None

    def to_cratedb(
        self,
        dburi: str,
        table: str,
        if_exists: t.Literal["append", "noop", "replace"] = "noop",
        drop: bool = False,
    ):
        """
        Load dataset into CrateDB, using SQL.
        """
        sql = requests.get(self.load_url, timeout=5).text
        sql = sql.format(table=table)
        db = DatabaseAdapter(dburi=dburi)

        if drop:
            db.drop_table(table)

        if if_exists == "replace":
            db.prune_table(table, errors="ignore")

        cardinality = db.count_records(table, errors="ignore")
        has_data = cardinality > 0

        if not has_data and not if_exists == "noop":
            for statement in sqlparse.parse(sql):
                db.run_sql(str(statement))


class DatasetRegistry:
    """
    Manage multiple datasets, and provide discovery.
    """

    def __init__(self):
        self.datasets: t.List[Dataset] = []

    def add(self, dataset: Dataset):
        self.datasets.append(dataset)

    def find(self, name: str) -> Dataset:
        for dataset in self.datasets:
            if dataset.name == name:
                return dataset
        raise KeyError(f"Dataset not found: {name}")
