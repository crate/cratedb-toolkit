import dataclasses
import typing as t

import requests
import sqlparse

from cratedb_toolkit.util import DatabaseAdapter

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type: ignore[assignment]


@dataclasses.dataclass
class Dataset:
    """
    Wrapper around a dataset.
    """

    reference: str
    file: t.Optional[str] = None
    ddl: t.Optional[str] = None
    title: t.Optional[str] = None
    documentation: t.Optional[str] = None
    data_url: t.Optional[str] = None
    init_url: t.Optional[str] = None
    init_includes_loading: t.Optional[bool] = False

    # Native datasets wrapper handle, e.g. `datasets`
    # https://github.com/huggingface/datasets
    handle: t.Optional[t.Any] = None

    def acquire(self):
        if self.reference:
            if self.reference.startswith("huggingface://"):
                from datasets import load_dataset as load_dataset_huggingface

                upstream_name = self.reference.replace("huggingface://", "")
                self.handle = load_dataset_huggingface(upstream_name)
            if self.reference.startswith("kaggle://"):
                from cratedb_toolkit.datasets.util import load_dataset_kaggle

                # TODO: Use an URL library to split off the file fragment part.
                # kaggle://guillemservera/global-daily-climate-data/daily_weather.parquet
                # =>
                # kaggle://guillemservera/global-daily-climate-data, daily_weather.parquet
                upstream_name = self.reference.replace("kaggle://", "")
                parts = upstream_name.rsplit("/", 1)
                upstream_name = parts[0]
                self.file = parts[1]

                # FIXME: Don't use `DOWNLOAD` as folder. Use platformdirs.
                load_dataset_kaggle(dataset=upstream_name, path="DOWNLOAD")

    def dbtable(self, dburi: str, table: str):
        return DatasetToDatabaseTableAdapter(dataset=self, dburi=dburi, table=table)


@dataclasses.dataclass
class DatasetToDatabaseTableAdapter:
    """
    Run a dataset into a database table.
    """

    dataset: Dataset
    dburi: str
    table: str

    def __post_init__(self):
        self.init_sql = None
        self.db = DatabaseAdapter(dburi=self.dburi)

    def create(
        self,
        if_exists: Literal["append", "noop", "replace"] = "noop",
        drop: bool = False,
    ):
        """
        Load dataset into CrateDB, or prepare loading, using SQL.
        """

        self.dataset.acquire()
        if self.dataset.init_url:
            self.init_sql = requests.get(self.dataset.init_url, timeout=5).text
        elif self.dataset.ddl:
            self.init_sql = self.dataset.ddl

        if self.init_sql:
            self.init_sql = self.init_sql.format(table=self.table)

            if drop:
                self.db.drop_table(self.table)

            if if_exists == "replace":
                self.db.prune_table(self.table, errors="ignore")

            if not self.dataset.init_includes_loading:
                self.run_sql(self.init_sql)

    def load(
        self,
        if_exists: Literal["append", "noop", "replace"] = "noop",
        drop: bool = False,
    ):
        self.create(if_exists=if_exists, drop=drop)

        cardinality = self.db.count_records(self.table, errors="ignore")
        has_data = cardinality > 0

        if if_exists == "noop" and has_data:
            return

        if self.init_sql is None:
            raise ValueError("SQL for loading data is missing")
        self.run_sql(self.init_sql)

    def run_sql(self, sql: str):
        for statement in sqlparse.parse(sql):
            self.db.run_sql(str(statement))


class DatasetRegistry:
    """
    Manage multiple datasets, and provide discovery.
    """

    def __init__(self):
        self.datasets: t.List[Dataset] = []

    def add(self, dataset: Dataset):
        self.datasets.append(dataset)

    def find(self, reference: str) -> Dataset:
        for dataset in self.datasets:
            if dataset.reference == reference:
                return dataset
        raise KeyError(f"Dataset not found: {reference}")
