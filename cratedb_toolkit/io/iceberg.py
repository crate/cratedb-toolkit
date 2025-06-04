import dataclasses
import logging

import polars as pl
import pyarrow.parquet as pq
import sqlalchemy as sa
from boltons.urlutils import URL
from pyiceberg.catalog import Catalog, load_catalog
from sqlalchemy_cratedb import insert_bulk

from cratedb_toolkit.model import DatabaseAddress

logger = logging.getLogger(__name__)


CHUNK_SIZE = 75_000


@dataclasses.dataclass
class IcebergAddress:
    path: str
    catalog: str
    table: str

    @classmethod
    def from_url(cls, url: str):
        iceberg_url = URL(url)
        if iceberg_url.host == ".":
            iceberg_url.path = iceberg_url.path.lstrip("/")
        return cls(
            path=iceberg_url.path,
            catalog=iceberg_url.query_params.get("catalog"),
            table=iceberg_url.query_params.get("table"),
        )

    def load_catalog(self) -> Catalog:
        return load_catalog(
            self.catalog,
            **{
                "type": "sql",
                "uri": f"sqlite:///{self.path}/pyiceberg_catalog.db",
                "warehouse": f"file://{self.path}",
            },
        )

    @property
    def identifier(self):
        return (self.catalog, self.table)

    def load_table(self) -> pl.LazyFrame:
        if self.catalog is not None:
            catalog = self.load_catalog()
            return catalog.load_table(self.identifier).to_polars()
        else:
            return pl.scan_iceberg(self.path)


def from_iceberg(source_url, cratedb_url, progress: bool = False):
    """
    Scan an Iceberg table from local filesystem or object store, and load into CrateDB.
    https://docs.pola.rs/api/python/stable/reference/api/polars.scan_iceberg.html

    Synopsis
    --------
    export CRATEDB_CLUSTER_URL=crate://crate@localhost:4200/testdrive/demo
    ctk load table "file+iceberg:var/lib/iceberg/default.db/taxi_dataset/metadata/00001-dc8e5ed2-dc29-4e39-b2e4-019e466af4c3.metadata.json"
    ctk load table "iceberg://./var/lib/iceberg/?catalog=default&table=taxi_dataset"
    """

    iceberg_address = IcebergAddress.from_url(source_url)

    # Parse parameters.
    logger.info(
        f"Iceberg address: Path: {iceberg_address.path}, catalog: {iceberg_address.catalog}, table: {iceberg_address.table}"
    )

    cratedb_address = DatabaseAddress.from_string(cratedb_url)
    cratedb_url, cratedb_table = cratedb_address.decode()
    if cratedb_table.table is None:
        raise ValueError("Table name is missing. Please adjust CrateDB database URL.")
    logger.info(f"Target address: {cratedb_address}")

    # Invoke copy operation.
    logger.info("Running Iceberg copy")
    engine = sa.create_engine(str(cratedb_url))

    pl.Config.set_streaming_chunk_size(CHUNK_SIZE)
    table = iceberg_address.load_table()

    # This conversion to pandas is zero-copy,
    # so we can utilize their SQL utils for free.
    # https://github.com/pola-rs/polars/issues/7852
    # Note: This code also uses the most efficient `insert_bulk` method with CrateDB.
    # https://cratedb.com/docs/sqlalchemy-cratedb/dataframe.html#efficient-insert-operations-with-pandas
    table.collect(streaming=True).to_pandas().to_sql(
        name=cratedb_table.table,
        schema=cratedb_table.schema,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=CHUNK_SIZE,
        method=insert_bulk,
    )

    # Note: This was much slower.
    # table.to_polars().collect(streaming=True).write_database(table_name=table_address.fullname, connection=engine, if_table_exists="replace")


def to_iceberg(source_url, target_url, progress: bool = False):
    """
    Synopsis
    --------
    export CRATEDB_CLUSTER_URL=crate://crate@localhost:4200/testdrive/demo
    ctk load table "iceberg://./var/lib/iceberg/?catalog=default&table=taxi_dataset"
    ctk save table "file+iceberg:var/lib/iceberg/default.db/taxi_dataset/metadata/00001-dc8e5ed2-dc29-4e39-b2e4-019e466af4c3.metadata.json"
    """

    iceberg_address = IcebergAddress.from_url(target_url)
    catalog = iceberg_address.load_catalog()
    print("catalog:", catalog)

    # https://py.iceberg.apache.org/#write-a-pyarrow-dataframe
    df = pq.read_table("tmp/yellow_tripdata_2023-01.parquet")

    # Create a new Iceberg table.
    catalog.create_namespace_if_not_exists("default")
    table = catalog.create_table_if_not_exists(
        "default.taxi_dataset",
        schema=df.schema,
    )

    # Append the dataframe to the table.
    table.append(df)
    len(table.scan().to_arrow())
