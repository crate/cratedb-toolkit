import dataclasses
import logging
import tempfile
from copy import copy

import pandas as pd
import polars as pl
import sqlalchemy as sa
from boltons.urlutils import URL
from pyiceberg.catalog import Catalog, load_catalog
from sqlalchemy_cratedb import insert_bulk

from cratedb_toolkit.model import DatabaseAddress

logger = logging.getLogger(__name__)


CHUNK_SIZE = 75_000


@dataclasses.dataclass
class IcebergAddress:
    url: URL
    location: str
    catalog: str
    namespace: str
    table: str

    def __post_init__(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.temporary_catalog_location = self.tmpdir.name

    def __del__(self):
        self.tmpdir.cleanup()

    @classmethod
    def from_url(cls, url: str):
        iceberg_url = URL(url)
        if iceberg_url.scheme.startswith("file"):
            if iceberg_url.host == ".":
                iceberg_url.path = iceberg_url.path.lstrip("/")
            location = iceberg_url.path
        else:
            if iceberg_url.scheme.endswith("+iceberg"):
                iceberg_url.scheme = iceberg_url.scheme.replace("+iceberg", "")
            u2 = copy(iceberg_url)
            u2._query = ""
            location = str(u2)
        return cls(
            url=iceberg_url,
            location=location,
            catalog=iceberg_url.query_params.get("catalog"),
            namespace=iceberg_url.query_params.get("namespace"),
            table=iceberg_url.query_params.get("table"),
        )

    def load_catalog(self) -> Catalog:
        return load_catalog(self.catalog, **self.catalog_properties)

    @property
    def catalog_properties(self):
        return {
            "uri": self.url.query_params.get(
                "catalog-uri", f"sqlite:///{self.temporary_catalog_location}/pyiceberg_catalog.db"
            ),
            "token": self.url.query_params.get("catalog-token"),
            "warehouse": self.location,  # TODO: Is the `file://` prefix faster when accessing the local filesystem?
        }

    @property
    def storage_options(self):
        return {
            "s3.endpoint": self.url.query_params.get("s3.endpoint"),
            "s3.region": self.url.query_params.get("s3.region"),
            "s3.access-key-id": self.url.query_params.get("s3.access-key-id"),
            "s3.secret-access-key": self.url.query_params.get("s3.secret-access-key"),
        }

    @property
    def identifier(self):
        return (self.namespace, self.table)

    def load_table(self) -> pl.LazyFrame:
        """
        Load a table from a catalog, or by scanning the filesystem.
        """
        if self.catalog is not None:
            catalog = self.load_catalog()
            return catalog.load_table(self.identifier).to_polars()
        else:
            return pl.scan_iceberg(self.location, storage_options=self.storage_options)


def from_iceberg(source_url, cratedb_url, progress: bool = False):
    """
    Scan an Iceberg table from local filesystem or object store, and load into CrateDB.
    https://docs.pola.rs/api/python/stable/reference/api/polars.scan_iceberg.html

    Synopsis
    --------

    # Load from metadata file on filesystem.
    ctk load table \
        "file+iceberg://./var/lib/iceberg/demo/taxi_dataset/metadata/00001-79d5b044-8bce-46dd-b21c-83679a01c986.metadata.json" \
        --cluster-url="crate://crate@localhost:4200/demo/taxi_dataset"

    # Load from metadata file on AWS S3.
    ctk load table \
        "s3+iceberg://bucket1/demo/taxi-tiny/metadata/00003-dd9223cb-6d11-474b-8d09-3182d45862f4.metadata.json?s3.access-key-id=<your_access_key_id>&s3.secret-access-key=<your_secret_access_key>&s3.endpoint=<endpoint_url>&s3.region=<s3-region>" \
        --cluster-url="crate://crate@localhost:4200/demo/taxi-tiny"

    # Load from REST catalog on AWS S3.
    ctk load table \
        "s3+iceberg://bucket1/?catalog-uri=http://localhost:5001&catalog-token=foo&catalog=default&namespace=demo&table=taxi-tiny&s3.access-key-id=<your_access_key_id>&s3.secret-access-key=<your_secret_access_key>&s3.endpoint=<endpoint_url>&s3.region=<s3-region>" \
        --cluster-url="crate://crate@localhost:4200/demo/taxi-tiny3"

    """  # noqa:E501

    iceberg_address = IcebergAddress.from_url(source_url)

    # Display parameters.
    logger.info(f"Iceberg address: Path: {iceberg_address.location}")

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
        if_exists="replace",  # TODO: Make available via parameter.
        index=False,
        chunksize=CHUNK_SIZE,  # TODO: Make configurable.
        method=insert_bulk,
    )

    # Note: This was much slower.
    # table.to_polars().collect(streaming=True) \  # noqa: ERA001
    #     .write_database(table_name=table_address.fullname, connection=engine, if_table_exists="replace")


def to_iceberg(cratedb_url, target_url, progress: bool = False):
    """
    Synopsis
    --------
    ctk save table \
        --cluster-url="crate://crate@localhost:4200/demo/taxi_dataset" \
        "file+iceberg://./var/lib/iceberg/?catalog=default&namespace=demo&table=taxi_dataset"

    ctk save table \
        --cluster-url="crate://crate@localhost:4200/demo/taxi-tiny" \
        "s3+iceberg://bucket1/?catalog=default&namespace=demo&table=taxi-tiny&s3.access-key-id=<your_access_key_id>&s3.secret-access-key=<your_secret_access_key>&s3.endpoint=<endpoint_url>&s3.region=<s3-region>"
    """  # noqa:E501

    cratedb_address = DatabaseAddress.from_string(cratedb_url)
    cratedb_url, cratedb_table = cratedb_address.decode()
    if cratedb_table.table is None:
        raise ValueError("Table name is missing. Please adjust CrateDB database URL.")
    logger.info(f"Source address: {cratedb_address}")

    iceberg_address = IcebergAddress.from_url(target_url)
    logger.info(
        f"Iceberg address: Path: {iceberg_address.location}, "
        f"catalog: {iceberg_address.catalog}, namespace: {iceberg_address.namespace}, table: {iceberg_address.table}"
    )

    # Prepare namespace.
    catalog = iceberg_address.load_catalog()
    catalog.create_namespace_if_not_exists(iceberg_address.namespace)
    catalog.close()

    # Invoke copy operation.
    logger.info("Running Iceberg copy")
    catalog_properties = {}
    catalog_properties.update(iceberg_address.catalog_properties)
    catalog_properties.update(iceberg_address.storage_options)
    engine = sa.create_engine(str(cratedb_url))
    with engine.connect() as connection:
        pd.read_sql_table(table_name=cratedb_table.table, schema=cratedb_table.schema, con=connection).to_iceberg(
            iceberg_address.namespace + "." + iceberg_address.table,
            catalog_name=iceberg_address.catalog,
            catalog_properties=catalog_properties,
            append=False,  # TODO: Make available via parameter.
        )
