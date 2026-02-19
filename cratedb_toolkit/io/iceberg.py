"""
Apache Iceberg integration for CrateDB Toolkit.

This module provides functionality to transfer data between Iceberg tables
and CrateDB databases, supporting both import and export operations.
"""

import dataclasses
import logging
import tempfile
from copy import copy
from typing import Optional

import pandas as pd
import polars as pl
import sqlalchemy as sa
from boltons.urlutils import URL
from pyiceberg.catalog import Catalog, load_catalog
from sqlalchemy_cratedb import insert_bulk

from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util.data import asbool

logger = logging.getLogger(__name__)


DEFAULT_BATCH_SIZE = 75_000


@dataclasses.dataclass
class IcebergAddress:
    """
    Represent an Apache Iceberg location and provide loader methods.
    """

    url: URL
    location: str
    catalog: str
    namespace: str
    table: str
    batch_size: Optional[int] = None

    def __post_init__(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.temporary_catalog_location = self.tmpdir.name

    def __del__(self):
        self.tmpdir.cleanup()

    @classmethod
    def from_url(cls, url: str):
        """
        Parse an Iceberg location and return an IcebergAddress object.

        Examples:

        file+iceberg://./var/lib/iceberg/demo/taxi-tiny/metadata/00003-dd9223cb
        s3+iceberg://bucket1/demo/taxi-tiny/metadata/00003-dd9223cb
        s3+iceberg://bucket1/?catalog-uri=http://iceberg-catalog.example.org:5000&catalog-token=foo
        s3+iceberg://bucket1/?catalog-uri=thrift://localhost:9083/&catalog-credential=t-1234:secret...
        """
        iceberg_url = URL(url)
        if iceberg_url.scheme.startswith("file"):
            if iceberg_url.host == ".":
                iceberg_url.path = iceberg_url.path.lstrip("/")
            location = iceberg_url.path
        else:
            if iceberg_url.scheme.endswith("+iceberg"):
                iceberg_url.scheme = iceberg_url.scheme.replace("+iceberg", "")
            u2 = copy(iceberg_url)
            u2.query_params.clear()
            location = str(u2)
        return cls(
            url=iceberg_url,
            location=location,
            catalog=iceberg_url.query_params.get("catalog"),
            namespace=iceberg_url.query_params.get("namespace"),
            table=iceberg_url.query_params.get("table"),
            batch_size=int(iceberg_url.query_params.get("batch-size", DEFAULT_BATCH_SIZE)),
        )

    def load_catalog(self) -> Catalog:
        """
        Load the Iceberg catalog with appropriate configuration.
        """
        # TODO: Consider accepting catalog configuration as parameters
        #       to support different catalog types (Hive, REST, etc.).
        return load_catalog(self.catalog, **self.catalog_properties)

    @property
    def catalog_properties(self):
        """
        Provide Iceberg catalog properties.
        https://py.iceberg.apache.org/reference/pyiceberg/catalog/
        """
        return {
            "uri": self.url.query_params.get(
                "catalog-uri", f"sqlite:///{self.temporary_catalog_location}/pyiceberg_catalog.db"
            ),
            "token": self.url.query_params.get("catalog-token"),
            "warehouse": self.location,  # TODO: Is the `file://` prefix faster when accessing the local filesystem?
        }

    @property
    def storage_options(self):
        opts = {
            "s3.endpoint": self.url.query_params.get("s3.endpoint"),
            "s3.region": self.url.query_params.get("s3.region"),
            "s3.access-key-id": self.url.query_params.get("s3.access-key-id"),
            "s3.secret-access-key": self.url.query_params.get("s3.secret-access-key"),
        }
        return {k: v for k, v in opts.items() if v is not None}
        """
        Provide Iceberg storage properties.
        https://py.iceberg.apache.org/configuration/#fileio
        """

    @property
    def identifier(self):
        """
        Return the catalog-table identifier tuple.
        """
        return (self.namespace, self.table)

    def load_table(self) -> pl.LazyFrame:
        """
        Load the Iceberg table as a Polars LazyFrame.

        Either load a table from a catalog, or by scanning the filesystem.
        """
        if self.catalog is not None:
            catalog = self.load_catalog()
            return catalog.load_table(self.identifier).to_polars()
        return pl.scan_iceberg(self.location, storage_options=self.storage_options)


def from_iceberg(source_url, target_url, progress: bool = False):
    """
    Scan an Apache Iceberg table from local filesystem or object store, and load into CrateDB.
    Documentation: https://cratedb-toolkit.readthedocs.io/io/iceberg/#load

    See also: https://docs.pola.rs/api/python/stable/reference/api/polars.scan_iceberg.html

    # Synopsis: Load from metadata file on filesystem.
    ctk load table \
        "file+iceberg://./iceberg/demo/taxi/metadata/00001-79d5b044-8bce-46dd-b21c-83679a01c986.metadata.json" \
        --cluster-url="crate://crate@localhost:4200/demo/taxi"
    """

    iceberg_address = IcebergAddress.from_url(source_url)

    # Display parameters.
    logger.info(f"Iceberg address: Path: {iceberg_address.location}")

    cratedb_address = DatabaseAddress.from_string(target_url)
    cratedb_url, cratedb_table = cratedb_address.decode()
    if_exists = URL(target_url).query_params.get("if-exists") or "fail"
    if cratedb_table.table is None:
        raise ValueError("Table name is missing. Please adjust CrateDB database URL.")
    logger.info("Target address: %s", cratedb_address)

    # Invoke copy operation.
    chunksize = iceberg_address.batch_size
    logger.info(f"Running Iceberg copy with chunksize={chunksize}")
    engine = sa.create_engine(str(cratedb_url))

    # This conversion to pandas is zero-copy,
    # so we can utilize their SQL utils for free.
    # https://github.com/pola-rs/polars/issues/7852
    # Note: This code also uses the most efficient `insert_bulk` method with CrateDB.
    # https://cratedb.com/docs/sqlalchemy-cratedb/dataframe.html#efficient-insert-operations-with-pandas
    with pl.Config(streaming_chunk_size=chunksize):
        table = iceberg_address.load_table()
        table.collect(engine="streaming").to_pandas().to_sql(
            name=cratedb_table.table,
            schema=cratedb_table.schema,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
            method=insert_bulk,
        )

    # Note: This variant was much slower.
    """
    table.to_polars().collect(streaming=True).write_database(
        table_name=cratedb_table.fullname, connection=engine, if_table_exists="replace"
    )
    """

    return True


def to_iceberg(source_url, target_url, progress: bool = False):
    """
    Export a table from CrateDB into an Apache Iceberg table.
    Documentation: https://cratedb-toolkit.readthedocs.io/io/iceberg/#save

    # Synopsis: Save to filesystem.
    ctk save table \
        --cluster-url="crate://crate@localhost:4200/demo/taxi" \
        "file+iceberg://./iceberg/?catalog=default&namespace=demo&table=taxi"
    """

    cratedb_address = DatabaseAddress.from_string(source_url)
    cratedb_url, cratedb_table = cratedb_address.decode()
    if cratedb_table.table is None:
        raise ValueError("Table name is missing. Please adjust CrateDB database URL.")
    logger.info(f"Source address: {cratedb_address}")

    iceberg_address = IcebergAddress.from_url(target_url)
    if not iceberg_address.namespace:
        raise ValueError("Iceberg table namespace is missing or empty")
    if not iceberg_address.table:
        raise ValueError("Iceberg table name is missing or empty")
    iceberg_identifier = iceberg_address.namespace + "." + iceberg_address.table
    iceberg_append = asbool(URL(target_url).query_params.get("append")) or False
    logger.info(
        f"Iceberg address: Path: {iceberg_address.location}, "
        f"catalog: {iceberg_address.catalog}, namespace: {iceberg_address.namespace}, table: {iceberg_address.table}"
    )

    # Prepare namespace and optionally drop table when `append=false`.
    catalog = iceberg_address.load_catalog()
    catalog.create_namespace_if_not_exists(iceberg_address.namespace)
    if catalog.table_exists(iceberg_identifier) and not iceberg_append:
        catalog.drop_table(iceberg_identifier)
    catalog.close()

    # Invoke copy operation.
    chunksize = int(URL(source_url).query_params.get("batch-size", DEFAULT_BATCH_SIZE))
    logger.info(f"Running Iceberg copy with chunksize={chunksize}")
    catalog_properties = {}
    catalog_properties.update(iceberg_address.catalog_properties)
    catalog_properties.update(iceberg_address.storage_options)
    engine = sa.create_engine(str(cratedb_url))
    with engine.connect() as connection:
        for chunk in pd.read_sql_table(
            table_name=cratedb_table.table, schema=cratedb_table.schema, con=connection, chunksize=chunksize
        ):
            chunk.to_iceberg(
                table_identifier=iceberg_identifier,
                catalog_name=iceberg_address.catalog,
                catalog_properties=catalog_properties,
                append=True,
            )

    return True
