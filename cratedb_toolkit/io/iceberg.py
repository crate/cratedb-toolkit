"""
Apache Iceberg integration for CrateDB Toolkit.

This module provides functionality to transfer data between Iceberg tables
and CrateDB databases, supporting both import and export operations.
"""

import dataclasses
import logging
import tempfile
from typing import Dict, List, Optional

import polars as pl
from boltons.urlutils import URL
from pyiceberg.catalog import Catalog, load_catalog

from cratedb_toolkit.io.util import parse_uri, polars_to_cratedb, read_cratedb
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
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE

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
        url_obj, location = parse_uri(url, "iceberg")
        return cls(
            url=url_obj,
            location=location,
            catalog=url_obj.query_params.get("catalog"),
            namespace=url_obj.query_params.get("namespace"),
            table=url_obj.query_params.get("table"),
            batch_size=int(url_obj.query_params.get("batch-size", DEFAULT_BATCH_SIZE)),
        )

    def load_catalog(self) -> Catalog:
        """
        Load the Iceberg catalog with appropriate configuration.
        """
        return load_catalog(self.catalog, **self.catalog_properties)

    @property
    def catalog_properties(self):
        """
        Provide Iceberg catalog properties.
        https://py.iceberg.apache.org/configuration/#catalogs
        https://py.iceberg.apache.org/reference/pyiceberg/catalog/
        """
        opts = {
            "uri": self.url.query_params.get(
                "catalog-uri", f"sqlite:///{self.temporary_catalog_location}/pyiceberg_catalog.db"
            ),
            "credential": self.url.query_params.get("catalog-credential"),
            "token": self.url.query_params.get("catalog-token"),
            "type": self.url.query_params.get("catalog-type"),
            "warehouse": self.location,  # TODO: Is the `file://` prefix faster when accessing the local filesystem?
        }
        prefixes = ["dynamodb.", "gcp.", "glue."]
        opts.update(self.collect_properties(self.url.query_params, prefixes))
        return {k: v for k, v in opts.items() if v is not None}

    @property
    def storage_options(self):
        """
        Provide Iceberg storage properties.
        https://py.iceberg.apache.org/configuration/#fileio
        """
        prefixes = ["adls.", "gcs.", "hdfs.", "hf.", "s3."]
        return self.collect_properties(self.url.query_params, prefixes)

    @staticmethod
    def collect_properties(query_params: Dict, prefixes: List) -> Dict[str, str]:
        """
        Collect parameters from URL query string.
        """
        opts = {}
        for name, value in query_params.items():
            for prefix in prefixes:
                if name.startswith(prefix) and value is not None:
                    opts[name] = value
                    break
        return opts

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
    source = IcebergAddress.from_url(source_url)
    logger.info(f"Iceberg address: {source.location}")
    return polars_to_cratedb(
        frame=source.load_table(),
        target_url=target_url,
        chunk_size=source.batch_size,
    )


def to_iceberg(source_url, target_url, progress: bool = False):
    """
    Export a table from CrateDB into an Apache Iceberg table.
    Documentation: https://cratedb-toolkit.readthedocs.io/io/iceberg/#save

    See also: https://pandas.pydata.org/docs/dev/reference/api/pandas.DataFrame.to_iceberg.html

    # Synopsis: Save to filesystem.
    ctk save table \
        --cluster-url="crate://crate@localhost:4200/demo/taxi" \
        "file+iceberg://./iceberg/?catalog=default&namespace=demo&table=taxi"
    """

    # Decode Iceberg target address.
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

    # Prepare Iceberg namespace and optionally drop table when `append=false`.
    catalog = iceberg_address.load_catalog()
    catalog.create_namespace_if_not_exists(iceberg_address.namespace)
    if catalog.table_exists(iceberg_identifier) and not iceberg_append:
        catalog.drop_table(iceberg_identifier)
    catalog.close()

    # Prepare Iceberg catalog and storage options.
    catalog_properties = {}
    catalog_properties.update(iceberg_address.catalog_properties)
    catalog_properties.update(iceberg_address.storage_options)

    # Invoke copy operation.
    for chunk in read_cratedb(source_url, default_batch_size=DEFAULT_BATCH_SIZE):
        # TODO: Q: Why not use Polars here?
        #       A: Its `write_iceberg` method is currently very slim, as of version 1.38.
        #       https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_iceberg.html
        chunk.to_iceberg(
            table_identifier=iceberg_identifier,
            catalog_name=iceberg_address.catalog,
            catalog_properties=catalog_properties,
            append=True,
        )
    return True
