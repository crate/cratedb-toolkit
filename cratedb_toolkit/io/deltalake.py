"""
Delta Lake integration for CrateDB Toolkit.

This module provides functionality to transfer data between DeltaLake tables
and CrateDB databases, supporting both import and export operations.

https://delta.io/
https://delta-io.github.io/delta-rs/
https://github.com/delta-io/delta-rs
"""

import dataclasses
import logging
from typing import Dict, List, Optional, Union

import polars as pl
from boltons.urlutils import URL

from cratedb_toolkit.io.util import parse_uri, polars_to_cratedb, read_cratedb

logger = logging.getLogger(__name__)


DEFAULT_BATCH_SIZE = 75_000


@dataclasses.dataclass
class DeltaLakeAddress:
    """
    Represent a DeltaLake location and provide loader methods.
    """

    url: URL
    location: str
    catalog: Optional[str] = None
    database: Optional[str] = None
    table: Optional[str] = None
    version: Optional[Union[int, str]] = None
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE

    @classmethod
    def from_url(cls, url: str):
        """
        Parse a DeltaLake location and return a DeltaLakeAddress object.

        Examples:

        file+deltalake://./var/lib/delta/demo/taxi-tiny
        s3+deltalake://bucket1/demo/taxi-tiny
        """
        url_obj, location = parse_uri(url, "deltalake")
        version = url_obj.query_params.get("version")
        if version is not None and version.isdigit():
            version = int(version)
        return cls(
            url=url_obj,
            location=location,
            catalog=url_obj.query_params.get("catalog"),
            database=url_obj.query_params.get("database"),
            table=url_obj.query_params.get("table"),
            version=version,
            batch_size=int(url_obj.query_params.get("batch-size", DEFAULT_BATCH_SIZE)),
        )

    @property
    def storage_options(self):
        """
        Provide DeltaLake storage options.

        https://delta-io.github.io/delta-rs/usage/loading-table/
        https://delta-io.github.io/delta-rs/integrations/object-storage/lakefs/#example
        """
        prefixes = ["aws_", "azure_", "google_", "delta_", "endpoint", "access_key_id", "secret_access_key"]
        return self.collect_properties(self.url.query_params, prefixes)

    @staticmethod
    def collect_properties(query_params: Dict, prefixes: List) -> Dict[str, str]:
        """
        Collect parameters from URL query string.
        """
        opts = {}
        for name, value in query_params.items():
            for prefix in prefixes:
                if name.lower().startswith(prefix) and value is not None:
                    opts[name.upper()] = value
                    break
        return opts

    def load_table(self) -> pl.LazyFrame:
        """
        Load the DeltaLake table as a Polars LazyFrame.
        """
        return pl.scan_delta(self.location, version=self.version, storage_options=self.storage_options)


def from_deltalake(source_url, target_url, progress: bool = False):
    """
    Scan a DeltaLake table from local filesystem or object store, and load into CrateDB.
    Documentation: https://cratedb-toolkit.readthedocs.io/io/deltalake/#load

    See also: https://docs.pola.rs/api/python/stable/reference/api/polars.scan_delta.html

    # Synopsis: Load from filesystem.
    ctk load table \
        "file+deltalake://./var/lib/delta" \
        --cluster-url="crate://crate@localhost:4200/demo/taxi"
    """
    source = DeltaLakeAddress.from_url(source_url)
    logger.info(f"DeltaLake address: {source.location}")
    return polars_to_cratedb(
        frame=source.load_table(),
        target_url=target_url,
        chunk_size=source.batch_size,
    )


def to_deltalake(source_url, target_url, progress: bool = False):
    """
    Export a table from CrateDB into a DeltaLake table.
    Documentation: https://cratedb-toolkit.readthedocs.io/io/deltalake/#save

    See also: https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_delta.html

    # Synopsis: Save to filesystem.
    ctk save table \
        --cluster-url="crate://crate@localhost:4200/demo/taxi" \
        "file+deltalake://./var/lib/delta"
    """

    # Decode DeltaLake target address.
    deltalake_address = DeltaLakeAddress.from_url(target_url)
    deltalake_mode = deltalake_address.url.query_params.get("mode", "error")
    logger.info(f"DeltaLake address: {deltalake_address.location}")

    # Invoke copy operation.
    for i, chunk in enumerate(read_cratedb(source_url, default_batch_size=DEFAULT_BATCH_SIZE)):
        pl.from_pandas(chunk).write_delta(
            target=deltalake_address.location,
            mode=deltalake_mode if i == 0 else "append",
            storage_options=deltalake_address.storage_options,
        )
    return True
