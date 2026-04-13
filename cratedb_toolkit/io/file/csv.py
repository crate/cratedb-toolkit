"""
CSV file integration for CrateDB Toolkit.

This module provides functionality to transfer data between CSV files
and CrateDB database tables, supporting both import and export operations.
"""

import dataclasses
import logging
from typing import Dict, List, Optional

import polars as pl
from boltons.urlutils import URL

from cratedb_toolkit.io.util import parse_uri, polars_to_cratedb

logger = logging.getLogger(__name__)


DEFAULT_SEPARATOR = ","
DEFAULT_QUOTE_CHAR = '"'
DEFAULT_BATCH_SIZE = 75_000


@dataclasses.dataclass
class CsvFileAddress:
    """
    Represent a CSV file location and provide loader methods.
    """

    url: URL
    location: str
    pipeline: Optional[List[str]] = dataclasses.field(default_factory=list)
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE
    # TODO: What about other parameters? See `polars.io.csv.functions`.
    separator: Optional[str] = DEFAULT_SEPARATOR
    quote_char: Optional[str] = DEFAULT_QUOTE_CHAR

    @classmethod
    def from_url(cls, url: str) -> "CsvFileAddress":
        """
        Parse a CSV file location and return a CsvFileAddress object.

        Examples:

        csv://./var/lib/example.csv
        https://guided-path.s3.us-east-1.amazonaws.com/demo_climate_data_export.csv
        """
        url_obj, location = parse_uri(url, "csv")
        return cls(
            url=url_obj,
            location=location,
            pipeline=url_obj.query_params.getlist("pipe"),
            batch_size=int(url_obj.query_params.get("batch-size", DEFAULT_BATCH_SIZE)),
            separator=url_obj.query_params.get("separator", DEFAULT_SEPARATOR),
            quote_char=url_obj.query_params.get("quote-char", DEFAULT_QUOTE_CHAR),
        )

    @property
    def storage_options(self) -> Dict[str, str]:
        """
        Provide file storage options.

        TODO: Generalize.
        """
        prefixes = ["aws_", "azure_", "google_", "delta_"]
        return self.collect_properties(self.url.query_params, prefixes)

    @staticmethod
    def collect_properties(query_params: Dict, prefixes: List) -> Dict[str, str]:
        """
        Collect parameters from URL query string.

        TODO: Generalize.
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
        Load the CSV file as a Polars LazyFrame.
        """

        # Read from data source.
        lf = pl.scan_csv(
            self.location,
            separator=self.separator,
            quote_char=self.quote_char,
            storage_options=self.storage_options,
        )

        # Optionally apply transformations.
        if self.pipeline:
            from macropipe import MacroPipe

            mp = MacroPipe.from_recipes(*self.pipeline)
            lf = mp.apply(lf)

        return lf


def from_csv(source_url, target_url, progress: bool = False) -> bool:
    """
    Scan a CSV file from local filesystem or object store, and load into CrateDB.
    Documentation: https://cratedb-toolkit.readthedocs.io/io/file/csv.html

    See also: https://docs.pola.rs/api/python/stable/reference/api/polars.scan_csv.html

    # Synopsis: Load from filesystem.
    ctk load \
        "csv://./var/lib/example.csv" \
        "crate://crate@localhost:4200/demo/example"
    """
    source = CsvFileAddress.from_url(source_url)
    logger.info(f"File address: {source.location}")
    return polars_to_cratedb(
        frame=source.load_table(),
        target_url=target_url,
        chunk_size=source.batch_size,
    )
