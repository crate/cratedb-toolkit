import typing as t
from copy import copy

import pandas as pd
import polars as pl
import sqlalchemy as sa
from boltons.urlutils import URL
from sqlalchemy_cratedb import insert_bulk

from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util.database import logger


def parse_uri(url: str, kind: str) -> t.Tuple[URL, str]:
    """
    Parse a typical URI from CTK's I/O machinery flavor.

    file+iceberg://./var/lib/iceberg/demo/taxi-tiny/metadata/00003-dd9223cb
    s3+iceberg://bucket1/demo/taxi-tiny/metadata/00003-dd9223cb
    s3+iceberg://bucket1/?catalog-uri=http://iceberg-catalog.example.org:5000&catalog-token=foo

    file+deltalake://./var/lib/delta/demo/taxi-tiny
    s3+deltalake://bucket1/demo/taxi-tiny
    """
    url_obj = URL(url)
    if url_obj.scheme.endswith(f"+{kind}"):
        url_obj.scheme = url_obj.scheme.replace(f"+{kind}", "")
    if url_obj.scheme.startswith("file"):
        if url_obj.host == ".":
            url_obj.path = url_obj.path.lstrip("/")
        location = url_obj.path
    else:
        u2 = copy(url_obj)
        u2.query_params.clear()
        location = str(u2)
    return url_obj, location


def polars_to_cratedb(frame: pl.LazyFrame, target_url, chunk_size: int) -> bool:
    """
    Write a Polars LazyFrame to a CrateDB table, in batches/chunks.
    """
    cratedb_address = DatabaseAddress.from_string(target_url)
    cratedb_url, cratedb_table = cratedb_address.decode()
    if_exists = URL(target_url).query_params.get("if-exists") or "fail"
    if cratedb_table.table is None:
        raise ValueError("Table name is missing. Please adjust CrateDB database URL.")
    logger.info("Target address: %s", cratedb_address)

    # Invoke copy operation.
    logger.info(f"Running copy with chunk size={chunk_size}")
    engine = sa.create_engine(str(cratedb_url))

    # Note: The conversion to pandas is zero-copy,
    #       so we can utilize their SQL utils for free.
    #       https://github.com/pola-rs/polars/issues/7852
    # Note: This code also uses the most efficient `insert_bulk` method with CrateDB.
    #       https://cratedb.com/docs/sqlalchemy-cratedb/dataframe.html#efficient-insert-operations-with-pandas
    # Note: `collect_batches()` is marked as unstable and slower than native sinks;
    #       consider native Polars sinks (e.g., write_database) as a faster alternative if available.
    #       https://github.com/crate/cratedb-toolkit/pull/444#discussion_r2825382887
    # Note: This variant appeared to be much slower, let's revisit and investigate why?
    #       table.to_polars().collect(streaming=True).write_database(
    #         table_name=cratedb_table.fullname, connection=engine, if_table_exists="replace"  # noqa: ERA001
    # Note: When `collect_batches` yields more than one batch, the first batch must use the
    #       user-specified `if_exists`, but subsequent batches must use "append".
    with pl.Config(streaming_chunk_size=chunk_size):
        for i, batch in enumerate(frame.collect_batches(engine="streaming", chunk_size=chunk_size)):
            batch.to_pandas().to_sql(
                name=cratedb_table.table,
                schema=cratedb_table.schema,
                con=engine,
                if_exists=if_exists if i == 0 else "append",
                index=False,
                chunksize=chunk_size,
                method=insert_bulk,
            )

    engine.dispose()
    return True


def pandas_from_cratedb(
    source_url: str, schema: str, table: str, chunk_size: int
) -> t.Generator[pd.DataFrame, None, None]:
    """
    Read from a CrateDB table into pandas data frames, yielding batches/chunks.
    """
    engine = sa.create_engine(source_url)
    with engine.connect() as connection:
        for chunk in pd.read_sql_table(
            table_name=table,
            schema=schema,
            con=connection,
            chunksize=chunk_size,
        ):
            yield chunk


def read_cratedb(url: str, default_batch_size: int) -> t.Generator[pd.DataFrame, None, None]:
    """
    Read data from a CrateDB table defined by URL into pandas data frames, yielding batches/chunks.
    """
    cratedb_address = DatabaseAddress.from_string(url)
    cratedb_url, cratedb_table = cratedb_address.decode()
    if cratedb_table.table is None:
        raise ValueError("Table name is missing. Please adjust CrateDB database URL.")
    batch_size = int(URL(url).query_params.get("batch-size", default_batch_size))
    logger.info(f"Source: Address={cratedb_address}, Batch Size={batch_size}")

    for chunk in pandas_from_cratedb(
        source_url=str(cratedb_url),
        schema=cratedb_table.schema or "doc",
        table=cratedb_table.table,
        chunk_size=batch_size,
    ):
        yield chunk
