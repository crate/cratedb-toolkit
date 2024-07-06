"""
CrateDB Diagnostics: System Tables Exporter and Importer.

Schemas and results of following queries should be included:
```sql
SELECT * FROM sys.cluster
SELECT * FROM sys.nodes
SELECT * FROM sys.shards
SELECT * FROM sys.allocations
SELECT * FROM sys.jobs_log
SELECT * FROM sys.operations_log
```

https://cratedb.com/docs/python/en/latest/by-example/sqlalchemy/inspection-reflection.html
https://docs.sqlalchemy.org/en/20/faq/metadata_schema.html#how-can-i-get-the-create-table-drop-table-output-as-a-string
"""

import datetime as dt
import logging
import os
import tarfile
import tempfile
import typing as t
from pathlib import Path

import polars as pl
import sqlalchemy as sa
from tqdm import tqdm

from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.util.cli import error_logger
from cratedb_toolkit.util.sqlalchemy import patch_encoder
from cratedb_toolkit.wtf.core import InfoContainer

logger = logging.getLogger(__name__)


DataFormat = t.Literal["csv", "jsonl", "ndjson", "parquet"]


class SystemTableKnowledge:
    """
    Manage a few bits of knowledge about CrateDB internals.
    """

    # Name of CrateDB's schema for system tables.
    SYS_SCHEMA = "sys"

    # TODO: Reflecting the `summits` table raises an error.
    # AttributeError: 'UserDefinedType' object has no attribute 'get_col_spec'
    REFLECTION_BLOCKLIST = ["summits"]


class ExportSettings:
    """
    Manage a few bits of knowledge about how to export system tables from CrateDB.
    """

    # Subdirectories where to store schema vs. data information.
    SCHEMA_PATH = "schema"
    DATA_PATH = "data"

    # The filename prefix when storing tables to disk.
    TABLE_FILENAME_PREFIX = "sys-"


class SystemTableInspector:
    """
    Reflect schema information from CrateDB system tables.
    """

    def __init__(self, dburi: str):
        self.dburi = dburi
        self.adapter = DatabaseAdapter(dburi=self.dburi)
        self.engine = self.adapter.engine
        self.inspector = sa.inspect(self.engine)

    def table_names(self):
        return self.inspector.get_table_names(schema=SystemTableKnowledge.SYS_SCHEMA)

    def ddl(self, tablename_in: str, tablename_out: str, out_schema: str = None, with_drop_table: bool = False) -> str:
        meta = sa.MetaData(schema=SystemTableKnowledge.SYS_SCHEMA)
        table = sa.Table(tablename_in, meta, autoload_with=self.engine)
        table.schema = out_schema
        table.name = tablename_out
        sql = ""
        if with_drop_table:
            sql += sa.schema.DropTable(table, if_exists=True).compile(self.engine).string.strip() + ";\n"
        sql += sa.schema.CreateTable(table, if_not_exists=True).compile(self.engine).string.strip() + ";\n"
        return sql


class PathProvider:
    def __init__(self, path: t.Union[Path]):
        self.path = path


class Archive:
    def __init__(self, path_provider: PathProvider):
        self.path_provider = path_provider
        self.temp_dir = tempfile.TemporaryDirectory()
        self.target_path = self.path_provider.path
        self.path_provider.path = Path(self.temp_dir.name)

    def close(self):
        self.temp_dir.cleanup()

    def make_tarfile(self) -> Path:
        source_path = self.path_provider.path
        with tarfile.open(self.target_path, "x:gz") as tar:
            tar.add(source_path.absolute(), arcname=os.path.basename(source_path))
        return self.target_path


class SystemTableExporter(PathProvider):
    """
    Export schema and data from CrateDB system tables.
    """

    def __init__(self, dburi: str, target: t.Union[Path], data_format: DataFormat = "jsonl"):
        super().__init__(target)
        self.dburi = dburi
        self.data_format = data_format
        self.adapter = DatabaseAdapter(dburi=self.dburi)
        self.info = InfoContainer(adapter=self.adapter)
        self.inspector = SystemTableInspector(dburi=self.dburi)

    def read_table(self, tablename: str) -> pl.DataFrame:
        sql = f'SELECT * FROM "{SystemTableKnowledge.SYS_SCHEMA}"."{tablename}"'  # noqa: S608
        logger.debug(f"Running SQL: {sql}")
        return pl.read_database(
            query=sql,  # noqa: S608
            connection=self.adapter.engine,
            infer_schema_length=1000,
        )

    def dump_table(self, frame: pl.DataFrame, file: t.Union[t.TextIO, None] = None):
        if self.data_format == "csv":
            # polars.exceptions.ComputeError: CSV format does not support nested data
            # return df.write_csv()  # noqa: ERA001
            return frame.to_pandas().to_csv(file)
        elif self.data_format in ["jsonl", "ndjson"]:
            return frame.write_ndjson(file and file.buffer)  # type: ignore[arg-type]
        elif self.data_format in ["parquet", "pq"]:
            return frame.write_parquet(file and file.buffer)  # type: ignore[arg-type]
        else:
            raise NotImplementedError(f"Output format not implemented: {self.data_format}")

    def save(self) -> Path:
        self.path.mkdir(exist_ok=True, parents=True)
        timestamp = dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        path = self.path / self.info.cluster_name / timestamp / "sys"
        logger.info(f"Exporting system tables to: {path}")
        system_tables = self.inspector.table_names()
        path_schema = path / ExportSettings.SCHEMA_PATH
        path_data = path / ExportSettings.DATA_PATH
        path_schema.mkdir(parents=True, exist_ok=True)
        path_data.mkdir(parents=True, exist_ok=True)
        table_count = 0
        for tablename in tqdm(system_tables, disable=None):
            logger.debug(f"Exporting table: {tablename}")
            if tablename in SystemTableKnowledge.REFLECTION_BLOCKLIST:
                continue

            path_table_schema = path_schema / f"{ExportSettings.TABLE_FILENAME_PREFIX}{tablename}.sql"
            path_table_data = path_data / f"{ExportSettings.TABLE_FILENAME_PREFIX}{tablename}.{self.data_format}"
            tablename_out = self.adapter.quote_relation_name(f"{ExportSettings.TABLE_FILENAME_PREFIX}{tablename}")

            # Write schema file.
            with open(path_table_schema, "w") as fh_schema:
                print(self.inspector.ddl(tablename_in=tablename, tablename_out=tablename_out), file=fh_schema)

            # Write data file.
            df = self.read_table(tablename=tablename)
            if df.is_empty():
                continue

            table_count += 1

            mode = "w"
            if self.data_format in ["parquet", "pq"]:
                mode = "wb"
            with open(path_table_data, mode) as fh_data:
                self.dump_table(frame=df, file=t.cast(t.TextIO, fh_data))

        logger.info(f"Successfully exported {table_count} system tables")
        return path


class SystemTableImporter:
    """
    Import schema and data about CrateDB system tables.
    """

    def __init__(self, dburi: str, source: Path, data_format: DataFormat = "jsonl", debug: bool = False):
        self.dburi = dburi
        self.source = source
        self.data_format = data_format
        self.debug = debug
        self.adapter = DatabaseAdapter(dburi=self.dburi)

    def table_names(self):
        path_schema = self.source / ExportSettings.SCHEMA_PATH
        names = []
        for item in path_schema.glob("*.sql"):
            name = item.name.replace(ExportSettings.TABLE_FILENAME_PREFIX, "").replace(".sql", "")
            names.append(name)
        return names

    def load(self):
        path_schema = self.source / ExportSettings.SCHEMA_PATH
        path_data = self.source / ExportSettings.DATA_PATH

        if not path_schema.exists():
            raise FileNotFoundError(f"Path does not exist: {path_schema}")

        logger.info(f"Importing system tables from: {self.source}")

        table_count = 0
        for tablename in tqdm(self.table_names()):
            tablename_restored = ExportSettings.TABLE_FILENAME_PREFIX + tablename

            path_table_schema = path_schema / f"{ExportSettings.TABLE_FILENAME_PREFIX}{tablename}.sql"
            path_table_data = path_data / f"{ExportSettings.TABLE_FILENAME_PREFIX}{tablename}.{self.data_format}"

            # Skip import of non-existing or empty files.
            if not path_table_data.exists() or path_table_data.stat().st_size == 0:
                continue

            table_count += 1

            # Invoke SQL DDL.
            schema_sql = path_table_schema.read_text()
            self.adapter.run_sql(schema_sql)

            # Load data.
            try:
                df: pl.DataFrame = self.load_table(path_table_data)
                df.write_database(table_name=tablename_restored, connection=self.dburi, if_table_exists="append")
            except Exception as ex:
                error_logger(self.debug)(f"Importing table failed: {tablename}. Reason: {ex}")

        logger.info(f"Successfully imported {table_count} system tables")
        # df.to_pandas().to_sql(name=tablename, con=self.adapter.engine, if_exists="append", index=False)  # noqa: ERA001, E501

    def load_table(self, path: Path) -> pl.DataFrame:
        if path.suffix in [".jsonl"]:
            return pl.read_ndjson(path)
        elif path.suffix in [".parquet", ".pq"]:
            return pl.read_parquet(path)
        else:
            raise NotImplementedError(f"Input format not implemented: {path.suffix}")


patch_encoder()
