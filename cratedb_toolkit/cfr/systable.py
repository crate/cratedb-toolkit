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
import json
import logging
import os
import tarfile
import tempfile
import typing as t
from pathlib import Path

import orjsonl
from sqlalchemy_cratedb import insert_bulk
from tqdm.contrib.logging import logging_redirect_tqdm

if t.TYPE_CHECKING:
    import polars as pl

import sqlalchemy as sa
from tqdm import tqdm

from cratedb_toolkit.info.core import InfoContainer
from cratedb_toolkit.util.cli import error_logger
from cratedb_toolkit.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


DataFormat = t.Literal["csv", "jsonl", "ndjson", "parquet"]


class SystemTableKnowledge:
    """
    Manage a few bits of knowledge about CrateDB internals.
    """

    # Name of CrateDB's schema for system tables.
    SYS_SCHEMA = "sys"

    # Name of the SQL standard schema describing the cluster's own relations.
    INFORMATION_SCHEMA = "information_schema"

    # Schemas exported verbatim into the bundle, in bundle order.
    EXPORT_SCHEMAS = (SYS_SCHEMA, INFORMATION_SCHEMA)

    # Schemas that do not belong to the user, hence are not subject to DDL capture.
    NON_USER_SCHEMAS = (SYS_SCHEMA, INFORMATION_SCHEMA, "pg_catalog", "blob")

    # `information_schema.tables.table_type` value identifying a regular table.
    # `SHOW CREATE TABLE` only works on those, not on views or system relations.
    BASE_TABLE_TYPE = "BASE TABLE"

    # Columns CrateDB hands out in the clear that must never reach a bundle.
    REDACTED_COLUMNS: t.Dict[t.Tuple[str, str], t.Tuple[str, ...]] = {
        (INFORMATION_SCHEMA, "user_mapping_options"): ("option_value",),
        (INFORMATION_SCHEMA, "foreign_server_options"): ("option_value",),
    }

    # What a redacted value is replaced with.
    REDACTION_MARKER = "[redacted by cratedb-toolkit]"

    # Tables whose data is deliberately not collected
    DATA_SKIPLIST: t.Dict[t.Tuple[str, str], str] = {
        (SYS_SCHEMA, "summits"): ("static dataset shipped with the server"),
    }


class ExportSettings:
    """
    Manage a few bits of knowledge about how to export system tables from CrateDB.
    """

    # Subdirectories where to store schema vs. data information.
    SCHEMA_PATH = "schema"
    DATA_PATH = "data"

    # The filename prefix when storing tables to disk.
    TABLE_FILENAME_PREFIX = "sys-"

    # Per-schema filename prefixes. `sys` keeps its historical prefix.
    FILENAME_PREFIXES = {
        SystemTableKnowledge.SYS_SCHEMA: TABLE_FILENAME_PREFIX,
        SystemTableKnowledge.INFORMATION_SCHEMA: "is-",
    }

    # Where the user's own relation definitions are stored.
    DDL_PATH = "ddl"
    DDL_TABLES_PATH = "tables"
    DDL_VIEWS_PATH = "views"

    # The bundle's self-description.
    MANIFEST_FILENAME = "manifest.json"


class SystemTableInspector:
    """
    Reflect schema information from CrateDB system tables.
    """

    def __init__(self, dburi: str):
        self.dburi = dburi
        self.adapter = DatabaseAdapter(dburi=self.dburi)
        self.engine = self.adapter.engine
        self.inspector = sa.inspect(self.engine)

    def table_names(self, schema: t.Optional[str] = None):
        return self.inspector.get_table_names(schema=schema or SystemTableKnowledge.SYS_SCHEMA)

    def ddl(
        self,
        tablename_in: str,
        tablename_out: str,
        in_schema: t.Optional[str] = None,
        out_schema: t.Optional[str] = None,
        with_drop_table: bool = False,
    ) -> str:
        meta = sa.MetaData(schema=in_schema or SystemTableKnowledge.SYS_SCHEMA)
        table = sa.Table(tablename_in, meta, autoload_with=self.engine)
        table.schema = out_schema
        table.name = tablename_out
        sql = ""
        if with_drop_table:
            sql += sa.schema.DropTable(table, if_exists=True).compile(self.engine).string.strip() + ";\n"
        sql += sa.schema.CreateTable(table, if_not_exists=True).compile(self.engine).string.strip() + ";\n"
        return sql


class SchemaCapture:
    """
    Capture the user's own relation definitions.

    Table definitions come from the cluster's own `SHOW CREATE TABLE`, rather
    than being derived by hand from metadata tables: that renders every clause
    support needs (nested objects, arrays, geo and vector types, generated
    columns, fulltext indexes, sharding, partitioning, table settings).

    """

    NON_USER_SCHEMAS_SQL = ", ".join(f"'{name}'" for name in SystemTableKnowledge.NON_USER_SCHEMAS)

    def __init__(self, adapter: DatabaseAdapter):
        self.adapter = adapter

    def relations(self) -> t.List[t.Dict[str, str]]:
        """
        Discover the user's relations, i.e. everything outside the system schemas.
        """
        sql = f"""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ({self.NON_USER_SCHEMAS_SQL})
            ORDER BY table_schema, table_name
        """  # noqa: S608
        return self.adapter.run_sql(sql, records=True) or []

    def table_ddl(self, schema: str, table: str) -> str:
        """
        Ask the cluster for a table's own definition.
        """
        relation = self.adapter.quote_relation_name(f"{schema}.{table}")
        records = self.adapter.run_sql(f"SHOW CREATE TABLE {relation}", records=True)
        if not records:
            raise ValueError(f"No definition returned for {schema}.{table}")
        return str(list(records[0].values())[0])

    def views(self) -> t.List[t.Dict[str, str]]:
        """
        Read view definitions, which `SHOW CREATE TABLE` cannot provide.
        """
        sql = f"""
            SELECT table_schema, table_name, view_definition
            FROM information_schema.views
            WHERE table_schema NOT IN ({self.NON_USER_SCHEMAS_SQL})
            ORDER BY table_schema, table_name
        """  # noqa: S608
        return self.adapter.run_sql(sql, records=True) or []


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

    def make_tarfile(self, source_path: t.Optional[Path] = None, arcname: t.Optional[str] = None) -> Path:
        """
        Archive `source_path` under a single top-level entry named `arcname`.
        """
        source_path = source_path or self.path_provider.path
        with tarfile.open(self.target_path, "x:gz") as tar:
            tar.add(source_path.absolute(), arcname=arcname or os.path.basename(source_path))
        return self.target_path


class SystemTableExporter(PathProvider):
    """
    Export schema and data from CrateDB system tables.
    """

    def __init__(
        self,
        dburi: str,
        target: t.Union[Path],
        data_format: DataFormat = "jsonl",
    ):
        super().__init__(target)
        self.dburi = dburi
        self.data_format = data_format
        self.adapter = DatabaseAdapter(dburi=self.dburi)
        self.info = InfoContainer(adapter=self.adapter)
        self.inspector = SystemTableInspector(dburi=self.dburi)
        self.schema_capture = SchemaCapture(adapter=self.adapter)
        self.schema_failures: t.List[t.Dict[str, str]] = []
        self.data_failures: t.List[t.Dict[str, str]] = []
        self.definition_failures: t.List[t.Dict[str, str]] = []
        self.data_skipped: t.List[t.Dict[str, str]] = []
        self.table_count = 0
        self.data_file_count = 0

    def cratedb_version(self) -> str:
        """
        Read the cluster's version from the cluster. Never assume it.
        """
        try:
            records = self.adapter.run_sql("SELECT version['number'] AS version FROM sys.nodes LIMIT 1", records=True)
            if records:
                return str(list(records[0].values())[0])
        except Exception as ex:
            logger.warning(f"Could not determine CrateDB version: {ex}")
        return "unknown"

    def read_table(self, tablename: str, schema: t.Optional[str] = None) -> "pl.DataFrame":
        import polars as pl

        schema = schema or SystemTableKnowledge.SYS_SCHEMA
        sql = f'SELECT * FROM "{schema}"."{tablename}"'  # noqa: S608
        logger.debug(f"Running SQL: {sql}")
        return pl.read_database(
            query=sql,  # noqa: S608
            connection=self.adapter.connection,
            infer_schema_length=100_000,
        )

    def redact(self, frame: "pl.DataFrame", schema: str, tablename: str) -> "pl.DataFrame":
        """
        Blank out values CrateDB returns in the clear, but a bundle must not carry.

        """
        import polars as pl

        columns = SystemTableKnowledge.REDACTED_COLUMNS.get((schema, tablename))
        if not columns:
            return frame
        marker = SystemTableKnowledge.REDACTION_MARKER
        replacements = [
            pl.when(pl.col(column).is_null()).then(None).otherwise(pl.lit(marker)).alias(column)
            for column in columns
            if column in frame.columns
        ]
        return frame.with_columns(replacements) if replacements else frame

    def dump_table(self, frame: "pl.DataFrame", file: t.Union[t.TextIO, None] = None):
        if self.data_format == "csv":
            # polars.exceptions.ComputeError: CSV format does not support nested data
            # return df.write_csv()  # noqa: ERA001
            return frame.to_pandas().to_csv(file)
        elif self.data_format in ["jsonl", "ndjson"]:
            return frame.write_ndjson(file and file.buffer)
        elif self.data_format in ["parquet", "pq"]:
            return frame.write_parquet(file and file.buffer)  # ty: ignore[invalid-argument-type]
        else:
            raise NotImplementedError(f"Output format not implemented: {self.data_format}")

    def save(self) -> Path:
        import cratedb_toolkit

        self.path.mkdir(exist_ok=True, parents=True)
        now = dt.datetime.now().astimezone()
        timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
        # The bundle root. `sys-import` consumes the per-schema subdirectories
        bundle_path = self.path / self.info.cluster_name / timestamp
        logger.info(f"Exporting system tables to: {bundle_path}")

        with logging_redirect_tqdm():
            for schema in SystemTableKnowledge.EXPORT_SCHEMAS:
                self._save_schema(bundle_path, schema)
            definitions = self._save_definitions(bundle_path)
            self._write_manifest(
                bundle_path,
                cratedb_version=self.cratedb_version(),
                toolkit_version=cratedb_toolkit.__version__,
                collected_at=now.isoformat(timespec="seconds"),
                definitions=definitions,
            )

        schemas = ", ".join(SystemTableKnowledge.EXPORT_SCHEMAS)
        logger.info(
            f"Successfully exported {self.table_count} tables from {schemas} "
            f"({self.data_file_count} with data, {len(self.data_skipped)} skipped, "
            f"{len(self.schema_failures)} schema and {len(self.data_failures)} data failures, "
            f"{len(self.definition_failures)} definition failures)"
        )
        return bundle_path

    def _write_manifest(
        self,
        bundle_path: Path,
        cratedb_version: str,
        toolkit_version: str,
        collected_at: str,
        definitions: t.Dict[str, int],
    ) -> Path:
        """
        Describe the bundle, so a recipient knows what they are looking at.
        """
        manifest = {
            "cluster_name": self.info.cluster_name,
            "cratedb_version": cratedb_version,
            "toolkit_version": toolkit_version,
            "collected_at": collected_at,
            "schemas_exported": list(SystemTableKnowledge.EXPORT_SCHEMAS),
            "tables_exported": self.table_count,
            "data_files_written": self.data_file_count,
            "definitions_captured": definitions,
            "schema_failures": self.schema_failures,
            "data_failures": self.data_failures,
            "definition_failures": self.definition_failures,
            "data_skipped": self.data_skipped,
            "redactions": [
                {"schema": schema, "table": table, "columns": list(columns)}
                for (schema, table), columns in SystemTableKnowledge.REDACTED_COLUMNS.items()
            ],
        }
        target = bundle_path / ExportSettings.MANIFEST_FILENAME
        target.write_text(json.dumps(manifest, indent=2) + "\n")
        return target

    def _save_schema(self, bundle_path: Path, schema: str) -> None:
        """
        Export every table of one schema.
        """
        base = bundle_path / schema
        path_schema = base / ExportSettings.SCHEMA_PATH
        path_data = base / ExportSettings.DATA_PATH
        path_schema.mkdir(parents=True, exist_ok=True)
        path_data.mkdir(parents=True, exist_ok=True)
        prefix = ExportSettings.FILENAME_PREFIXES[schema]

        try:
            tablenames = self.inspector.table_names(schema=schema)
        except Exception as ex:
            logger.warning(f"Could not list tables of schema `{schema}`: {ex}")
            self.data_failures.append({"schema": schema, "table": "*", "reason": f"{type(ex).__name__}: {ex}"})
            return

        for tablename in tqdm(tablenames, desc=f"Exporting {schema}", disable=None):
            logger.debug(f"Exporting table: {schema}.{tablename}")
            self._save_table(
                schema=schema,
                tablename=tablename,
                path_schema=path_schema,
                path_data=path_data,
                prefix=prefix,
            )

    def _save_table(self, schema: str, tablename: str, path_schema: Path, path_data: Path, prefix: str) -> None:
        """
        Export one table's schema and data *independently*.
        """
        tablename_out = f"{prefix}{tablename}"
        path_table_schema = path_schema / f"{tablename_out}.sql"
        path_table_data = path_data / f"{tablename_out}.{self.data_format}"
        self.table_count += 1

        # Schema. Not every CrateDB column type can be represented in SQLAlchemy
        # DDL, so reflection can fail per table.
        try:
            ddl = self.inspector.ddl(tablename_in=tablename, tablename_out=tablename_out, in_schema=schema)
            with open(path_table_schema, "w") as fh_schema:
                print(ddl, file=fh_schema)
        except Exception as ex:
            logger.warning(f"Could not generate schema for {schema}.{tablename}: {ex}")
            self.schema_failures.append({"schema": schema, "table": tablename, "reason": f"{type(ex).__name__}: {ex}"})

        skip_reason = SystemTableKnowledge.DATA_SKIPLIST.get((schema, tablename))
        if skip_reason is not None:
            logger.debug(f"Not collecting data of {schema}.{tablename}: {skip_reason}")
            self.data_skipped.append({"schema": schema, "table": tablename, "reason": skip_reason})
            return

        try:
            frame = self.redact(self.read_table(tablename=tablename, schema=schema), schema, tablename)
            if frame.is_empty():
                return
            mode = "wb" if self.data_format in ["parquet", "pq"] else "w"
            with open(path_table_data, mode) as fh_data:
                self.dump_table(frame=frame, file=t.cast(t.TextIO, fh_data))
            self.data_file_count += 1
        except Exception as ex:
            logger.warning(f"Could not export data of {schema}.{tablename}: {ex}")
            self.data_failures.append({"schema": schema, "table": tablename, "reason": f"{type(ex).__name__}: {ex}"})

    def _save_definitions(self, bundle_path: Path) -> t.Dict[str, int]:
        """
        Capture the user's own table and view definitions.
        """
        path_tables = bundle_path / ExportSettings.DDL_PATH / ExportSettings.DDL_TABLES_PATH
        path_views = bundle_path / ExportSettings.DDL_PATH / ExportSettings.DDL_VIEWS_PATH
        path_tables.mkdir(parents=True, exist_ok=True)
        path_views.mkdir(parents=True, exist_ok=True)
        counts = {"tables": 0, "views": 0}

        try:
            relations = self.schema_capture.relations()
        except Exception as ex:
            logger.warning(f"Could not discover user relations: {ex}")
            self.definition_failures.append({"kind": "relations", "reason": f"{type(ex).__name__}: {ex}"})
            return counts

        for relation in tqdm(relations, desc="Capturing definitions", disable=None):
            schema = relation["table_schema"]
            name = relation["table_name"]
            # `SHOW CREATE TABLE` only works on regular tables. Views come from
            # `information_schema.views` below; anything else is left alone.
            if relation.get("table_type") != SystemTableKnowledge.BASE_TABLE_TYPE:
                continue
            try:
                ddl = self.schema_capture.table_ddl(schema=schema, table=name)
                (path_tables / f"{schema}.{name}.sql").write_text(ddl.rstrip() + "\n")
                counts["tables"] += 1
            except Exception as ex:
                logger.warning(f"Could not capture definition of {schema}.{name}: {ex}")
                self.definition_failures.append(
                    {"kind": "table", "schema": schema, "name": name, "reason": f"{type(ex).__name__}: {ex}"}
                )

        try:
            views = self.schema_capture.views()
        except Exception as ex:
            logger.warning(f"Could not read view definitions: {ex}")
            self.definition_failures.append({"kind": "views", "reason": f"{type(ex).__name__}: {ex}"})
            return counts

        for view in views:
            schema = view["table_schema"]
            name = view["table_name"]
            definition = view.get("view_definition")
            if not definition:
                continue
            try:
                relation = self.adapter.quote_relation_name(f"{schema}.{name}")
                statement = f"CREATE OR REPLACE VIEW {relation} AS\n{definition.rstrip()};\n"
                (path_views / f"{schema}.{name}.sql").write_text(statement)
                counts["views"] += 1
            except Exception as ex:
                logger.warning(f"Could not capture view {schema}.{name}: {ex}")
                self.definition_failures.append(
                    {"kind": "view", "schema": schema, "name": name, "reason": f"{type(ex).__name__}: {ex}"}
                )

        return counts


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

    def table_names(self) -> t.List[str]:
        """
        The target table names, as written by the exporter.

        Filenames already carry their schema's prefix (`sys-`, `is-`), and that
        prefix is part of the restored table's name.
        """
        path_schema = self.source / ExportSettings.SCHEMA_PATH
        return sorted(item.stem for item in path_schema.glob("*.sql"))

    def load(self):
        path_schema = self.source / ExportSettings.SCHEMA_PATH
        path_data = self.source / ExportSettings.DATA_PATH

        if not path_schema.exists():
            raise FileNotFoundError(f"Path does not exist: {path_schema}")

        logger.info(f"Importing system tables from: {self.source}")

        with logging_redirect_tqdm():
            self._load(path_schema, path_data)

    def _load(self, path_schema: Path, path_data: Path):
        import pandas as pd

        table_count = 0
        for tablename in tqdm(self.table_names()):
            path_table_schema = path_schema / f"{tablename}.sql"
            path_table_data = path_data / f"{tablename}.{self.data_format}"

            # Skip import of non-existing or empty files.
            if not path_table_data.exists() or path_table_data.stat().st_size == 0:
                continue

            table_count += 1

            # Invoke SQL DDL.
            schema_sql = path_table_schema.read_text()
            self.adapter.run_sql(schema_sql)

            # Truncate table.
            self.adapter.run_sql(f"DELETE FROM {self.adapter.quote_relation_name(tablename)};")  # noqa: S608

            # Load data.
            try:
                df: "pd.DataFrame" = pd.DataFrame.from_records(self.load_table(path_table_data))
                df.to_sql(
                    name=tablename,
                    con=self.adapter.engine,
                    index=False,
                    if_exists="append",
                    method=insert_bulk,
                )
            except Exception as ex:
                error_logger(self.debug)(f"Importing table failed: {tablename}. Reason: {ex}")

        logger.info(f"Successfully imported {table_count} system tables")

    def load_table(self, path: Path) -> t.List:
        import polars as pl

        if path.suffix in [".jsonl"]:
            return orjsonl.load(path)
        elif path.suffix in [".parquet", ".pq"]:
            return pl.read_parquet(path).to_pandas().to_dict("records")
        else:
            raise NotImplementedError(f"Input format not implemented: {path.suffix}")
