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
import re
import tarfile
import tempfile
import time
import typing as t
from pathlib import Path

import orjsonl
from boltons.strutils import bytes2human
from tqdm.contrib.logging import logging_redirect_tqdm

if t.TYPE_CHECKING:
    import polars as pl

import sqlalchemy as sa
from tqdm import tqdm

from cratedb_toolkit.info.core import InfoContainer
from cratedb_toolkit.model import DatabaseAddress
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

    # Logs each node keeps of its own recent entries. Read in full, they make one node
    # hold every node's log at once.
    LOG_TABLES: t.Tuple[t.Tuple[str, str], ...] = (
        (SYS_SCHEMA, "jobs_log"),
        (SYS_SCHEMA, "operations_log"),
    )

    # Object columns keyed by node attribute, codec and setting names. Those can contain
    # dots, which CrateDB forbids in the name of an indexed sub-column.
    UNINDEXED_OBJECT_COLUMNS: t.Dict[t.Tuple[str, str], t.Tuple[str, ...]] = {
        (SYS_SCHEMA, "nodes"): ("attributes",),
        (SYS_SCHEMA, "segments"): ("attributes",),
        (SYS_SCHEMA, "sessions"): ("settings",),
        (SYS_SCHEMA, "users"): ("session_settings",),
    }

    # Text columns that can outgrow Lucene's maximum term length of 32766 bytes:
    # statements and error messages quote user input, and the cluster state grows with the cluster.
    UNINDEXED_TEXT_COLUMNS: t.Dict[t.Tuple[str, str], t.Tuple[str, ...]] = {
        (SYS_SCHEMA, "cluster"): ("state",),
        (SYS_SCHEMA, "jobs"): ("stmt",),
        (SYS_SCHEMA, "jobs_log"): ("stmt", "error"),
        (SYS_SCHEMA, "operations_log"): ("error",),
        (SYS_SCHEMA, "sessions"): ("last_statement",),
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

    # How many of the most recent entries to read from each log table, and how many
    # of them one response carries.
    LOG_LIMIT = 1000
    LOG_PAGE_SIZE = 1000

    # How long to wait for one response, in seconds.
    READ_TIMEOUT = 120


class UnindexedObject(sa.types.UserDefinedType):
    """
    An object column whose keys are stored but never become sub-columns.
    """

    def get_col_spec(self, **kw):
        return "OBJECT(IGNORED)"


class UnindexedText(sa.types.UserDefinedType):
    """
    A text column without index or column store, so no value is too long to store.
    """

    def get_col_spec(self, **kw):
        return "TEXT INDEX OFF STORAGE WITH (columnstore = false)"


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
        in_schema = in_schema or SystemTableKnowledge.SYS_SCHEMA
        meta = sa.MetaData(schema=in_schema)
        table = sa.Table(tablename_in, meta, autoload_with=self.engine)
        self.unindex_columns(table, schema=in_schema, tablename=tablename_in)
        table.schema = out_schema
        table.name = tablename_out
        sql = ""
        if with_drop_table:
            sql += sa.schema.DropTable(table, if_exists=True).compile(self.engine).string.strip() + ";\n"
        sql += sa.schema.CreateTable(table, if_not_exists=True).compile(self.engine).string.strip() + ";\n"
        return sql

    @staticmethod
    def unindex_columns(table: sa.Table, schema: str, tablename: str) -> None:
        """
        Declare the columns holding values CrateDB cannot index as unindexed.
        """
        objects = SystemTableKnowledge.UNINDEXED_OBJECT_COLUMNS.get((schema, tablename), ())
        texts = SystemTableKnowledge.UNINDEXED_TEXT_COLUMNS.get((schema, tablename), ())
        for column in table.columns:
            if column.name in objects:
                column.type = UnindexedObject()
            elif column.name in texts:
                column.type = UnindexedText()


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
        log_limit: int = ExportSettings.LOG_LIMIT,
    ):
        super().__init__(target)
        self.dburi = self.with_timeout(dburi)
        self.data_format = data_format
        self.log_limit = log_limit
        self.adapter = DatabaseAdapter(dburi=self.dburi)
        self.info = InfoContainer(adapter=self.adapter)
        self.inspector = SystemTableInspector(dburi=self.dburi)
        self.schema_capture = SchemaCapture(adapter=self.adapter)
        self.schema_failures: t.List[t.Dict[str, str]] = []
        self.data_failures: t.List[t.Dict[str, str]] = []
        self.definition_failures: t.List[t.Dict[str, str]] = []
        self.data_skipped: t.List[t.Dict[str, str]] = []
        self.data_partial: t.List[t.Dict[str, t.Any]] = []
        self.table_count = 0
        self.data_file_count = 0

    @staticmethod
    def with_timeout(dburi: str) -> str:
        """
        Give every read a deadline, so a cluster that stops answering ends the export instead
        of blocking it. An address carrying its own timeout keeps it.
        """
        address = DatabaseAddress.from_string(dburi)
        address.uri.query_params.setdefault("timeout", str(ExportSettings.READ_TIMEOUT))
        return address.dburi

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
        """
        Read one system table, logging its row count, in-memory size and duration.
        """
        schema = schema or SystemTableKnowledge.SYS_SCHEMA
        started = time.monotonic()
        if (schema, tablename) in SystemTableKnowledge.LOG_TABLES:
            frame = self.read_log(schema=schema, tablename=tablename)
        else:
            frame = self.query(f'SELECT * FROM "{schema}"."{tablename}"')  # noqa: S608
        duration = time.monotonic() - started
        logger.debug(
            f"Read {schema}.{tablename}: {frame.height} rows, "
            f"~{bytes2human(frame.estimated_size(), ndigits=1)} in memory, {duration:.3f}s"
        )
        return frame

    def query(self, sql: str) -> "pl.DataFrame":
        import polars as pl

        logger.debug(f"Running SQL: {sql}")
        return pl.read_database(
            query=sql,
            connection=self.adapter.connection,
            infer_schema_length=100_000,
        )

    def read_log(self, schema: str, tablename: str) -> "pl.DataFrame":
        """
        Read the most recent entries of a log table, one page at a time.

        A page is delimited by the time its oldest entry ended, so each node applies it to its
        own log. `ORDER BY ended DESC LIMIT` instead makes every node send its own newest rows
        for one node to merge.
        """
        import polars as pl

        relation = f'"{schema}"."{tablename}"'
        frames: t.List["pl.DataFrame"] = []
        collected = 0
        boundary: t.Optional[int] = None
        while collected < self.log_limit:
            size = min(ExportSettings.LOG_PAGE_SIZE, self.log_limit - collected)
            cutoff = self.log_cutoff(relation, size=size, boundary=boundary)
            conditions = [] if boundary is None else [f"ended < {boundary}"]
            if cutoff is not None:
                conditions.append(f"ended >= {cutoff}")
            where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
            try:
                frame = self.query(f"SELECT * FROM {relation}{where}")  # noqa: S608
            except Exception as ex:
                if not frames:
                    raise
                logger.warning(f"Could not read all of {schema}.{tablename}: {ex}")
                self.data_partial.append(
                    {
                        "schema": schema,
                        "table": tablename,
                        "rows": collected,
                        "reason": f"{type(ex).__name__}: {ex}",
                    }
                )
                break
            frames.append(frame)
            collected += frame.height
            # Without a cut-off, the page took whatever the log still held below the boundary.
            if frame.is_empty() or cutoff is None:
                break
            boundary = cutoff
        return pl.concat(frames, how="vertical_relaxed")

    def log_cutoff(self, relation: str, size: int, boundary: t.Optional[int]) -> t.Optional[int]:
        """
        When did the oldest entry of a page of `size` end? `None` when the log holds fewer.
        """
        where = "" if boundary is None else f"WHERE ended < {boundary}"
        sql = f"SELECT ended::bigint AS ended FROM {relation} {where} ORDER BY ended DESC LIMIT 1 OFFSET {size - 1}"  # noqa: S608
        records = self.adapter.run_sql(sql, records=True)
        return records[0]["ended"] if records else None

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
            f"({self.data_file_count} with data, {len(self.data_partial)} partial, {len(self.data_skipped)} skipped, "
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
            "data_partial": self.data_partial,
            "log_limit": self.log_limit,
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

        with tqdm(tablenames, desc=f"Exporting {schema}", disable=None) as progress:
            for tablename in progress:
                progress.set_postfix_str(tablename)
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

        skip_reason = SystemTableKnowledge.DATA_SKIPLIST.get((schema, tablename))
        if skip_reason is not None:
            logger.debug(f"Not collecting {schema}.{tablename}: {skip_reason}")
            self.data_skipped.append({"schema": schema, "table": tablename, "reason": skip_reason})
            return

        # Schema. Not every CrateDB column type can be represented in SQLAlchemy
        # DDL, so reflection can fail per table.
        try:
            ddl = self.inspector.ddl(tablename_in=tablename, tablename_out=tablename_out, in_schema=schema)
            with open(path_table_schema, "w") as fh_schema:
                print(ddl, file=fh_schema)
        except Exception as ex:
            logger.warning(f"Could not generate schema for {schema}.{tablename}: {ex}")
            self.schema_failures.append({"schema": schema, "table": tablename, "reason": f"{type(ex).__name__}: {ex}"})

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

        with tqdm(relations, desc="Capturing definitions", disable=None) as progress:
            for relation in progress:
                schema = relation["table_schema"]
                name = relation["table_name"]
                progress.set_postfix_str(f"{schema}.{name}")
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


class SystemTableImportError(Exception):
    """
    Raised when a bundle's tables did not all reach the cluster intact.
    """


class BulkInsertOutcome:
    """
    Tally what a bulk insert wrote. CrateDB answers with HTTP 200 and a `rowcount`
    of -2 for each refused row, which the driver does not raise on.
    """

    def __init__(self):
        self.sent = 0
        self.written = 0
        self.reasons: t.Dict[str, int] = {}

    @property
    def missing(self) -> int:
        return self.sent - self.written

    def insert(self, pd_table, conn, keys, data_iter) -> None:
        """
        Insertion method for `pandas.DataFrame.to_sql`, called once per chunk.
        """
        sql = str(pd_table.table.insert().compile(bind=conn))
        data = list(data_iter)
        self.sent += len(data)
        cursor = conn._dbapi_connection.cursor()
        try:
            results = cursor.executemany(sql, data) or []
        finally:
            cursor.close()
        for result in results:
            rowcount = result.get("rowcount", 0)
            if rowcount > 0:
                self.written += rowcount
            else:
                reason = str(result.get("error", {}).get("message", "no reason reported"))
                self.reasons[reason] = self.reasons.get(reason, 0) + 1

    def reason_summary(self) -> str:
        if not self.reasons:
            return "the cluster reported no reason"
        return "; ".join(f"{count}x {reason}" for reason, count in self.reasons.items())


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

    @staticmethod
    def undeclared_columns(tablename: str, schema_sql: str) -> t.List[str]:
        """
        The columns a bundle's DDL declares without the clause CrateDB needs to store their values.
        """
        source = None
        for schema, prefix in ExportSettings.FILENAME_PREFIXES.items():
            if tablename.startswith(prefix):
                source = (schema, tablename[len(prefix) :])
                break
        if source is None:
            return []

        required = dict.fromkeys(SystemTableKnowledge.UNINDEXED_OBJECT_COLUMNS.get(source, ()), "OBJECT(IGNORED)")
        required.update(dict.fromkeys(SystemTableKnowledge.UNINDEXED_TEXT_COLUMNS.get(source, ()), "INDEX OFF"))

        undeclared = []
        for column, declaration in required.items():
            match = re.search(rf'^\s*"?{re.escape(column)}"?\s+(.*)$', schema_sql, re.MULTILINE)
            if match and declaration not in match.group(1):
                undeclared.append(column)
        return undeclared

    def load(self):
        path_schema = self.source / ExportSettings.SCHEMA_PATH
        path_data = self.source / ExportSettings.DATA_PATH

        if not path_schema.exists():
            raise FileNotFoundError(f"Path does not exist: {path_schema}")

        logger.info(f"Importing system tables from: {self.source}")

        with logging_redirect_tqdm():
            failures = self._load(path_schema, path_data)

        if failures:
            raise SystemTableImportError(f"Tables not restored in full: {', '.join(failures)}")

    def _load(self, path_schema: Path, path_data: Path) -> t.List[str]:
        """
        Restore every table the bundle carries, and return those not restored in full.
        """
        import pandas as pd

        restored = 0
        failures: t.List[str] = []
        for tablename in tqdm(self.table_names()):
            path_table_schema = path_schema / f"{tablename}.sql"
            path_table_data = path_data / f"{tablename}.{self.data_format}"

            outcome = BulkInsertOutcome()
            try:
                # The bundle's definition wins over one left behind by an earlier restore.
                schema_sql = path_table_schema.read_text()
                self.adapter.run_sql(f"DROP TABLE IF EXISTS {self.adapter.quote_relation_name(tablename)};")
                self.adapter.run_sql(schema_sql)

                # The exporter writes no data file for a table without rows.
                if path_table_data.exists() and path_table_data.stat().st_size > 0:
                    df: "pd.DataFrame" = pd.DataFrame.from_records(self.load_table(path_table_data))
                    df.to_sql(
                        name=tablename,
                        con=self.adapter.engine,
                        index=False,
                        if_exists="append",
                        method=outcome.insert,
                    )
            except Exception as ex:
                error_logger(self.debug)(f"Importing table failed: {tablename}. Reason: {ex}")
                failures.append(tablename)
                continue

            if outcome.missing:
                message = (
                    f"Importing table incomplete: {tablename}. "
                    f"The cluster took {outcome.written} of {outcome.sent} rows: "
                    f"{outcome.reason_summary()}"
                )
                undeclared = self.undeclared_columns(tablename, schema_sql)
                if undeclared:
                    message += (
                        f". This bundle defines {', '.join(undeclared)} without the declaration CrateDB needs "
                        f"to store their values. Export the bundle again from the source cluster, or correct "
                        f"the definition in {path_table_schema}"
                    )
                logger.error(message)
                failures.append(tablename)
                continue

            restored += 1

        logger.info(f"Successfully imported {restored} system tables")
        return failures

    def load_table(self, path: Path) -> t.List:
        import polars as pl

        if path.suffix in [".jsonl"]:
            return orjsonl.load(path)
        elif path.suffix in [".parquet", ".pq"]:
            return pl.read_parquet(path).to_pandas().to_dict("records")
        else:
            raise NotImplementedError(f"Input format not implemented: {path.suffix}")
