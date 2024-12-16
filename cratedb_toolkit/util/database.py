# Copyright (c) 2023-2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import io
import logging
import os
import typing as t
from pathlib import Path

import sqlalchemy as sa
import sqlparse
from boltons.urlutils import URL
from cratedb_sqlparse import sqlparse as sqlparse_cratedb
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql.elements import AsBoolean
from sqlalchemy_cratedb.dialect import CrateDialect

from cratedb_toolkit.model import TableAddress
from cratedb_toolkit.util.data import str_contains

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type: ignore[assignment]

logger = logging.getLogger(__name__)


def run_sql(dburi: str, sql: str, records: bool = False):
    return DatabaseAdapter(dburi=dburi).run_sql(sql=sql, records=records)


# Just an instance of the dialect used for quoting purposes.
dialect = CrateDialect()


class DatabaseAdapter:
    """
    Wrap SQLAlchemy connection to database.
    """

    internal_tag = "  -- ctk"

    def __init__(self, dburi: str, echo: bool = False, internal: bool = False):
        self.dburi = dburi
        self.internal = internal
        self.engine = sa.create_engine(self.dburi, echo=echo)
        # TODO: Make that go away.
        logger.debug(f"Connecting to CrateDB: {dburi}")
        self.connection = self.engine.connect()

    @staticmethod
    def quote_relation_name(ident: str) -> str:
        """
        Quote a simple or full-qualified table/relation name, when needed.

        Simple:         <table>
        Full-qualified: <schema>.<table>

        Happy path examples:

            foo => foo
            Foo => "Foo"
            "Foo" => "Foo"
            foo.bar => foo.bar
            foo-bar.baz_qux => "foo-bar".baz_qux

        Such input strings will not be modified:

            "foo.bar" => "foo.bar"
        """

        # Heuristically consider that if a quote exists at the beginning or the end
        # of the input string, that the relation name has been quoted already.
        if ident.startswith('"') or ident.endswith('"'):
            return ident

        # Heuristically consider if a dot is included, that it's a full-qualified
        # identifier like <schema>.<table>. It needs to be split, in order to apply
        # identifier quoting properly.
        if "." in ident:
            parts = ident.split(".")
            if len(parts) > 2:
                raise ValueError(f"Invalid relation name {ident}")
            return (
                dialect.identifier_preparer.quote_schema(parts[0]) + "." + dialect.identifier_preparer.quote(parts[1])
            )
        return dialect.identifier_preparer.quote(ident=ident)

    def run_sql(
        self,
        sql: t.Union[str, Path, io.IOBase],
        parameters: t.Mapping[str, str] = None,
        records: bool = False,
        ignore: str = None,
    ):
        """
        Run SQL statement, and return results, optionally ignoring exceptions.
        """

        sql_effective: str
        if isinstance(sql, str):
            sql_effective = sql
        elif isinstance(sql, Path):
            sql_effective = sql.read_text()
        elif isinstance(sql, io.IOBase):
            sql_effective = sql.read()
        else:
            raise TypeError("SQL statement type must be either string, Path, or IO handle")

        try:
            return self.run_sql_real(sql=sql_effective, parameters=parameters, records=records)
        except Exception as ex:
            if not ignore:
                raise
            if ignore not in str(ex):
                raise

    def run_sql_real(self, sql: str, parameters: t.Mapping[str, str] = None, records: bool = False):
        """
        Invoke SQL statement, and return results.
        """
        results = []
        with self.engine.connect() as connection:
            for statement in sqlparse.split(sql):
                if self.internal:
                    statement += self.internal_tag
                result = connection.execute(sa.text(statement), parameters)
                data: t.Any
                if records:
                    rows = result.mappings().fetchall()
                    data = [dict(row.items()) for row in rows]
                else:
                    data = result.fetchall()
                results.append(data)

        # Backward-compatibility.
        if len(results) == 1:
            return results[0]
        else:
            return results

    def count_records(self, name: str, errors: Literal["raise", "ignore"] = "raise", where: str = ""):
        """
        Return number of records in table.
        """
        sql = f"SELECT COUNT(*) AS count FROM {self.quote_relation_name(name)}"  # noqa: S608
        if where:
            sql += f" WHERE {where}"
        try:
            results = self.run_sql(sql=sql)
        except ProgrammingError as ex:
            is_candidate = "RelationUnknown" not in str(ex)
            if is_candidate and errors == "raise":
                raise
            return 0
        return results[0][0]

    def table_exists(self, name: str) -> bool:
        """
        Check whether given table exists.
        """
        sql = f"SELECT 1 FROM {self.quote_relation_name(name)} LIMIT 1;"  # noqa: S608
        try:
            self.run_sql(sql=sql)
            return True
        except Exception:
            return False

    def refresh_table(self, name: str):
        """
        Run a `REFRESH TABLE ...` command.
        """
        sql = f"REFRESH TABLE {self.quote_relation_name(name)};"  # noqa: S608
        self.run_sql(sql=sql)
        return True

    def prune_table(self, name: str, errors: Literal["raise", "ignore"] = "raise"):
        """
        Run a `DELETE FROM ...` command.
        """
        sql = f"DELETE FROM {self.quote_relation_name(name)};"  # noqa: S608
        try:
            self.run_sql(sql=sql)
        except ProgrammingError as ex:
            is_candidate = "RelationUnknown" not in str(ex)
            if is_candidate and errors == "raise":
                raise
            return False
        return True

    def drop_table(self, name: str):
        """
        Run a `DROP TABLE ...` command.
        """
        sql = f"DROP TABLE IF EXISTS {self.quote_relation_name(name)};"  # noqa: S608
        self.run_sql(sql=sql)
        return True

    def drop_repository(self, name: str):
        """
        Drop snapshot repository.
        """
        # TODO: DROP REPOSITORY IF EXISTS
        try:
            sql = f"DROP REPOSITORY {name};"
            self.run_sql(sql)
        except ProgrammingError as ex:
            if not str_contains(ex, "RepositoryUnknownException", "RepositoryMissingException"):
                raise

    def ensure_repository_fs(
        self,
        name: str,
        typename: str,
        location: str,
        drop: bool = False,
    ):
        """
        Make sure the repository exists, and optionally drop it upfront.
        """
        if drop:
            self.drop_repository(name)

        # TODO: CREATE REPOSITORY IF NOT EXISTS
        sql = f"""
            CREATE REPOSITORY
                {name}
            TYPE
                {typename}
            WITH (
                location   = '{location}'
            );
        """
        self.run_sql(sql)

    def ensure_repository_s3(
        self,
        name: str,
        typename: str,
        protocol: str,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        drop: bool = False,
    ):
        """
        Make sure the repository exists, and optionally drop it upfront.
        """
        if drop:
            self.drop_repository(name)

        # TODO: CREATE REPOSITORY IF NOT EXISTS
        sql = f"""
            CREATE REPOSITORY
                {name}
            TYPE
                {typename}
            WITH (
                protocol   = '{protocol}',
                endpoint   = '{endpoint}',
                access_key = '{access_key}',
                secret_key = '{secret_key}',
                bucket     = '{bucket}'
            );
        """
        self.run_sql(sql)

    def ensure_repository_az(
        self,
        name: str,
        typename: str,
        protocol: str,
        endpoint: str,
        account: str,
        key: str,
        container: str,
        drop: bool = False,
    ):
        """
        Make sure the repository exists, and optionally drop it upfront.
        """
        if drop:
            self.drop_repository(name)

        # TODO: CREATE REPOSITORY IF NOT EXISTS
        sql = f"""
            CREATE REPOSITORY
                {name}
            TYPE
                {typename}
            WITH (
                protocol   = '{protocol}',
                endpoint   = '{endpoint}',
                account    = '{account}',
                key        = '{key}',
                container  = '{container}'
            );
        """
        self.run_sql(sql)

    def import_csv_pandas(
        self, filepath: t.Union[str, Path], tablename: str, index=False, chunksize=1000, if_exists="replace"
    ):
        """
        Import CSV data using pandas.
        """
        import pandas as pd

        try:
            from sqlalchemy_cratedb.support import insert_bulk
        except ImportError:  # pragma: nocover
            from crate.client.sqlalchemy.support import insert_bulk

        df = pd.read_csv(filepath)
        with self.engine.connect() as connection:
            return df.to_sql(
                tablename, connection, index=index, chunksize=chunksize, if_exists=if_exists, method=insert_bulk
            )

    def import_csv_dask(
        self,
        filepath: t.Union[str, Path],
        tablename: str,
        index=False,
        chunksize=1000,
        if_exists="replace",
        npartitions: int = None,
        progress: bool = False,
    ):
        """
        Import CSV data using Dask.
        """
        import dask.dataframe as dd
        import pandas as pd

        try:
            from sqlalchemy_cratedb.support import insert_bulk
        except ImportError:  # pragma: nocover
            from crate.client.sqlalchemy.support import insert_bulk

        # Set a few defaults.
        npartitions = npartitions or os.cpu_count()

        if progress:
            from dask.diagnostics import ProgressBar

            pbar = ProgressBar()
            pbar.register()

        # Load data into database.
        df = pd.read_csv(filepath)
        ddf = dd.from_pandas(df, npartitions=npartitions)
        return ddf.to_sql(
            tablename,
            uri=self.dburi,
            index=index,
            chunksize=chunksize,
            if_exists=if_exists,
            method=insert_bulk,
            parallel=True,
            engine_kwargs={"echo": False},
        )

    def describe_table_columns(self, table_name: str):
        """
        Introspect table schema returning defined columns and their types.
        """
        inspector = sa.inspect(self.engine)
        table_address = TableAddress.from_string(table_name)
        return inspector.get_columns(table_name=t.cast(str, table_address.table), schema=table_address.schema)


def sa_is_empty(thing):
    """
    When a WHERE criteria clause is empty, i.e. it contains only an
    `and_` element, let's consider it to be empty.

    TODO: Verify this. How to actually compare SQLAlchemy elements by booleanness?
    """
    return isinstance(thing, AsBoolean)


def decode_database_table(url: str) -> t.Tuple[str, t.Union[str, None]]:
    """
    Decode database and table names from database URI path and/or query string.

    Variants:

        /<database>/<table>
        ?database=<database>&table=<table>

    TODO: Synchronize with `influxio.model.decode_database_table`.
          This one uses `boltons`, the other one uses `yarl`.
    """
    url_ = URL(url)
    database, table = None, None
    try:
        database, table = url_.path.strip("/").split("/")
    except ValueError as ex:
        if "too many values to unpack" not in str(ex) and "not enough values to unpack" not in str(ex):
            raise
        try:
            (database,) = url_.path.strip("/").split("/")
        except ValueError as ex:
            if "too many values to unpack" not in str(ex) and "not enough values to unpack" not in str(ex):
                raise

            database = url_.query_params.get("database")
            table = url_.query_params.get("table")
            if url_.scheme == "crate" and not database:
                database = url_.query_params.get("schema")
            if database is None and table is None:
                if url_.scheme.startswith("file") or url_.scheme.startswith("http"):
                    _, database, table = url_.path.rsplit("/", 2)

                    # If table name is coming from a filesystem, strip suffix, e.g. `books-relaxed.ndjson`.
                    if table:
                        table, _ = table.split(".", 1)

    if database is None and table is None:
        raise ValueError("Database and table must be specified")

    return database, table


def get_table_names(sql: str) -> t.List[t.List[str]]:
    """
    Decode table names from SQL statements.
    """
    names = []
    statements = sqlparse_cratedb(sql)
    for statement in statements:
        local_names = []
        for table in statement.metadata.tables:
            local_names.append(table.name)
        names.append(local_names)
    return names
