# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import sqlalchemy as sa
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql.elements import AsBoolean

from cratedb_toolkit.util.data import str_contains


def run_sql(dburi: str, sql: str, records: bool = False):
    return DatabaseAdapter(dburi=dburi).run_sql(sql=sql, records=records)


class DatabaseAdapter:
    """
    Wrap SQLAlchemy connection to database.
    """

    def __init__(self, dburi: str):
        self.dburi = dburi
        # TODO: Make `echo=True` configurable.
        self.engine = sa.create_engine(self.dburi, echo=False)
        self.connection = self.engine.connect()

    def run_sql(self, sql: str, records: bool = False, ignore: str = None):
        """
        Run SQL statement, and return results, optionally ignoring exceptions.
        """
        try:
            return self.run_sql_real(sql=sql, records=records)
        except Exception as ex:
            if not ignore:
                raise
            if ignore not in str(ex):
                raise

    def run_sql_real(self, sql: str, records: bool = False):
        """
        Invoke SQL statement, and return results.
        """
        result = self.connection.execute(sa.text(sql))
        if records:
            rows = result.mappings().fetchall()
            return [dict(row.items()) for row in rows]
        else:
            return result.fetchall()

    def count_records(self, tablename_full: str):
        """
        Return number of records in table.
        """
        sql = f"SELECT COUNT(*) AS count FROM {tablename_full};"  # noqa: S608
        results = self.run_sql(sql=sql)
        return results[0][0]

    def table_exists(self, tablename_full: str) -> bool:
        """
        Check whether given table exists.
        """
        sql = f"SELECT 1 FROM {tablename_full} LIMIT 1;"  # noqa: S608
        try:
            self.run_sql(sql=sql)
            return True
        except Exception:
            return False

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


def sa_is_empty(thing):
    """
    When a WHERE criteria clause is empty, i.e. it contains only an
    `and_` element, let's consider it to be empty.

    TODO: Verify this. How to actually compare SQLAlchemy elements by booleanness?
    """
    return isinstance(thing, AsBoolean)
