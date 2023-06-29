# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import typing as t

import sqlalchemy as sa
from sqlalchemy.exc import ProgrammingError


def run_sql(dburi: str, sql: str, records: bool = False):
    return DatabaseAdapter(dburi=dburi).run_sql(sql=sql, records=records)


class DatabaseAdapter:
    """
    Wrap SQLAlchemy connection to database.
    """

    def __init__(self, dburi: str):
        self.dburi = dburi
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


class DatabaseTagHelper:
    """
    Support tags stored within an `OBJECT(DYNAMIC)` column in CrateDB.
    """

    def __init__(self, database: DatabaseAdapter, tablename: str):
        self.database = database
        self.tablename = tablename

    @staticmethod
    def get_tags_sql(tags: t.Optional[t.List[str]], field_prefix: str = ""):
        """
        Return list of SQL WHERE constraint clauses from given tags.
        """
        items = []
        if tags:
            for tag in tags:
                item = f"{field_prefix}tags['{tag}'] IS NOT NULL"
                items.append(item)
        return items

    def tags_exist(self, tags: t.List[str]):
        """
        Check if given tags exist in the database.

        When not, no query can yield results, so we do not need to bother about
        failing JOIN operations because if non-existing tags.
        """
        where_clause = sql_and(self.get_tags_sql(tags=tags))
        # Compute SQL WHERE clause for filtering by tags.
        sql = f"SELECT * FROM {self.tablename} WHERE {where_clause};"  # noqa: S608
        try:
            self.database.run_sql(sql)
        except ProgrammingError as ex:
            if "ColumnUnknownException" in str(ex):
                return False
            else:
                raise
        return True


def sql_and(items):
    """
    Join items with SQL's `AND`.
    """
    return str(sa.and_(*map(sa.text, items)))
