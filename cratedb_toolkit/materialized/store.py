# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import typing as t
import uuid

import sqlalchemy as sa
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

from cratedb_toolkit.materialized.model import MaterializedView, MaterializedViewSettings
from cratedb_toolkit.model import TableAddress
from cratedb_toolkit.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


class MaterializedViewStore:
    """
    A wrapper around the materialized view management table.

    TODO: Generalize, see `RetentionPolicyStore`.
    """

    def __init__(self, settings: MaterializedViewSettings):
        self.settings = settings

        if self.settings.materialized_table.table is None:
            raise ValueError("Unable to create MaterializedViewStore without table name")

        logger.info(
            f"Connecting to database {self.settings.database.safe}, table {self.settings.materialized_table.fullname}"
        )

        # Set up generic database adapter.
        self.database: DatabaseAdapter = DatabaseAdapter(dburi=self.settings.database.dburi)

        # Set up SQLAlchemy Core adapter for materialized view management table.
        metadata = MetaData(schema=self.settings.materialized_table.schema)
        self.table = Table(self.settings.materialized_table.table, metadata, autoload_with=self.database.engine)

    def create(self, mview: MaterializedView, ignore: t.Optional[str] = None):
        """
        Create a new materialized view, and return its identifier.
        """

        # TODO: Sanity check, whether target table already exists?

        ignore = ignore or ""

        # Sanity checks.
        if mview.table_schema is None:
            raise ValueError("Table schema needs to be defined")
        if mview.table_name is None:
            raise ValueError("Table name needs to be defined")
        if self.exists(mview):
            if not ignore.startswith("DuplicateKey"):
                raise ValueError(f"Materialized view '{mview.table_schema}.{mview.table_name}' already exists")

        table = self.table
        # TODO: Add UUID as converter to CrateDB driver?
        identifier = str(uuid.uuid4())
        data = mview.to_storage_dict(identifier=identifier)
        insertable = sa.insert(table).values(**data).returning(table.c.id)
        cursor = self.execute(insertable)
        identifier = cursor.one()[0]
        self.synchronize()
        return identifier

    def retrieve(self):
        """
        Retrieve all records from database table.

        TODO: Add filtering capabilities.
        """

        # Run SELECT statement, and return result.
        selectable = sa.select(self.table)
        records = self.query(selectable)
        return records

    def get_by_table(self, table_address: TableAddress) -> MaterializedView:
        """
        Retrieve a materialized view by its table address (schema and table name).

        Raises KeyError if the materialized view doesn't exist.
        """
        table = self.table
        selectable = sa.select(table).where(
            table.c.table_schema == table_address.schema,
            table.c.table_name == table_address.table,
        )
        logger.info(f"View definition DQL: {selectable}")
        try:
            record = self.query(selectable)[0]
        except IndexError as ex:
            raise KeyError(
                f"Synthetic materialized table definition does not exist: {table_address.schema}.{table_address.table}"
            ) from ex
        mview = MaterializedView.from_record(record)
        return mview

    def delete(self, identifier: str) -> int:
        """
        Delete materialized view by identifier.

        Note: This only removes the entry from the management table,
        it does not drop the actual materialized view table.
        """
        table = self.table
        constraint = table.c.id == identifier
        deletable = sa.delete(table).where(constraint)
        result = self.execute(deletable)
        self.synchronize()
        if result.rowcount == 0:
            logger.warning(f"Materialized view not found with id: {identifier}")
        return result.rowcount

    def execute(self, statement) -> CursorResult:
        """
        Execute SQL statement, and return result object.
        """
        with Session(self.database.engine) as session:
            result = session.execute(statement)
            session.commit()
            return result

    def query(self, statement) -> t.List[t.Dict]:
        """
        Execute SQL statement, fetch result rows, and return them converted to dictionaries.
        """
        cursor = self.execute(statement)
        rows = cursor.mappings().fetchall()
        records = [dict(row.items()) for row in rows]
        return records

    def exists(self, mview: MaterializedView):
        """
        Check if the materialized view already exists.
        """
        table = self.table
        selectable = sa.select(table).where(
            table.c.table_schema == mview.table_schema,
            table.c.table_name == mview.table_name,
        )
        result = self.query(selectable)
        return bool(result)

    def synchronize(self):
        """
        Synchronize data by issuing a `REFRESH TABLE` statement.
        """
        sql = f"REFRESH TABLE {self.settings.materialized_table.fullname};"
        self.database.run_sql(sql)
