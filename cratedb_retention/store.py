# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import abc
import logging
import typing as t
import uuid

import sqlalchemy as sa
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import NamedFromClause

from cratedb_retention.model import JobSettings, RetentionPolicy
from cratedb_retention.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


class SQLAlchemyTagHelperMixin:
    """
    Support tags stored within an `OBJECT(DYNAMIC)` column in CrateDB, managed with SQLAlchemy.
    """

    tag_column = "tags"

    @abc.abstractmethod
    def execute(self, statement):
        pass

    def get_tags_constraints(self, tags: t.Union[t.List[str], t.Set[str]], table_alias: t.Optional[str] = None):
        """
        Return list of SQL WHERE constraint clauses from given tags.
        """
        real_table: NamedFromClause = self.table  # type: ignore[attr-defined]
        if table_alias:
            real_table = real_table.alias(table_alias)
        constraints = []
        for tag in tags:
            if not tag:
                continue
            constraint = real_table.c[self.tag_column][tag] != sa.Null()
            constraints.append(constraint)
        if not constraints:
            return None
        return sa.and_(*constraints)

    def tags_exist(self, tags: t.Union[t.List[str], t.Set[str]]):
        """
        Check if given tags exist in the database.

        When not, no query can yield results, so we do not need to bother about
        failing JOIN operations because if non-existing tags.

        TODO: Create corresponding issue at crate/crate.
        """
        table = self.table  # type: ignore[attr-defined]
        where_clause = self.get_tags_constraints(tags)
        if where_clause is None:
            return False
        selectable = sa.select(table).where(where_clause)
        try:
            self.execute(selectable)
        except ProgrammingError as ex:
            if "ColumnUnknownException" in str(ex):
                return False
            else:
                raise
        return True

    def delete_by_tag(self, tag: str):
        """
        Delete retention policy by tag.
        """
        return self.delete_by_all_tags([tag])

    def delete_by_all_tags(self, tags: t.Union[t.List[str], t.Set[str]]):
        """
        Delete retention policy by tag.
        """
        if not tags:
            logger.warning("No tags obtained, skipping deletion")
            return 0

        if not self.tags_exist(tags):
            logger.warning(f"No retention policies found with tags: {tags}")
            return 0

        table = self.table  # type: ignore[attr-defined]
        where_clause = self.get_tags_constraints(tags)
        if where_clause is None:
            logger.warning("Unable to compute constraints for deletion")
            return 0
        deletable = sa.delete(table).where(where_clause)  # type: ignore[arg-type]
        result = self.execute(deletable)
        return result.rowcount


class RetentionPolicyStore(SQLAlchemyTagHelperMixin):
    """
    A wrapper around the retention policy database table.
    """

    def __init__(self, settings: JobSettings):
        self.settings = settings

        logger.info(
            f"Connecting to database {self.settings.database.safe}, " f"table {self.settings.policy_table.fullname}"
        )

        # Set up generic database adapter.
        self.database: DatabaseAdapter = DatabaseAdapter(dburi=self.settings.database.dburi)

        # Set up SQLAlchemy Core adapter for retention policy table.
        metadata = MetaData(schema=self.settings.policy_table.schema)
        self.table = Table(self.settings.policy_table.table, metadata, autoload_with=self.database.engine)

    def create(self, policy: RetentionPolicy, ignore: t.Optional[str] = None):
        """
        Create a new retention policy, and return its identifier.
        """
        table = self.table
        # TODO: Add UUID as converter to CrateDB driver?
        identifier = str(uuid.uuid4())
        insertable = sa.insert(table).values(id=identifier, **policy.to_storage_dict()).returning(table.c.id)
        cursor = self.execute(insertable)
        identifier = cursor.one()[0]
        return identifier

    def retrieve(self):
        """
        Retrieve all records from database table.

        TODO: Add filtering capabilities.
        """
        # Synchronize data.
        sql = f"REFRESH TABLE {self.settings.policy_table.fullname};"
        self.database.run_sql(sql)

        # Run SELECT statement, and return result.
        selectable = sa.select(self.table)
        records = self.query(selectable)
        records = list(map(self.row_to_record, records))
        return records

    @staticmethod
    def row_to_record(item):
        """
        Compute serializable representation from database row.
        """
        if isinstance(item["tags"], dict):
            item["tags"] = sorted(item["tags"].keys())
        return item

    def delete(self, identifier: str):
        """
        Delete retention policy by identifier.
        """
        table = self.table
        constraint = table.c.id == identifier
        deletable = sa.delete(table).where(constraint)
        result = self.execute(deletable)
        return result.rowcount

    def execute(self, statement):
        """
        Execute SQL statement, and return result object.
        """
        with Session(self.database.engine) as session:
            result = session.execute(statement)
            session.commit()
            return result

    def query(self, statement):
        """
        Execute SQL statement, fetch result rows, and return them converted to dictionaries.
        """
        cursor = self.execute(statement)
        rows = cursor.mappings().fetchall()
        records = [dict(row.items()) for row in rows]
        return records
