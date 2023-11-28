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

from cratedb_toolkit.retention.model import JobSettings, RetentionPolicy, RetentionStrategy
from cratedb_toolkit.util.database import DatabaseAdapter, sa_is_empty

logger = logging.getLogger(__name__)


class SQLAlchemyTagHelperMixin:
    """
    Support tags stored within an `OBJECT(DYNAMIC)` column in CrateDB, managed with SQLAlchemy.
    """

    tag_column = "tags"

    @abc.abstractmethod
    def execute(self, statement):
        pass

    @abc.abstractmethod
    def synchronize(self):
        pass

    def get_tags_constraints(self, tags: t.Union[t.List[str], t.Set[str]]):
        """
        Return list of SQL WHERE constraint clauses from given tags.
        """
        from sqlalchemy.sql.selectable import NamedFromClause  # type: ignore[attr-defined]

        table: NamedFromClause = self.table  # type: ignore[attr-defined]
        constraints = []
        for tag in tags:
            if not tag:
                continue
            constraint = table.c[self.tag_column][tag] != sa.Null()  # type: ignore[attr-defined]
            constraints.append(constraint)
        return sa.and_(sa.true(), *constraints)

    def tags_exist(self, tags: t.Union[t.List[str], t.Set[str]]):
        """
        Check if given tags exist in the database.

        When not, no query can yield results, so we do not need to bother about
        failing JOIN operations because of non-existing tags.

        TODO: Create corresponding issue at crate/crate.
        """
        table = self.table  # type: ignore[attr-defined]
        where_clause = self.get_tags_constraints(tags)
        if sa_is_empty(where_clause):
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
        if sa_is_empty(where_clause):
            logger.warning("Unable to compute criteria for deletion")
            return 0
        deletable = sa.delete(table).where(where_clause)  # type: ignore[arg-type]
        result = self.execute(deletable)
        self.synchronize()
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

        if self.settings.policy_table.table is None:
            raise ValueError("Unable to create RetentionPolicyStore without table name")

        # Set up generic database adapter.
        self.database: DatabaseAdapter = DatabaseAdapter(dburi=self.settings.database.dburi)

        # Set up SQLAlchemy Core adapter for retention policy table.
        metadata = MetaData(schema=self.settings.policy_table.schema)
        self.table = Table(self.settings.policy_table.table, metadata, autoload_with=self.database.engine)

    def create(self, policy: RetentionPolicy, ignore: t.Optional[str] = None):
        """
        Create a new retention policy, and return its identifier.
        """

        ignore = ignore or ""

        # Sanity checks.
        if policy.table_schema is None:
            raise ValueError("Table schema needs to be defined")
        if policy.table_name is None:
            raise ValueError("Table name needs to be defined")
        if self.exists(policy):
            if not ignore.startswith("DuplicateKey"):
                raise ValueError(
                    f"Retention policy for table '{policy.table_schema}.{policy.table_name}' already exists"
                )

        table = self.table
        # TODO: Add UUID as converter to CrateDB driver?
        identifier = str(uuid.uuid4())
        data = policy.to_storage_dict(identifier=identifier)
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
        records = list(map(self.row_to_record, records))
        return records

    def retrieve_policies(self, strategy: RetentionStrategy, tags):
        """
        Retrieve effective policies to process, by strategy and tags.
        """
        table = self.table
        selectable = sa.select(table).where(table.c.strategy == strategy.to_database())
        criteria = self.get_tags_constraints(tags)
        selectable = selectable.where(criteria)
        for row in self.query(selectable):
            policy = RetentionPolicy.from_record(row)
            yield policy

    def retrieve_tags(self):
        """
        Retrieve all tags from database table.

        TODO: Add filtering capabilities.
        """

        # Run SELECT statement, and return result.
        selectable = sa.select(self.table.c.tags)
        records = self.query(selectable)
        tags = []
        for record in records:
            tags += self.row_to_record(record)["tags"]
        tags = sorted(set(tags))
        return tags

    @staticmethod
    def row_to_record(item):
        """
        Compute serializable representation from database row.

        Convert between representation of tags. In Python land, it's a `set`,
        while in database land, it's an `OBJECT`.
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
        self.synchronize()
        if result.rowcount == 0:
            logger.warning(f"Retention policy not found with id: {identifier}")
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

    def exists(self, policy: RetentionPolicy):
        """
        Check if retention policy for specific table already exists.
        """
        table = self.table
        selectable = sa.select(table).where(
            table.c.table_schema == policy.table_schema,
            table.c.table_name == policy.table_name,
        )
        result = self.query(selectable)
        return bool(result)

    def synchronize(self):
        """
        Synchronize data by issuing `REFRESH TABLE` statement.
        """
        sql = f"REFRESH TABLE {self.settings.policy_table.fullname};"
        self.database.run_sql(sql)
