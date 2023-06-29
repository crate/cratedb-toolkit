# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import dataclasses
import logging
import typing as t
from collections import OrderedDict
from copy import deepcopy
from enum import Enum
from importlib.resources import read_text

from boltons.urlutils import URL

from cratedb_retention.util.database import run_sql

logger = logging.getLogger(__name__)


class RetentionStrategy(Enum):
    """
    Enumerate list of retention strategies.
    """

    DELETE = "DELETE"
    REALLOCATE = "REALLOCATE"
    SNAPSHOT = "SNAPSHOT"


@dataclasses.dataclass
class RetentionPolicy:
    """
    Manage a retention policy entity.
    """

    strategy: str
    table_schema: str
    table_name: str
    table_fullname: str
    partition_column: str
    partition_value: str
    reallocation_attribute_name: str
    reallocation_attribute_value: str
    target_repository_name: str

    @classmethod
    def from_record(cls, record):
        """
        Factory for creating instance from database record.
        """
        return cls(**cls.record_mapper(record))

    @classmethod
    def record_mapper(cls, record):
        """
        Map database record to instance attributes.
        """
        fields = dataclasses.fields(cls)
        out = OrderedDict()
        for index, field in enumerate(fields):
            out[field.name] = record[index]
        return out


@dataclasses.dataclass
class DatabaseAddress:
    """
    Manage a database address, which is either a SQLAlchemy-
    compatible database URI, or a regular HTTP URL.
    """

    uri: URL

    @classmethod
    def from_string(cls, url):
        """
        Factory method to create an instance from a URI in string format.
        """
        return cls(uri=URL(url))

    @property
    def dburi(self):
        """
        Return a string representation of the database URI.
        """
        return str(self.uri)

    @property
    def safe(self):
        """
        Return a string representation of the database URI, safe for printing.
        The password is stripped from the URL, and replaced by `REDACTED`.
        """
        uri = deepcopy(self.uri)
        uri.password = "REDACTED"  # noqa: S105
        return str(uri)


@dataclasses.dataclass
class TableAddress:
    """
    Manage a table address, which is made of "<schema>"."<table>".
    """

    schema: str
    table: str

    @property
    def fullname(self):
        return f'"{self.schema}"."{self.table}"'


@dataclasses.dataclass
class JobSettings:
    """
    Bundle all configuration and runtime settings.
    """

    # Database connection URI.
    database: DatabaseAddress = dataclasses.field(
        default_factory=lambda: DatabaseAddress.from_string("crate://localhost/")
    )

    # Retention strategy.
    strategy: t.Optional[RetentionStrategy] = None

    # Retention cutoff timestamp.
    cutoff_day: t.Optional[str] = None

    # Where the retention policy table is stored.
    policy_table: TableAddress = dataclasses.field(
        default_factory=lambda: TableAddress(schema="ext", table="retention_policy")
    )

    def to_dict(self):
        data = dataclasses.asdict(self)
        data["policy_table"] = self.policy_table
        return data


@dataclasses.dataclass
class GenericRetention:
    """
    Represent a complete generic data retention job.
    """

    # Runtime context settings.
    settings: JobSettings

    # File name of SQL statement to load retention policies.
    _tasks_sql: str = dataclasses.field(init=False)

    # Which action class to use for deserializing records.
    _action_class: t.Any = dataclasses.field(init=False)

    def start(self):
        """
        Evaluate retention policies, and invoke actions.
        """

        for policy in self.get_policies():
            logger.info(f"Executing data retention policy: {policy}")
            sql_bunch: t.Iterable = policy.to_sql()
            if not isinstance(sql_bunch, t.List):
                sql_bunch = [sql_bunch]

            # Run a sequence of SQL statements for this task.
            # Stop the sequence once anyone fails.
            for sql in sql_bunch:
                logger.info(f"Running data retention SQL statement: {sql}")
                try:
                    run_sql(dburi=self.settings.database.dburi, sql=sql)
                except Exception:
                    logger.exception(f"Data retention SQL statement failed: {sql}")
                    # TODO: Do not `raise`, but `break`. Other policies should be executed.
                    raise

    def get_policies(self):
        """
        Resolve retention policy items.
        """
        if self._tasks_sql is None:
            raise ValueError("Loading retention policies needs an SQL statement")
        if self._action_class is None:
            raise ValueError("Loading retention policies needs an action class")

        # Read baseline SQL clause, for selecting records from the retention policy
        # database table, to be interpolated into the other templates.
        policy_dql = read_text("cratedb_retention.strategy", "policy.sql")

        # Read SQL statement, and interpolate runtime settings as template variables.
        sql = read_text("cratedb_retention.strategy", self._tasks_sql)
        tplvars = self.settings.to_dict()
        try:
            sql = sql.format(policy_dql=policy_dql)
        except KeyError:
            pass
        sql = sql.format_map(tplvars)

        # Load retention policies.
        policy_records = run_sql(self.settings.database.dburi, sql)
        logger.info(f"Loaded retention policies: {policy_records}")
        for record in policy_records:
            # Unmarshal entity from database table record to Python object.
            policy = self._action_class.from_record(record)
            yield policy
