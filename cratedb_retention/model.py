# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import dataclasses
import logging
import typing as t
from abc import abstractmethod
from enum import Enum
from importlib.resources import read_text

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
class Settings:
    """
    Bundle all configuration and runtime settings.
    """

    # Database connection URI.
    dburi: str

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


class GenericAction:
    @classmethod
    def from_record(cls, record):
        """
        Factory for creating instance from database record.
        """
        return cls(**cls.record_mapper(record))

    @abstractmethod
    def to_sql(self):
        pass

    @abstractmethod
    def record_mapper(record):
        pass


@dataclasses.dataclass
class GenericRetention:
    """
    Represent a complete generic data retention job.
    """

    # Runtime context settings.
    settings: Settings

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
                    run_sql(dburi=self.settings.dburi, sql=sql)
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

        # Read SQL statement.
        sql = read_text("cratedb_retention.strategy", self._tasks_sql)
        tplvars = self.settings.to_dict()
        sql = sql.format(**tplvars)

        # Resolve retention policies.
        policy_records = run_sql(self.settings.dburi, sql)
        logger.info(f"Loaded retention policies: {policy_records}")
        for record in policy_records:
            policy = self._action_class.from_record(record)
            yield policy
