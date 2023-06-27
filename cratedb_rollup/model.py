# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import dataclasses
import logging
import typing as t
from abc import abstractmethod
from importlib.resources import read_text

from cratedb_rollup.util.database import run_sql

logger = logging.getLogger(__name__)


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

    # Database connection URI.
    dburi: str

    # Retention cutoff timestamp.
    cutoff_day: str

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

            for sql in sql_bunch:
                logger.info(f"Running data retention SQL statement: {sql}")
                try:
                    run_sql(dburi=self.dburi, sql=sql)
                except:
                    logger.exception(f"Data retention SQL statement failed: {sql}")
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
        sql = read_text("cratedb_rollup.strategy", self._tasks_sql)
        sql = sql.format(day=self.cutoff_day)

        # Resolve retention policies.
        policy_records = run_sql(self.dburi, sql)
        logger.info(f"Loaded retention policies: {policy_records}")
        for record in policy_records:
            policy = self._action_class.from_record(record)
            yield policy
