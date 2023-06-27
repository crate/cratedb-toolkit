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

    _tasks_sql: str = dataclasses.field(init=False)
    _action_class: t.Any = dataclasses.field(init=False)

    def start(self):
        """
        Evaluate retention policies, and invoke actions.
        """
        for policy in self.get_policies():
            logger.info(f"Executing data retention policy: {policy}")
            sql = policy.to_sql()

            logger.info(f"Running data retention SQL statement: {sql}")
            run_sql(dburi=self.dburi, sql=sql)

    def get_policies(self):
        """
        Resolve retention policy items.
        """
        if self._tasks_sql is None:
            raise ValueError("Loading retention policies needs an SQL statement")
        if self._action_class is None:
            raise ValueError("Loading retention policies needs an action class")

        # Read SQL DDL statement.
        sql = read_text("cratedb_rollup.strategy", self._tasks_sql)
        sql = sql.format(day=self.cutoff_day)

        # Resolve retention policies.
        policy_records = run_sql(self.dburi, sql)
        logger.info(f"Loaded retention policies: {policy_records}")
        for record in policy_records:
            policy = self._action_class.from_record(record)
            yield policy
