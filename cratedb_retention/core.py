# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import dataclasses
import logging
import typing as t
from importlib.resources import read_text

import sqlalchemy as sa

from cratedb_retention.model import JobSettings, RetentionStrategy
from cratedb_retention.store import RetentionPolicyStore
from cratedb_retention.util.database import run_sql

logger = logging.getLogger(__name__)


class RetentionJob:
    """
    The retention job implementation evaluates its configuration and runtime settings,
    and dispatches to corresponding retention strategy implementations.

    This is effectively the main application, implementing a retention and expiration
    management subsystem for CrateDB.
    """

    def __init__(self, settings: JobSettings):
        self.settings = settings
        self.store = RetentionPolicyStore(settings=self.settings)

    def start(self):
        logger.info(
            f"Connecting to database {self.settings.database.safe}, " f"table {self.settings.policy_table.fullname}"
        )

        logger.info(
            f"Starting data retention using '{self.settings.strategy}' " f"and cut-off day '{self.settings.cutoff_day}'"
        )

        strategy = self.settings.strategy

        implementation: t.Type[GenericRetention]

        # Resolve strategy implementation.
        if strategy is RetentionStrategy.DELETE:
            from cratedb_retention.strategy.delete import DeleteRetention

            implementation = DeleteRetention
        elif strategy is RetentionStrategy.REALLOCATE:
            from cratedb_retention.strategy.reallocate import ReallocateRetention

            implementation = ReallocateRetention
        elif strategy is RetentionStrategy.SNAPSHOT:
            from cratedb_retention.strategy.snapshot import SnapshotRetention

            implementation = SnapshotRetention
        else:
            raise NotImplementedError(f"Retention strategy {strategy} not implemented yet")

        # Propagate runtime context settings, and invoke job.
        # TODO: Add audit logging.
        # TODO: Add job tracking.
        job = implementation(settings=self.settings, store=self.store)
        job.start()


@dataclasses.dataclass
class GenericRetention:
    """
    Represent a complete generic data retention job.
    """

    # Runtime context settings.
    settings: JobSettings

    # Retention policy store API.
    store: RetentionPolicyStore

    # Which action class to use for deserializing records.
    _action_class: t.Any = dataclasses.field(init=False)

    # File name of SQL statement to load retention policies.
    _tasks_sql_file: t.Union[str, None] = dataclasses.field(init=False, default=None)

    # SQL statement to load retention policies.
    _tasks_sql_text: t.Union[str, None] = dataclasses.field(init=False, default=None)

    def start(self):
        """
        Evaluate retention policies, and invoke actions.
        """

        for policy in self.get_policy_tasks():
            logger.info(f"Executing data retention policy: {policy}")
            sql_bunch: t.Iterable = policy.to_sql()
            if not isinstance(sql_bunch, t.List):
                sql_bunch = [sql_bunch]

            # Run a sequence of SQL statements for this task.
            # Stop the sequence once anyone fails.
            for sql in sql_bunch:
                try:
                    run_sql(dburi=self.settings.database.dburi, sql=sql)
                except Exception:
                    logger.exception(f"Data retention SQL statement failed: {sql}")
                    # TODO: Do not `raise`, but `break`. Other policies should be executed.
                    raise

    def get_where_clause(self, field_prefix: str = ""):
        """
        Compute SQL WHERE clause based on selected strategy and tags.

        TODO: Migrate to SQLAlchemy.
        """
        if self.settings.strategy is None:
            raise ValueError("Unable to build where clause without retention strategy")

        strategy = str(self.settings.strategy.value).lower()
        fragments = [f"{field_prefix}strategy='{strategy}'"]

        constraints = self.store.get_tags_constraints(self.settings.tags, table_alias="r")
        if constraints is not None:
            constraints_sql = constraints.compile(self.store.database.engine, compile_kwargs={"literal_binds": True})
            fragments += [str(constraints_sql)]

        clauses = map(sa.text, fragments)
        return sa.and_(*clauses)

    def get_policy_tasks(self):
        """
        Resolve retention policy items.
        """
        if self._action_class is None:
            raise ValueError("Loading retention policies needs an action class")

        # Read baseline SQL clause, for selecting records from the retention policy
        # database table, to be interpolated into the other templates.
        policy_dql = read_text("cratedb_retention.strategy", "policy.sql")

        if self.settings.tags and not self.store.tags_exist(self.settings.tags):
            logger.warning(f"No retention policies found with tags: {self.settings.tags}")
            return []

        where_clause = self.get_where_clause(field_prefix="r.")
        logger.info(f"Retention tasks - WHERE clause: {where_clause}")

        # Read SQL statement, and interpolate runtime settings as template variables.
        if self._tasks_sql_file:
            sql = read_text("cratedb_retention.strategy", self._tasks_sql_file)
        elif self._tasks_sql_text:
            sql = self._tasks_sql_text
        else:
            sql = f"""
{policy_dql}
WHERE
{where_clause}
;
            """
        tplvars = self.settings.to_dict()
        try:
            sql = sql.format(policy_dql=policy_dql, where_clause=where_clause)
        except KeyError:
            pass
        sql = sql.format_map(tplvars)

        # Load retention policies, already resolved against source table,
        # and retention period vs. cut-off date.
        policy_records = run_sql(self.settings.database.dburi, sql)
        if not policy_records:
            logger.warning("Retention policies and/or data table not found, or no data to be retired")
            return []

        logger.info(f"Loaded retention policies: {policy_records}")
        for record in policy_records:
            # Unmarshal entity from database table record to Python object.
            policy = self._action_class.from_record(record)
            yield policy
