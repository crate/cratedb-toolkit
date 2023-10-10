# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import datetime
import logging
import typing as t

import sqlalchemy as sa

from cratedb_toolkit.retention.model import JobSettings, RetentionPolicy, RetentionStrategy, RetentionTask
from cratedb_toolkit.retention.store import RetentionPolicyStore
from cratedb_toolkit.retention.strategy.delete import DeleteRetentionTask
from cratedb_toolkit.retention.strategy.reallocate import ReallocateRetentionJob, ReallocateRetentionTask
from cratedb_toolkit.retention.strategy.snapshot import SnapshotRetentionTask
from cratedb_toolkit.util.database import run_sql

logger = logging.getLogger(__name__)


class RetentionJob:
    """
    The main application, implementing a retention and expiration
    management subsystem for CrateDB.

    The retention job evaluates its configuration and runtime settings,
    and dispatches to corresponding retention strategy implementations.
    """

    def __init__(self, settings: JobSettings):
        # When no cutoff date is obtained from the caller, use `today()` as default value.
        if settings.cutoff_day is None:
            today = datetime.date.today().isoformat()
            logger.info(f"No cutoff date selected, will use today(): {today}")
            settings.cutoff_day = today

        # Runtime context settings.
        self.settings = settings

        # Retention policy store API.
        self.store = RetentionPolicyStore(settings=self.settings)

    def start(self):
        """
        Resolve retention policies to tasks, and invoke them.
        """
        logger.info(
            f"Connecting to database {self.settings.database.safe}, " f"table {self.settings.policy_table.fullname}"
        )

        msg = f"Starting data retention using '{self.settings.strategy}', cutoff day '{self.settings.cutoff_day}'"
        if self.settings.tags:
            msg += f", and tags '{self.settings.tags}'"
        logger.info(msg)

        # Invoke job.
        # TODO: Add audit logging.
        # TODO: Add job tracking.

        for task in self.get_retention_tasks():
            logger.info(f"Executing data retention task: {task}")

            sql_bunch: t.Iterable = task.to_sql()
            if not isinstance(sql_bunch, t.List):
                sql_bunch = [sql_bunch]

            # Run a sequence of SQL statements for this task.
            # Stop the sequence when a step fails.
            for sql in sql_bunch:
                if self.settings.dry_run:
                    logger.info(f"Pretending to execute SQL statement:\n{sql}")
                    continue
                try:
                    run_sql(dburi=self.settings.database.dburi, sql=sql)
                except Exception:
                    logger.exception(f"Data retention SQL statement failed: {sql}")
                    break

    def get_retention_tasks(self) -> t.Generator[RetentionTask, None, None]:
        """
        Derive retention tasks from policies. There will be one task per resolved partition.
        """

        # Sanity checks.
        if self.settings.strategy is None:
            raise ValueError("Unable to load retention policies without strategy")

        if self.settings.tags and not self.store.tags_exist(self.settings.tags):
            logger.warning(f"No retention policies found with tags: {sorted(self.settings.tags)}")
            return

        # Load policies, filtering by strategy and tags.
        policies = list(self.store.retrieve_policies(strategy=self.settings.strategy, tags=self.settings.tags))
        if not policies:
            logger.warning("No matching retention policies found")
            return

        for policy in policies:
            logger.info(f"Processing data retention policy: {policy}")

            # Verify data table exists.
            if not self.store.database.table_exists(policy.table_fullname):
                logger.warning(f"Data table not found: {policy.table_fullname}")
                continue

            # Render SQL statement to gather tasks.
            sql_renderer = TaskSqlRenderer(settings=self.settings, store=self.store, policy=policy)
            selectable = sql_renderer.render()
            results = self.store.query(selectable)
            if not results:
                logger.warning(f"No data to be retired from data table: {policy.table_fullname}")
                continue

            # Resolve task runner implementation.
            task_class: t.Type[RetentionTask]
            if policy.strategy is RetentionStrategy.DELETE:
                task_class = DeleteRetentionTask
            elif policy.strategy is RetentionStrategy.REALLOCATE:
                task_class = ReallocateRetentionTask
            elif policy.strategy is RetentionStrategy.SNAPSHOT:
                task_class = SnapshotRetentionTask
            else:
                raise NotImplementedError(f"Retention strategy {policy.strategy_type} not implemented yet")

            # Iterate tasks, and invoke task runner.
            for result in results:
                task = task_class.factory(
                    table_schema=policy.table_schema,
                    table_name=policy.table_name,
                    table_fullname=policy.table_fullname,
                    partition_column=policy.partition_column,
                    partition_value=result["partition_value"],
                    reallocation_attribute_name=policy.reallocation_attribute_name,
                    reallocation_attribute_value=policy.reallocation_attribute_value,
                    target_repository_name=policy.target_repository_name,
                )
                yield task


class TaskSqlRenderer:
    """
    Render SQL statement to gather retention tasks.
    """

    def __init__(self, settings: JobSettings, store: RetentionPolicyStore, policy: RetentionPolicy):
        """
        TODO: Get rid of `self.settings`?
        """
        self.settings = settings
        self.store = store
        self.policy = policy

    def render(self):
        """
        Render SQL statement.

        SQL DQL clause for selecting records from the retention policy database table,
        used for all strategy implementations. It can be interpolated into other SQL
        templates by using the `policy_dql` template variable.

        The retention policy database table is called `"ext"."retention_policy"` by default.
        """

        # Sanity checks.
        if self.settings.cutoff_day is None:
            raise ValueError("Unable to operate without cutoff date")

        # Formulate SQL statement to gather relevant partitions to be expired.
        # The `ORDER BY` is used to process partitions in chronological order, to not produce
        # temporary gaps. Imagine a table is partitioned by month: If partitions which are
        # selected for expiry are not processed in order, the recipe could first drop February,
        # then March, and then January, which would produce inconsistent gaps in the data, if
        # it was queried at the same time concurrently, while the expiration process is taking
        # place.
        policy = self.policy
        selectable = sa.text(
            f"""
        SELECT
            *,
            TRY_CAST(p.values['{policy.partition_column}'] AS BIGINT) AS partition_value
        FROM
            "information_schema"."table_partitions" AS p
        WHERE 1
            AND p.table_schema = '{policy.table_schema}'
            AND p.table_name = '{policy.table_name}'
            AND p.values['{policy.partition_column}'] <
                '{self.settings.cutoff_day}'::TIMESTAMP - '{policy.retention_period} days'::INTERVAL
        ORDER BY
            partition_value ASC
        """
        )
        return self.specialize(selectable)

    def specialize(self, selectable):
        """
        Specialize SQL statement.

        For example, the `REALLOCATE` strategy needs to wrap the foundational SQL statement
        into another one, in order to correlate the results with `sys.shards` and `sys.nodes`
        tables.
        """
        if self.settings.strategy is RetentionStrategy.REALLOCATE:
            # Acquire SQL template.
            sql_wrapper = ReallocateRetentionJob.SQL

            # Render SQL statement using classic string templating.
            tplvars = self.policy.to_storage_dict()
            sql = sql_wrapper.format(policy_dql=selectable, **tplvars)
            selectable = sa.text(sql)

        return selectable
