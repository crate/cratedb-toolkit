# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import sys
import typing as t

import click
from click import ClickException
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.retention.core import RetentionJob
from cratedb_toolkit.retention.model import JobSettings, RetentionPolicy, RetentionStrategy
from cratedb_toolkit.retention.setup.schema import setup_schema
from cratedb_toolkit.retention.store import RetentionPolicyStore
from cratedb_toolkit.util.cli import (
    boot_click,
    click_options_from_dataclass,
    make_command,
    split_list,
)
from cratedb_toolkit.util.data import jd

logger = logging.getLogger(__name__)


def help_setup():
    """
    Setup database schema for storing retention policies.

    Synopsis
    ========

    # Set up the retention policy database table schema.
    cratedb-retention setup "crate://localhost/"

    """  # noqa: E501


def help_list_policies():
    """
    List data retention policies.

    Examples
    ========

    cratedb-retention list-policies "crate://localhost/"

    """  # noqa: E501


def help_list_tags():
    """
    List all tags.

    Examples
    ========

    cratedb-retention list-tags "crate://localhost/"

    """  # noqa: E501


def help_create_policy():
    """
    Create a data retention policy.

    Examples
    ========

    # Create data retention policy using the DELETE strategy.
    cratedb-retention create-policy --strategy=delete \\
      --table-schema=doc --table-name=raw_metrics \\
      --partition-column=ts_day --retention-period=1 \\
      "crate://localhost/"

    """  # noqa: E501


def help_delete_policy():
    """
    Delete a data retention policy.

    Examples
    ========

    # Delete data retention policy by identifier.
    cratedb-retention delete-policy --id=2af93f28-b315-4bb0-b870-38d3416277f7 \\
      "crate://localhost/"

    """  # noqa: E501


def help_run():
    """
    Invoke data retention workflow.

    Synopsis
    ========

    # Invoke data retention workflow using the `delete` strategy.
    cratedb-retention run --cutoff-day=2023-06-27 --strategy=delete "crate://localhost/"

    """  # noqa: E501


dryrun_option: t.Any = click.option(
    "--dry-run", type=bool, is_flag=True, default=False, help="Do not invoke SQL statements, but display them instead"
)
schema_option: t.Any = click.option(
    "--schema",
    type=str,
    required=False,
    envvar="CRATEDB_EXT_SCHEMA",
    help="Select schema where extension tables are created",
)

cutoff_date_option: t.Any = click.option(
    "--cutoff-day",
    type=str,
    required=False,
    help="Select cutoff date. Retire all data older than »cutoff date« minus "
    "»data retention duration«. The default value is 'today()'.",
)
strategy_option: t.Any = click.option(
    "--strategy",
    type=str,
    required=True,
    help="Which kind of retention strategy to apply",
)
identifier_option: t.Any = click.option(
    "--identifier", "--id", type=str, required=False, help="Identifier of retention policy"
)
tags_option: t.Any = click.option(
    "--tags", type=str, required=False, help="Tags for retention policy, used for grouping and filtering"
)


@click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool):
    return boot_click(ctx, verbose, debug)


@make_command(cli, "setup", help_setup)
@click.argument("dburi", envvar="CRATEDB_URI")
@dryrun_option
@schema_option
@click.pass_context
def setup(ctx: click.Context, dburi: str, dry_run: bool, schema: t.Optional[str]):
    if not dburi:
        logger.error("Unable to operate without database")
        sys.exit(1)

    # Create `JobSettings` instance, and configure schema name.
    # It is the single source of truth about configuration and runtime settings.
    settings = JobSettings(database=DatabaseAddress.from_string(dburi), dry_run=dry_run)
    if schema is not None:
        settings.policy_table.schema = schema

    # Install database schema.
    setup_schema(settings=settings)


@make_command(cli, "list-policies", help_list_policies, aliases=["list"])
@click.argument("dburi", envvar="CRATEDB_URI")
@schema_option
@click.pass_context
def list_policies(ctx: click.Context, dburi: str, schema: str):
    # Sanity checks.
    if not dburi:
        logger.error("Unable to operate without database")
        sys.exit(1)

    # Create `JobSettings` instance, and configure schema name.
    # It is the single source of truth about configuration and runtime settings.
    settings = JobSettings(
        database=DatabaseAddress.from_string(dburi),
    )
    if schema is not None:
        settings.policy_table.schema = schema

    # Set up adapter to retention policy store.
    store = RetentionPolicyStore(settings=settings)
    jd(store.retrieve())


@make_command(cli, "list-tags", help_list_tags)
@click.argument("dburi", envvar="CRATEDB_URI")
@schema_option
@click.pass_context
def list_tags(ctx: click.Context, dburi: str, schema: str):
    # Sanity checks.
    if not dburi:
        logger.error("Unable to operate without database")
        sys.exit(1)

    # Create `JobSettings` instance, and configure schema name.
    # It is the single source of truth about configuration and runtime settings.
    settings = JobSettings(
        database=DatabaseAddress.from_string(dburi),
    )
    if schema is not None:
        settings.policy_table.schema = schema

    # Set up adapter to retention policy store.
    store = RetentionPolicyStore(settings=settings)
    jd(store.retrieve_tags())


@make_command(cli, "create-policy", help_create_policy, aliases=["create", "add"])
@click.argument("dburi", envvar="CRATEDB_URI")
@schema_option
@click_options_from_dataclass(RetentionPolicy)
@click.pass_context
def create_policy(ctx: click.Context, dburi: str, schema: str, **kwargs):
    # Sanity checks.
    if "strategy" not in kwargs or kwargs["strategy"] is None:
        raise ValueError("--strategy option is required")

    # TODO: Converge to Click converters.
    kwargs["strategy"] = RetentionStrategy(kwargs["strategy"].upper())
    kwargs["tags"] = split_list(kwargs["tags"])

    # Sanity checks.
    if not dburi:
        logger.error("Unable to operate without database")
        sys.exit(1)

    # Create `JobSettings` instance, and configure schema name.
    # It is the single source of truth about configuration and runtime settings.
    settings = JobSettings(
        database=DatabaseAddress.from_string(dburi),
    )
    if schema is not None:
        settings.policy_table.schema = schema

    # Set up adapter to retention policy store.
    store = RetentionPolicyStore(settings=settings)

    # Create a retention policy record.
    policy = RetentionPolicy(**kwargs)
    identifier = store.create(policy)
    logger.info(f"Created new retention policy: {identifier}")


@make_command(cli, "delete-policy", help_delete_policy, aliases=["del", "rm"])
@click.argument("dburi", envvar="CRATEDB_URI")
@schema_option
@identifier_option
@tags_option
@click.pass_context
def delete_policy(ctx: click.Context, dburi: str, schema: str, identifier: str, tags: str):
    # TODO: Converge to Click converters.
    tag_list = set(split_list(tags))

    # Sanity checks.
    if not dburi:
        logger.error("Unable to operate without database")
        sys.exit(1)

    # Create `JobSettings` instance, and configure schema name.
    # It is the single source of truth about configuration and runtime settings.
    settings = JobSettings(
        database=DatabaseAddress.from_string(dburi),
    )
    if schema is not None:
        settings.policy_table.schema = schema

    # Set up adapter to retention policy store.
    store = RetentionPolicyStore(settings=settings)

    # Create a retention policy record.
    if identifier:
        store.delete(identifier=identifier)
    elif tags:
        store.delete_by_all_tags(tags=tag_list)
    else:
        message = "Unable to obtain deletion constraint"
        logger.error(message)
        raise ClickException(message)


@make_command(cli, "run", help_run)
@click.argument("dburi", envvar="CRATEDB_URI")
@dryrun_option
@schema_option
@cutoff_date_option
@strategy_option
@tags_option
@click.pass_context
def run(
    ctx: click.Context, dburi: str, cutoff_day: str, strategy: str, tags: str, dry_run: bool, schema: t.Optional[str]
):
    # TODO: Converge to Click converters.
    strategy_type = RetentionStrategy(strategy.upper())
    tag_list = set(split_list(tags))

    if not dburi:
        logger.error("Unable to operate without database")
        sys.exit(1)

    # Create `JobSettings` instance.
    # It is the single source of truth about configuration and runtime settings.
    settings = JobSettings(
        database=DatabaseAddress.from_string(dburi),
        strategy=strategy_type,
        tags=tag_list,
        cutoff_day=cutoff_day,
        dry_run=dry_run,
    )
    if schema is not None:
        settings.policy_table.schema = schema

    # Invoke the data retention job.
    job = RetentionJob(settings=settings)
    job.start()
