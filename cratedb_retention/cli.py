# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import sys
import typing as t

import click

from cratedb_retention.core import RetentionJob
from cratedb_retention.model import DatabaseAddress, JobSettings, RetentionStrategy
from cratedb_retention.setup.schema import setup_schema
from cratedb_retention.util.cli import boot_click, docstring_format_verbatim

logger = logging.getLogger(__name__)


def help_setup():
    """
    Setup database schema for storing retention policies.

    Synopsis
    ========

    # Set up the retention policy database table schema.
    cratedb-retention setup "crate://localhost/"

    """  # noqa: E501


def help_run():
    """
    Invoke data retention workflow.

    Synopsis
    ========

    # Invoke data retention workflow using the `delete` strategy.
    cratedb-retention run --cutoff-day=2023-06-27 --strategy=delete "crate://localhost/testdrive/data"

    """  # noqa: E501


schema_option = click.option(
    "--schema",
    type=str,
    required=False,
    envvar="CRATEDB_EXT_SCHEMA",
    help="Select schema where extension tables are created",
)


@click.group()
@click.version_option(package_name="cratedb-retention")
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool):
    verbose = True
    return boot_click(ctx, verbose, debug)


@cli.command(
    "setup",
    help=docstring_format_verbatim(help_setup.__doc__),
    context_settings={"max_content_width": 120},
)
@click.argument("dburi")
@schema_option
@click.pass_context
def setup(ctx: click.Context, dburi: str, schema: t.Optional[str]):
    if not dburi:
        logger.error("Unable to operate without database")
        sys.exit(1)

    # Create `JobSettings` instance.
    # It is the single source of truth about configuration and runtime settings.
    settings = JobSettings(database=DatabaseAddress.from_string(dburi))
    if schema is not None:
        settings.policy_table.schema = schema

    # Install database schema.
    setup_schema(settings=settings)


@cli.command(
    "run",
    help=docstring_format_verbatim(help_run.__doc__),
    context_settings={"max_content_width": 120},
)
@click.argument("dburi")
@click.option("--cutoff-day", type=str, required=True, help="Select day parameter")
@click.option("--strategy", type=str, required=True, help="Select retention strategy")
@schema_option
@click.pass_context
def run(ctx: click.Context, dburi: str, cutoff_day: str, strategy: str, schema: t.Optional[str]):
    strategy = strategy.upper()
    strategy_choices = ["DELETE", "REALLOCATE", "SNAPSHOT"]
    if not dburi:
        logger.error("Unable to operate without database")
        sys.exit(1)
    if strategy not in strategy_choices:
        logger.error(f"Unknown strategy. Select one of {strategy_choices}")
        sys.exit(1)

    # Create `JobSettings` instance.
    # It is the single source of truth about configuration and runtime settings.
    settings = JobSettings(
        database=DatabaseAddress.from_string(dburi),
        strategy=RetentionStrategy(strategy),
        cutoff_day=cutoff_day,
    )
    if schema is not None:
        settings.policy_table.schema = schema

    # Invoke the data retention job.
    job = RetentionJob(settings=settings)
    job.start()
