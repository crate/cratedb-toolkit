# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.

import logging
import sys

import click

from cratedb_retentions.setup.schema import setup_schema
from cratedb_retentions.strategy.delete import run_delete_job
from cratedb_retentions.util.cli import boot_click, docstring_format_verbatim

logger = logging.getLogger(__name__)


def help_setup():
    """
    Setup database schema for storing retention policies.

    Synopsis
    ========

    # Materialize `retention_policies` database schema.
    cratedb-retentions setup "crate://localhost/"

    """  # noqa: E501


def help_run():
    """
    Invoke data retention workflow.

    Synopsis
    ========

    # Invoke data retention workflow using the `delete` strategy.
    cratedb-retentions run --day=2023-06-27 --strategy=delete "crate://localhost/testdrive/data"

    """  # noqa: E501


@click.group()
@click.version_option(package_name="cratedb-retentions")
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
@click.pass_context
def setup(ctx: click.Context, dburi: str):
    if not dburi:
        logger.error("Unable to operate without database")
        sys.exit(1)
    logger.info(f"Installing retention policy bookkeeping tables at: {dburi}")
    setup_schema(dburi)


@cli.command(
    "run",
    help=docstring_format_verbatim(help_run.__doc__),
    context_settings={"max_content_width": 120},
)
@click.argument("dburi")
@click.option("--day", type=str, required=True, help="Select day parameter")
@click.option("--strategy", type=str, required=True, help="Select retention strategy")
@click.pass_context
def run(ctx: click.Context, dburi: str, day: str, strategy: str):
    strategy_choices = ["delete", "reallocate", "snapshot"]
    if not dburi:
        logger.error("Unable to operate without database")
        sys.exit(1)
    if strategy not in strategy_choices:
        logger.error(f"Unknown strategy. Select one of {strategy_choices}")
        sys.exit(1)
    logger.info(f"Starting data retention with strategy '{strategy}' up to day '{day}' on: {dburi}")
    if strategy == "delete":
        run_delete_job(dburi, day)
    else:
        raise NotImplementedError(f"Retention strategy {strategy} not implemented yet")
