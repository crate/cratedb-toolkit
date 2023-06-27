# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.

import logging
import typing as t

import click

from cratedb_retentions.util.cli import boot_click, docstring_format_verbatim

logger = logging.getLogger(__name__)


def help_run():
    """
    Invoke data retention workflow.

    Synopsis
    ========

    # Invoke data retention workflow using the `delete` strategy.
    cratedb-retentions run --strategy=delete "crate://localhost/testdrive/data"

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
    "run",
    help=docstring_format_verbatim(help_run.__doc__),
    context_settings={"max_content_width": 120},
)
@click.argument("dburi", nargs=-1)
@click.option("--strategy", type=str, required=True, help="Turn on logging")
@click.pass_context
def run(ctx: click.Context, dburi: t.Tuple[str], strategy: str):
    logger.info(f"Starting data retention with strategy '{strategy}' on: {dburi}")
