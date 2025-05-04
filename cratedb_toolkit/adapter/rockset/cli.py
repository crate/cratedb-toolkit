# Copyright (c) 2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import sys

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.option import option_cluster_url
from cratedb_toolkit.util.cli import boot_click, make_command

logger = logging.getLogger()


def help_serve():
    """
    Start HTTP service, mocking a subset of the Rockset API.

    Synopsis
    ========

    export CRATEDB_CLUSTER_URL=crate://localhost/
    ctk rockset serve

    """  # noqa: E501


@click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
@option_cluster_url
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, cluster_url: str, verbose: bool, debug: bool):
    """
    Rockset adapter utilities.
    """
    if not cluster_url:
        logger.error("Unable to operate without database address")
        sys.exit(1)
    ctx.meta.update({"cluster_url": cluster_url})
    return boot_click(ctx, verbose, debug)


@make_command(cli, "serve", help_serve)
@click.option("--listen", type=click.STRING, default=None, help="HTTP server listen address")
@click.option("--reload", is_flag=True, help="Dynamically reload changed files")
@click.pass_context
def serve(ctx: click.Context, listen: str, reload: bool):
    from cratedb_toolkit.adapter.rockset.server.main import start

    start(listen, reload=reload)
