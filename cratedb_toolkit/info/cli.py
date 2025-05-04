# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging

import click

from cratedb_toolkit import DatabaseCluster
from cratedb_toolkit.info.core import InfoContainer, JobInfoContainer, LogContainer
from cratedb_toolkit.util.app import make_cli
from cratedb_toolkit.util.cli import make_command
from cratedb_toolkit.util.data import jd

logger = logging.getLogger(__name__)


def help_cluster():
    """
    Database cluster and system information.

    Synopsis
    ========

    export CRATEDB_CLUSTER_URL=crate://localhost/
    ctk info cluster

    """  # noqa: E501


def help_logs():
    """
    Database cluster logs.

    Synopsis
    ========

    export CRATEDB_CLUSTER_URL=crate://localhost/
    ctk info logs

    """  # noqa: E501


def help_serve():
    """
    Start HTTP service to expose collected information.

    Synopsis
    ========

    export CRATEDB_CLUSTER_URL=crate://localhost/
    ctk info serve

    """  # noqa: E501


cli = make_cli()


@make_command(cli, "cluster", help_cluster)
@click.pass_context
def cluster(ctx: click.Context):
    scrub = ctx.meta.get("scrub", False)
    dc = DatabaseCluster.from_options(ctx.meta["address"])
    sample = InfoContainer(adapter=dc.adapter, scrub=scrub)
    jd(sample.to_dict())


@make_command(cli, "logs", help_logs)
@click.pass_context
def logs(ctx: click.Context):
    scrub = ctx.meta.get("scrub", False)
    dc = DatabaseCluster.from_options(ctx.meta["address"])
    sample = LogContainer(adapter=dc.adapter, scrub=scrub)
    jd(sample.to_dict())


@make_command(cli, "jobs", "Display information about jobs / queries.")
@click.pass_context
def job_information(ctx: click.Context):
    """
    Display ad hoc job information.
    """
    scrub = ctx.meta.get("scrub", False)
    dc = DatabaseCluster.from_options(ctx.meta["address"])
    sample = JobInfoContainer(adapter=dc.adapter, scrub=scrub)
    jd(sample.to_dict())


@make_command(cli, "serve", help_serve)
@click.option("--listen", type=click.STRING, default=None, help="HTTP server listen address")
@click.option("--reload", is_flag=True, help="Dynamically reload changed files")
@click.pass_context
def serve(ctx: click.Context, listen: str, reload: bool):
    from cratedb_toolkit.info.http import start

    start(listen, reload=reload)
