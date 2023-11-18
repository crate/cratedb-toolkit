import logging
import sys

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit import ManagedCluster
from cratedb_toolkit.options import option_cluster_id, option_cluster_name
from cratedb_toolkit.cluster.model import ClusterInformation
from cratedb_toolkit.exception import CroudException
from cratedb_toolkit.util.cli import boot_click, make_command
from cratedb_toolkit.util.data import jd

logger = logging.getLogger(__name__)


@click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool):
    """
    Run cluster operations on CrateDB and CrateDB Cloud.
    """
    return boot_click(ctx, verbose, debug)


@make_command(cli, name="info")
@option_cluster_id
@option_cluster_name
@click.pass_context
def info(ctx: click.Context, cluster_id: str, cluster_name: str):
    """
    Display CrateDB Cloud Cluster information

    ctk cluster info
    ctk cluster info --cluster-id=e1e38d92-a650-48f1-8a70-8133f2d5c400
    croud clusters get e1e38d92-a650-48f1-8a70-8133f2d5c400 --format=json
    """
    cluster_info = ClusterInformation.from_id_or_name(cluster_id=cluster_id, cluster_name=cluster_name)
    try:
        jd(cluster_info.asdict())

    # When exiting so, it is expected that error logging has taken place appropriately.
    except CroudException:
        sys.exit(1)


@make_command(cli, name="start")
@option_cluster_id
@option_cluster_name
@click.pass_context
def start(ctx: click.Context, cluster_id: str, cluster_name: str):
    """
    Display CrateDB Cloud Cluster information

    ctk cluster start
    ctk cluster start --cluster-id=e1e38d92-a650-48f1-8a70-8133f2d5c400
    ctk cluster start --cluster-name=Hotzenplotz
    """

    # Acquire database cluster handle.
    cluster = ManagedCluster(id=cluster_id, name=cluster_name).start()
    logger.info(f"Successfully acquired cluster: {cluster}")

    # Output cluster information.
    try:
        jd(cluster.info.asdict())

    # When exiting so, it is expected that error logging has taken place appropriately.
    except CroudException:
        sys.exit(1)
