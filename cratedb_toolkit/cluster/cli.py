import sys

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.cluster.util import get_cluster_info
from cratedb_toolkit.exception import CroudException
from cratedb_toolkit.util import jd
from cratedb_toolkit.util.cli import boot_click, make_command


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
@click.option(
    "--cluster-id", envvar="CRATEDB_CLOUD_CLUSTER_ID", type=str, required=True, help="CrateDB Cloud cluster identifier"
)
@click.pass_context
def info(ctx: click.Context, cluster_id: str):
    """
    Display CrateDB Cloud Cluster information

    ctk cluster info
    ctk cluster info --cluster-id=e1e38d92-a650-48f1-8a70-8133f2d5c400
    croud clusters get e1e38d92-a650-48f1-8a70-8133f2d5c400 --format=json
    """
    cluster_info = get_cluster_info(cluster_id=cluster_id)
    try:
        jd(cluster_info.asdict())

    # When exiting so, it is expected that error logging has taken place appropriately.
    except CroudException:
        sys.exit(1)
