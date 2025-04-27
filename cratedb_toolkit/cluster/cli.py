import contextlib
import logging

import click
from click import ClickException
from click_aliases import ClickAliasedGroup

from cratedb_toolkit import ManagedCluster
from cratedb_toolkit.cluster.model import ClusterInformation
from cratedb_toolkit.exception import CroudException
from cratedb_toolkit.option import option_cluster_id, option_cluster_name
from cratedb_toolkit.util.cli import boot_click, make_command
from cratedb_toolkit.util.data import jd

logger = logging.getLogger(__name__)


def help_info():
    """
    Display information about cluster.

    ctk cluster info
    ctk cluster info --cluster-id=e1e38d92-a650-48f1-8a70-8133f2d5c400
    croud clusters get e1e38d92-a650-48f1-8a70-8133f2d5c400 --format=json
    """


def help_start():
    """
    Start or resume cluster.

    ctk cluster start
    ctk cluster start --cluster-id=e1e38d92-a650-48f1-8a70-8133f2d5c400
    ctk cluster start --cluster-name=Hotzenplotz
    """


def help_suspend():
    """
    Suspend cluster.

    ctk cluster suspend
    ctk cluster suspend --cluster-id=e1e38d92-a650-48f1-8a70-8133f2d5c400
    ctk cluster suspend --cluster-name=Hotzenplotz
    """


@click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool):
    """
    Run operations on CrateDB and CrateDB Cloud database clusters.
    """
    return boot_click(ctx, verbose, debug)


@make_command(cli, name="info", help=help_info)
@option_cluster_id
@option_cluster_name
@click.pass_context
def info(ctx: click.Context, cluster_id: str, cluster_name: str):
    """
    Display CrateDB Cloud Cluster information.
    """
    with handle_command_errors("inquire cluster info"):
        cluster_info = ClusterInformation.from_id_or_name(cluster_id=cluster_id, cluster_name=cluster_name)
        jd(cluster_info.asdict())


@make_command(cli, name="start", help=help_start)
@option_cluster_id
@option_cluster_name
@click.pass_context
def start(ctx: click.Context, cluster_id: str, cluster_name: str):
    """
    Start or resume a CrateDB Cloud Cluster.
    """

    with handle_command_errors("start cluster"):
        # Acquire the database cluster handle and submit the `start` command.
        cluster = ManagedCluster(cluster_id=cluster_id, cluster_name=cluster_name).start()
        logger.info(f"Successfully acquired cluster: {cluster}")

        # Display cluster information.
        jd(cluster.info.asdict())


@make_command(cli, name="suspend", help=help_suspend)
@option_cluster_id
@option_cluster_name
@click.pass_context
def suspend(ctx: click.Context, cluster_id: str, cluster_name: str):
    """
    Suspend CrateDB Cloud Cluster.
    """

    with handle_command_errors("suspend cluster"):
        # Acquire the database cluster handle and submit the `suspend` command.
        cluster = ManagedCluster(cluster_id=cluster_id, cluster_name=cluster_name).probe().suspend()
        logger.info(f"Successfully suspended cluster: {cluster}")

        # Display cluster information.
        jd(cluster.info.asdict())


@contextlib.contextmanager
def handle_command_errors(operation_name):
    """Handle common command errors and exit with appropriate error messages."""
    try:
        yield
    except CroudException as ex:
        logger.error(
            f"Failed to {operation_name}. "
            f"Please check if you are addressing the right cluster, "
            f"and if credentials and permissions are valid. "
            f"The underlying error was: {ex}"
        )
        raise
    except ClickException:
        raise
    except Exception as ex:
        logger.exception(f"Unexpected error on operation: {operation_name}")
        raise SystemExit(1) from ex
