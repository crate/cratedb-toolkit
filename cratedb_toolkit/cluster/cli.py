import contextlib
import json
import logging
import sys

import click
import yaml
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
    Display general cluster information.

    ctk cluster info
    ctk cluster info --cluster-id=e1e38d92-a650-48f1-8a70-8133f2d5c400
    croud clusters get e1e38d92-a650-48f1-8a70-8133f2d5c400 --format=json
    """


def help_start():
    """
    Start or resume cluster.

    ctk cluster start
    ctk cluster start --cluster-id=e1e38d92-a650-48f1-8a70-8133f2d5c400
    ctk cluster start --cluster-name=hotzenplotz
    """


def help_stop():
    """
    Stop (suspend) cluster.

    ctk cluster stop
    ctk cluster stop --cluster-id=e1e38d92-a650-48f1-8a70-8133f2d5c400
    ctk cluster stop --cluster-name=hotzenplotz
    """


def help_list_jobs():
    """
    List jobs on cluster.

    ctk cluster list-jobs
    croud clusters import-jobs
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
    Display general cluster information.
    """
    with handle_command_errors("inquire cluster info"):
        cluster_info = ClusterInformation.from_id_or_name(cluster_id=cluster_id, cluster_name=cluster_name)
        jd(cluster_info.asdict())


@make_command(cli, name="health")
@option_cluster_id
@option_cluster_name
@click.pass_context
def health(ctx: click.Context, cluster_id: str, cluster_name: str):
    """
    Display cluster health information.
    """
    with handle_command_errors("inquire cluster health"):
        cluster_info = ClusterInformation.from_id_or_name(cluster_id=cluster_id, cluster_name=cluster_name)
        jd(cluster_info.health)


@make_command(cli, name="ping")
@option_cluster_id
@option_cluster_name
@click.pass_context
def ping(ctx: click.Context, cluster_id: str, cluster_name: str):
    """
    Ping cluster: API and database.
    """
    with handle_command_errors("ping cluster"):
        cluster = ManagedCluster(cluster_id=cluster_id, cluster_name=cluster_name).start()
        has_db_result = bool(cluster.query("SELECT 42 AS availability_check  -- ctk"))

        # Ensure cluster.info is present.
        if not cluster.info:
            raise CroudException("Unable to retrieve cluster information")

        response = {
            "meta": cluster.info.meta,
            "cloud": cluster.info.ready,
            "database": has_db_result,
        }
        jd(response)
        sys.exit(0 if (cluster.info.ready and has_db_result) else 1)


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


@make_command(cli, name="stop", help=help_stop)
@option_cluster_id
@option_cluster_name
@click.pass_context
def stop(ctx: click.Context, cluster_id: str, cluster_name: str):
    """
    Stop (suspend) CrateDB Cloud Cluster.
    """

    with handle_command_errors("stop cluster"):
        # Acquire the database cluster handle and submit the `suspend` command.
        cluster = ManagedCluster(cluster_id=cluster_id, cluster_name=cluster_name).probe().stop()
        logger.info(f"Successfully suspended cluster: {cluster}")

        # Display cluster information.
        jd(cluster.info.asdict())


@make_command(cli, name="list-jobs", help=help_list_jobs)
@option_cluster_id
@option_cluster_name
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "yaml"], case_sensitive=False),
    default="json",
    required=False,
    help="The output format for the result of the operation",
)
@click.pass_context
def list_jobs(ctx: click.Context, cluster_id: str, cluster_name: str, output_format: str):
    """
    List jobs on cluster.
    """

    with handle_command_errors("list cluster jobs"):
        # Acquire the database cluster handle and acquire job information.
        cluster = ManagedCluster(cluster_id=cluster_id, cluster_name=cluster_name).probe()
        if not cluster.operation:
            raise CroudException("Cluster does not support job operations")
        data = cluster.operation.list_jobs()

        # Display job information.
        if output_format == "json":
            print(json.dumps(data, indent=2), file=sys.stdout)  # noqa: T201
        elif output_format == "yaml":
            print(yaml.dump(data), file=sys.stdout)  # noqa: T201


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
