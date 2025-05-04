import logging

import click

from cratedb_toolkit import DatabaseCluster
from cratedb_toolkit.option import (
    option_cluster_id,
    option_cluster_name,
    option_cluster_url,
    option_password,
    option_schema,
    option_username,
)
from cratedb_toolkit.util.cli import boot_click, docstring_format_verbatim
from cratedb_toolkit.util.crash import get_crash_output_formats, run_crash

logger = logging.getLogger(__name__)

output_formats = get_crash_output_formats()


def help_cli():
    """
    Start an interactive database shell, or invoke SQL commands.

    The API wrapper uses `crash` under the hood, provides a subset of its features,
    but a more convenient interface, specifically when using CrateDB Cloud.

    See also:
    - https://cratedb-toolkit.readthedocs.io/util/shell.html
    - https://cratedb.com/docs/crate/crash/

    TODO: Implement/forward more options of `crash`.
    """


@click.command(help=docstring_format_verbatim(help_cli.__doc__))
@option_cluster_id
@option_cluster_name
@option_cluster_url
@option_username
@option_password
@option_schema
@click.option("--command", "-c", type=str, required=False, help="SQL command")
@click.option(
    "--format",
    "format_",
    type=click.Choice(output_formats, case_sensitive=False),
    required=False,
    help="The output format of the database response",
)
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.version_option()
@click.pass_context
def cli(
    ctx: click.Context,
    cluster_id: str,
    cluster_name: str,
    cluster_url: str,
    username: str,
    password: str,
    schema: str,
    command: str,
    format_: str,
    verbose: bool,
    debug: bool,
):
    """
    Start an interactive database shell or invoke SQL commands, using `crash`.
    """
    boot_click(ctx, verbose, debug)

    cluster = DatabaseCluster.create(
        cluster_id=cluster_id,
        cluster_name=cluster_name,
        cluster_url=cluster_url,
    )

    if cluster.address is None:
        raise click.UsageError("Inquiring cluster address failed.")

    is_cloud = cluster_id is not None or cluster_name is not None
    jwt_token = None
    if is_cloud:
        if username is not None:
            logger.info("Using username/password credentials for authentication")
        else:
            logger.info("Using JWT token for authentication")
            jwt_token_obj = getattr(cluster.info, "jwt", None)
            if jwt_token_obj:
                jwt_token = jwt_token_obj.token

    run_crash(
        hosts=cluster.address.httpuri,
        username=username,
        password=password,
        jwt_token=jwt_token,
        schema=schema,
        command=command,
        output_format=format_,
    )
