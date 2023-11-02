import click

from cratedb_toolkit.cluster.util import get_cluster_info
from cratedb_toolkit.util.cli import boot_click
from cratedb_toolkit.util.crash import get_crash_output_formats, run_crash

output_formats = get_crash_output_formats()


@click.command()
@click.option(
    "--cluster-id", envvar="CRATEDB_CLOUD_CLUSTER_ID", type=str, required=True, help="CrateDB Cloud cluster identifier"
)
@click.option("--username", envvar="CRATEDB_USERNAME", type=str, required=False, help="Username for CrateDB cluster")
@click.option("--password", envvar="CRATEDB_PASSWORD", type=str, required=False, help="Password for CrateDB cluster")
@click.option(
    "--schema",
    envvar="CRATEDB_SCHEMA",
    type=str,
    required=False,
    help="Default schema for statements if schema is not explicitly stated within queries",
)
@click.option("--command", type=str, required=False, help="SQL command")
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
    username: str,
    password: str,
    schema: str,
    command: str,
    format_: str,
    verbose: bool,
    debug: bool,
):
    """
    Start an interactive database shell, or invoke SQL commands.

    TODO: Only talks to CrateDB Cloud for now. Also implement for standalone CrateDB servers.
    TODO: Learn/forward more options of `crash`.
    """
    boot_click(ctx, verbose, debug)

    cluster_info = get_cluster_info(cluster_id=cluster_id)
    cratedb_http_url = cluster_info.cloud["url"]

    run_crash(
        hosts=cratedb_http_url,
        username=username,
        password=password,
        schema=schema,
        command=command,
        output_format=format_,
    )
