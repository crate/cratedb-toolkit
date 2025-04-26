import click

from cratedb_toolkit.cluster.model import ClusterInformation
from cratedb_toolkit.exception import DatabaseAddressMissingError
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.options import option_cluster_id, option_cluster_name, option_sqlalchemy_url
from cratedb_toolkit.util.cli import boot_click, docstring_format_verbatim
from cratedb_toolkit.util.crash import get_crash_output_formats, run_crash

output_formats = get_crash_output_formats()


def help_cli():
    """
    Start an interactive database shell, or invoke SQL commands.

    The API wrapper uses `crash` under the hood, provides a subset of its features,
    but a more convenient interface, specifically when using CrateDB Cloud.

    See also:
    - https://cratedb-toolkit.readthedocs.io/util/shell.html
    - https://cratedb.com/docs/crate/crash/

    TODO: Learn/forward more options of `crash`.
    """


@click.command(help=docstring_format_verbatim(help_cli.__doc__))
@option_cluster_id
@option_cluster_name
@option_sqlalchemy_url
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
    cratedb_sqlalchemy_url: str,
    cluster_id: str,
    cluster_name: str,
    username: str,
    password: str,
    schema: str,
    command: str,
    format_: str,
    verbose: bool,
    debug: bool,
):
    """
    Start an interactive database shell, or invoke SQL commands, using `crash`.
    """
    boot_click(ctx, verbose, debug)

    if cratedb_sqlalchemy_url:
        address = DatabaseAddress.from_string(cratedb_sqlalchemy_url)
        cratedb_http_url = address.httpuri
    elif cluster_id or cluster_name:
        cluster_info = ClusterInformation.from_id_or_name(cluster_id=cluster_id, cluster_name=cluster_name)
        cratedb_http_url = cluster_info.cloud["url"]
    else:
        raise DatabaseAddressMissingError()

    run_crash(
        hosts=cratedb_http_url,
        username=username,
        password=password,
        schema=schema,
        command=command,
        output_format=format_,
    )
