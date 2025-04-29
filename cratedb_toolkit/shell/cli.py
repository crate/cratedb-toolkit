import click

from cratedb_toolkit import DatabaseCluster
from cratedb_toolkit.option import (
    option_cluster_id,
    option_cluster_name,
    option_http_url,
    option_password,
    option_schema,
    option_sqlalchemy_url,
    option_username,
)
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
@option_http_url
@option_username
@option_password
@option_schema
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
    cluster_name: str,
    cratedb_sqlalchemy_url: str,
    cratedb_http_url: str,
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
        sqlalchemy_url=cratedb_sqlalchemy_url,
        http_url=cratedb_http_url,
    ).probe()

    if cluster.address is None:
        raise click.UsageError("Inquiring cluster address failed.")

    cratedb_http_url = cluster.address.httpuri

    run_crash(
        hosts=cratedb_http_url,
        username=username,
        password=password,
        schema=schema,
        command=command,
        output_format=format_,
    )
