import logging
import sys

import click

from cratedb_toolkit.cmd.tail.main import TableTailer
from cratedb_toolkit.model import TableAddress
from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.util.cli import boot_click

logger = logging.getLogger(__name__)

cratedb_sqlalchemy_option = click.option(
    "--cratedb-sqlalchemy-url", envvar="CRATEDB_SQLALCHEMY_URL", type=str, required=False, help="CrateDB SQLAlchemy URL"
)


@click.command()
@cratedb_sqlalchemy_option
@click.option(
    "--lines", "-n", type=int, required=False, default=10, help="Displays n last lines of the input. Default: 10"
)
@click.option("--format", "format_", type=str, required=False, help="Select output format. Default: log / jsonl")
@click.option("--follow", "-f", is_flag=True, required=False, help="Follow new records added, by polling the table")
@click.option(
    "--interval", "-i", type=float, required=False, help="When following the tail, poll each N seconds. Default: 0.5"
)
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.argument("resource", nargs=-1, type=click.UNPROCESSED)
@click.version_option()
@click.pass_context
def cli(
    ctx: click.Context,
    cratedb_sqlalchemy_url: str,
    resource: str,
    lines: int,
    format_: str,
    follow: bool,
    interval: float,
    verbose: bool,
    debug: bool,
):
    """
    A polling tail implementation for database tables.
    """
    if not cratedb_sqlalchemy_url:
        logger.error("Unable to operate without database address")
        sys.exit(1)
    boot_click(ctx, verbose, debug)
    adapter = DatabaseAdapter(dburi=cratedb_sqlalchemy_url)
    # TODO: Tail multiple tables.
    if len(resource) > 1:
        raise NotImplementedError("`ctk tail` currently implements tailing a single table only")
    tt = TableTailer(db=adapter, resource=TableAddress.from_string(resource[0]), interval=interval, format=format_)
    tt.start(lines=lines, follow=follow)
