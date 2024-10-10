import logging

import click
from click_aliases import ClickAliasedGroup

from ..util.cli import boot_click
from .convert.cli import convert_query

logger = logging.getLogger(__name__)


@click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool):
    """
    Query expression utilities.
    """
    return boot_click(ctx, verbose, debug)


cli.add_command(convert_query, name="convert")
