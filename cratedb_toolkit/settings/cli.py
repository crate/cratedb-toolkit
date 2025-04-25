import logging

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.docs.cli import settings as docs_settings_cli
from cratedb_toolkit.util.cli import boot_click

from .compare import compare_cluster_settings

logger = logging.getLogger(__name__)


@click.group(cls=ClickAliasedGroup)
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool):
    """
    Tools for working with CrateDB's settings.
    """
    return boot_click(ctx, verbose, debug)


cli.add_command(compare_cluster_settings, name="compare")
cli.add_command(docs_settings_cli, name="list")
