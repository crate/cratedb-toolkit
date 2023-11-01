import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.util.cli import boot_click

from .io.cli import cli as io_cli

@click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool):
    return boot_click(ctx, verbose, debug)
cli.add_command(io_cli, name="load")
