import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.util.cli import boot_click

from .adapter.rockset.cli import cli as rockset_cli
from .cfr.cli import cli as cfr_cli
from .cluster.cli import cli as cloud_cli
from .io.cli import cli as io_cli
from .job.cli import cli_list_jobs
from .shell.cli import cli as shell_cli
from .wtf.cli import cli as wtf_cli


@click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool):
    return boot_click(ctx, verbose, debug)


cli.add_command(cfr_cli, name="cfr")
cli.add_command(cloud_cli, name="cluster")
cli.add_command(io_cli, name="load")
cli.add_command(rockset_cli, name="rockset")
cli.add_command(shell_cli, name="shell")
cli.add_command(wtf_cli, name="wtf")
cli.add_command(cli_list_jobs)
