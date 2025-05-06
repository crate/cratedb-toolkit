import click
from click import ClickException
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.options import option_cluster_id, option_cluster_name, option_http_url, option_sqlalchemy_url
from cratedb_toolkit.util.cli import boot_click


def make_cli():
    @click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
    @option_cluster_id
    @option_cluster_name
    @option_sqlalchemy_url
    @option_http_url
    @click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
    @click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
    @click.option("--scrub", envvar="SCRUB", is_flag=True, required=False, help="Blank out identifiable information")
    @click.version_option()
    @click.pass_context
    def cli(
        ctx: click.Context,
        cluster_id: str,
        cluster_name: str,
        cratedb_sqlalchemy_url: str,
        cratedb_http_url: str,
        verbose: bool,
        debug: bool,
        scrub: bool,
    ):
        """
        Diagnostics and informational utilities.
        """
        if not cratedb_sqlalchemy_url and not cratedb_http_url:
            raise ClickException("Unable to operate without database address")
        ctx.meta.update(
            {"cratedb_http_url": cratedb_http_url, "cratedb_sqlalchemy_url": cratedb_sqlalchemy_url, "scrub": scrub}
        )
        return boot_click(ctx, verbose, debug)

    return cli
