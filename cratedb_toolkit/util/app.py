import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.exception import DatabaseAddressDuplicateError, DatabaseAddressMissingError
from cratedb_toolkit.option import option_cluster_id, option_cluster_name, option_http_url, option_sqlalchemy_url
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
        Generic CrateDB API client.

        TODO: Use `UniversalCluster.{create,validate}` instead.
        """

        # Check if at least one address option was provided.
        if not cluster_id and not cluster_name and not cratedb_sqlalchemy_url and not cratedb_http_url:
            raise click.UsageError(DatabaseAddressMissingError.EXTENDED_MESSAGE)

        # Fail if more than one address option was provided.
        address_options_count = sum(
            1
            for option in [cluster_id, cluster_name, cratedb_sqlalchemy_url, cratedb_http_url]
            if option and option.strip()
        )
        if address_options_count > 1:
            raise click.UsageError(DatabaseAddressDuplicateError.STANDARD_MESSAGE)

        ctx.meta.update(
            {
                "cluster_id": cluster_id,
                "cluster_name": cluster_name,
                "cratedb_http_url": cratedb_http_url,
                "cratedb_sqlalchemy_url": cratedb_sqlalchemy_url,
                "scrub": scrub,
            }
        )
        return boot_click(ctx, verbose, debug)

    return cli
