# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.model import ClusterAddressOptions
from cratedb_toolkit.option import option_cluster_id, option_cluster_name, option_cluster_url
from cratedb_toolkit.util.cli import boot_click


def make_cli():
    @click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
    @option_cluster_id
    @option_cluster_name
    @option_cluster_url
    @click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
    @click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
    @click.option("--scrub", envvar="SCRUB", is_flag=True, required=False, help="Blank out identifiable information")
    @click.version_option()
    @click.pass_context
    def cli(
        ctx: click.Context,
        cluster_id: str,
        cluster_name: str,
        cluster_url: str,
        verbose: bool,
        debug: bool,
        scrub: bool,
    ):
        """
        Generic CrateDB API client.
        """

        # Read address parameters.
        address_options = ClusterAddressOptions.from_params(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            cluster_url=cluster_url,
        )
        ctx.meta.update(
            {
                "address": address_options,
                "scrub": scrub,
                # TODO: Remove individual options, use `address` instead.
                "cluster_id": cluster_id,
                "cluster_name": cluster_name,
                "cluster_url": cluster_url,
            }
        )
        return boot_click(ctx, verbose, debug)

    return cli
