import logging
import typing as t
from pathlib import Path

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.cluster.core import DatabaseCluster
from cratedb_toolkit.model import InputOutputResource, TableAddress
from cratedb_toolkit.option import option_cluster_id, option_cluster_name, option_cluster_url
from cratedb_toolkit.util.cli import boot_click, make_command

logger = logging.getLogger(__name__)


@click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool):
    """
    Load data into CrateDB.
    """
    return boot_click(ctx, verbose, debug)


@make_command(cli, name="table")
@click.argument("url")
@option_cluster_id
@option_cluster_name
@option_cluster_url
@click.option("--schema", envvar="CRATEDB_SCHEMA", type=str, required=False, help="Schema where to import the data")
@click.option("--table", envvar="CRATEDB_TABLE", type=str, required=False, help="Table where to import the data")
@click.option("--format", "format_", type=str, required=False, help="File format of the import resource")
@click.option("--compression", type=str, required=False, help="Compression format of the import resource")
@click.option("--transformation", type=Path, required=False, help="Path to Tikray transformation file")
@click.pass_context
def load_table(
    ctx: click.Context,
    url: str,
    cluster_id: str,
    cluster_name: str,
    cluster_url: str,
    schema: str,
    table: str,
    format_: str,
    compression: str,
    transformation: t.Union[Path, None],
):
    """
    Import data into CrateDB and CrateDB Cloud clusters.
    """

    # When `--transformation` is given, but empty, fix it.
    if transformation is not None and transformation.name == "":
        transformation = None

    # Encapsulate source and target parameters.
    source = InputOutputResource(url=url, format=format_, compression=compression)
    target = TableAddress(schema=schema, table=table)

    # Dispatch "load table" operation.
    cluster = DatabaseCluster.create(
        cluster_id=cluster_id,
        cluster_name=cluster_name,
        cluster_url=cluster_url,
    )
    cluster.load_table(source=source, target=target, transformation=transformation)
