import logging
import typing as t
from pathlib import Path

import click

from cratedb_toolkit.cluster.core import DatabaseCluster
from cratedb_toolkit.model import InputOutputResource, TableAddress
from cratedb_toolkit.option import option_cluster_id, option_cluster_name, option_cluster_url
from cratedb_toolkit.util.cli import boot_click

logger = logging.getLogger(__name__)


@click.command()
@click.argument("source_url")
@click.argument("target_url", required=False)
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@option_cluster_id
@option_cluster_name
@option_cluster_url
@click.option("--schema", envvar="CRATEDB_SCHEMA", type=str, required=False, help="Schema where to import the data")
@click.option("--table", envvar="CRATEDB_TABLE", type=str, required=False, help="Table where to import the data")
@click.option("--format", "format_", type=str, required=False, help="File format of the import resource")
@click.option("--compression", type=str, required=False, help="Compression format of the import resource")
@click.option("--transformation", type=Path, required=False, help="Path to Tikray transformation file")
@click.pass_context
def cli_load(
    ctx: click.Context,
    source_url: str,
    target_url: str,
    cluster_id: str,
    cluster_name: str,
    cluster_url: str,
    schema: str,
    table: str,
    format_: str,
    compression: str,
    transformation: t.Union[Path, None],
    verbose: bool,
    debug: bool,
):
    """
    Load data into CrateDB.
    """

    boot_click(ctx, verbose, debug)

    # API evolution adjustments.
    if target_url and cluster_url and target_url != cluster_url:
        raise click.UsageError("Specify cluster URL either as TARGET_URL or --cluster-url, not both.")
    if target_url:
        cluster_url = target_url

    # When `--transformation` is given, but empty, fix it.
    if transformation is not None and transformation.name == "":
        transformation = None

    # Encapsulate source and target parameters.
    source = InputOutputResource(url=source_url, format=format_, compression=compression)
    target = TableAddress(schema=schema, table=table)

    # Dispatch "load" operation.
    cluster = DatabaseCluster.create(
        cluster_id=cluster_id,
        cluster_name=cluster_name,
        cluster_url=cluster_url,
    )
    cluster.load_table(source=source, target=target, transformation=transformation)


@click.command()
@click.argument("source_url", required=False)
@click.argument("target_url", required=False)
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@option_cluster_id
@option_cluster_name
@option_cluster_url
@click.option("--format", "format_", type=str, required=False, help="File format of the export resource")
@click.option("--compression", type=str, required=False, help="Compression format of the export resource")
@click.option("--transformation", type=Path, required=False, help="Path to Tikray transformation file")
@click.pass_context
def cli_save(
    ctx: click.Context,
    source_url: t.Optional[str],
    target_url: t.Optional[str],
    cluster_id: str,
    cluster_name: str,
    cluster_url: str,
    format_: str,
    compression: str,
    transformation: t.Union[Path, None],
    verbose: bool,
    debug: bool,
):
    """
    Export data from CrateDB.
    """

    boot_click(ctx, verbose, debug)

    # API evolution adjustments.
    if source_url and not target_url:
        target_url = source_url
        source_url = None
    if source_url and cluster_url and source_url != cluster_url:
        raise click.UsageError("Specify cluster URL either as SOURCE_URL or --cluster-url, not both.")
    if source_url:
        cluster_url = source_url
    if not target_url:
        raise click.UsageError(
            "Missing TARGET_URL. Use `ctk save <target-url>` or `ctk save <cluster-url> <target-url>`."
        )

    # When `--transformation` is given, but empty, fix it.
    if transformation is not None and transformation.name == "":
        transformation = None

    # Encapsulate source and target parameters.
    target = InputOutputResource(url=target_url, format=format_, compression=compression)

    # Dispatch "save" operation.
    cluster = DatabaseCluster.create(
        cluster_id=cluster_id,
        cluster_name=cluster_name,
        cluster_url=cluster_url,
    )
    cluster.save_table(target=target, transformation=transformation)
