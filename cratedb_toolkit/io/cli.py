import logging
import typing as t
from pathlib import Path

import click

from cratedb_toolkit.cluster.core import DatabaseCluster
from cratedb_toolkit.model import InputOutputResource, TableAddress
from cratedb_toolkit.option import option_cluster_id, option_cluster_name, option_cluster_url
from cratedb_toolkit.util.app import make_cli
from cratedb_toolkit.util.cli import make_command

logger = logging.getLogger(__name__)


cli = make_cli()
cli.help = "Load data into CrateDB."


@make_command(cli, name="table")
@click.argument("url")
@option_cluster_id
@option_cluster_name
@option_cluster_url
@click.option("--schema", envvar="CRATEDB_SCHEMA", type=str, required=False, help="Schema where to import the data")
@click.option("--table", envvar="CRATEDB_TABLE", type=str, required=False, help="Table where to import the data")
@click.option("--format", "format_", type=str, required=False, help="File format of the import resource")
@click.option("--compression", type=str, required=False, help="Compression format of the import resource")
@click.option("--transformation", type=Path, required=False, help="Path to Zyp transformation file")
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

    logger.info(f"Importing table succeeded. source={source}, target={target}")


@make_command(cli, name="dataset")
@click.argument("name", envvar="DATASET_NAME", type=str)
@click.option("--schema", envvar="CRATEDB_SCHEMA", type=str, required=False, help="Schema where to import the data")
@click.option("--table", envvar="CRATEDB_TABLE", type=str, required=False, help="Table where to import the data")
@click.pass_context
def load_dataset(
    ctx: click.Context,
    name: str,
    schema: str,
    table: str,
):
    """
    Import named dataset into CrateDB and CrateDB Cloud clusters.
    """

    # Adjust/convert target table parameter.
    effective_table = None
    if table is not None:
        table_address = TableAddress(schema=schema, table=table)
        effective_table = table_address.fullname

    # Dispatch "load dataset" operation.
    cluster = DatabaseCluster.from_ctx(ctx)
    ds = cluster.load_dataset(name=name, table=effective_table)

    logger.info(f"Importing dataset succeeded. Name: {name}, Table: {ds.table}")
    logger.info(f"Peek SQL: SELECT * FROM {ds.table} LIMIT 42;")
