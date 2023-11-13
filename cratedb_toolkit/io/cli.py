import logging

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.api.main import ClusterBase, ManagedCluster, StandaloneCluster
from cratedb_toolkit.model import DatabaseAddress, InputOutputResource, TableAddress
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
@click.option(
    "--cluster-id", envvar="CRATEDB_CLOUD_CLUSTER_ID", type=str, required=False, help="CrateDB Cloud cluster identifier"
)
@click.option(
    "--cratedb-sqlalchemy-url", envvar="CRATEDB_SQLALCHEMY_URL", type=str, required=False, help="CrateDB SQLAlchemy URL"
)
@click.option("--cratedb-http-url", envvar="CRATEDB_HTTP_URL", type=str, required=False, help="CrateDB HTTP URL")
@click.option("--schema", envvar="CRATEDB_SCHEMA", type=str, required=False, help="Schema where to import the data")
@click.option("--table", envvar="CRATEDB_TABLE", type=str, required=False, help="Table where to import the data")
@click.option("--format", "format_", type=str, required=False, help="File format of the import resource")
@click.option("--compression", type=str, required=False, help="Compression format of the import resource")
@click.pass_context
def load_table(
    ctx: click.Context,
    url: str,
    cluster_id: str,
    cratedb_sqlalchemy_url: str,
    cratedb_http_url: str,
    schema: str,
    table: str,
    format_: str,
    compression: str,
):
    """
    Import data into CrateDB and CrateDB Cloud clusters.
    """

    error_message = (
        "Either CrateDB Cloud Cluster identifier or CrateDB SQLAlchemy or HTTP URL needs to be supplied. "
        "Use --cluster-id / --cratedb-sqlalchemy-url / --cratedb-http-url CLI options "
        "or CRATEDB_CLOUD_CLUSTER_ID / CRATEDB_SQLALCHEMY_URL / CRATEDB_HTTP_URL environment variables."
    )

    if not cluster_id and not cratedb_sqlalchemy_url and not cratedb_http_url:
        raise KeyError(error_message)

    # When SQLAlchemy URL is not given, but HTTP URL is, compute the former on demand.
    if cluster_id:
        address = None
    elif cratedb_sqlalchemy_url:
        address = DatabaseAddress.from_string(cratedb_sqlalchemy_url)
    elif cratedb_http_url:
        address = DatabaseAddress.from_httpuri(cratedb_sqlalchemy_url)
    else:
        raise KeyError(error_message)

    # Encapsulate source and target parameters.
    resource = InputOutputResource(url=url, format=format_, compression=compression)
    target = TableAddress(schema=schema, table=table)

    # Dispatch "load table" operation.
    cluster: ClusterBase
    if cluster_id:
        cluster = ManagedCluster(cloud_id=cluster_id)
    elif address:
        cluster = StandaloneCluster(address=address)
    else:
        raise NotImplementedError("Unable to select backend")
    return cluster.load_table(resource=resource, target=target)
