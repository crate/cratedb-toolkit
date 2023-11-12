import logging
import sys

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.cluster.util import get_cluster_info
from cratedb_toolkit.io.croud import CloudIo, CloudIoResource, CloudIoTarget
from cratedb_toolkit.util import jd
from cratedb_toolkit.util.cli import boot_click, make_command
from cratedb_toolkit.util.croud import CroudException

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


class GuidingTexts:
    """
    TODO: Add more richness / guidance to the text output.
    """

    def __init__(self, admin_url: str = None, table_name: str = None):
        self.admin_url = admin_url
        self.table_name = table_name

    def success(self):
        return f"""
        Excellent, that worked well.

        Now, you may want to inquire your data. To do that, use either CrateDB Admin UI,
        or connect on your terminal using `crash`, `ctk shell`, or `psql`.

        The CrateDB Admin UI for your cluster is available at [1]. To easily inspect a
        few samples of your imported data, or to check the cardinality of your database
        table, run [2] or [3]. If you want to export your data again, see [4].

        [1] {self.admin_url}
        [2] ctk shell --command "SELECT * FROM {self.table_name} LIMIT 10;"
        [3] ctk shell --command "SELECT COUNT(*) FROM {self.table_name};"
        [4] https://community.cratedb.com/t/cratedb-cloud-news-simple-data-export/1556
        """  # noqa: S608

    def error(self):
        return """
        That went south.

        If you can share your import source, we will love to hear from you on our community
        forum [1]. Otherwise, please send us an email [2] about the flaw you've discovered.
        To learn more about the data import feature, see [3].

        [1] https://community.cratedb.com/
        [2] support@crate.io
        [3] https://community.cratedb.com/t/importing-data-to-cratedb-cloud-clusters/1467
        """


@make_command(cli, name="table")
@click.argument("url")
@click.option(
    "--cluster-id", envvar="CRATEDB_CLOUD_CLUSTER_ID", type=str, required=False, help="CrateDB Cloud cluster identifier"
)
@click.option(
    "--cratedb-sqlalchemy-url", envvar="CRATEDB_SQLALCHEMY_URL", type=str, required=False, help="CrateDB SQLAlchemy URL"
)
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
    schema: str,
    table: str,
    format_: str,
    compression: str,
):
    """
    Import data into CrateDB and CrateDB Cloud clusters.
    """

    if not cluster_id and not cratedb_sqlalchemy_url:
        raise KeyError(
            "Either CrateDB Cloud Cluster identifier or CrateDB SQLAlchemy URL needs to be supplied. "
            "Use --cluster-id / --cratedb-sqlalchemy-url CLI options "
            "or CRATEDB_CLOUD_CLUSTER_ID / CRATEDB_SQLALCHEMY_URL environment variables."
        )

    resource = CloudIoResource(url=url, format=format_, compression=compression)
    target = CloudIoTarget(schema=schema, table=table)

    if cluster_id:
        load_table_cloud(cluster_id, resource, target)
    elif cratedb_sqlalchemy_url:
        load_table_cratedb(cratedb_sqlalchemy_url, url)
    else:
        raise NotImplementedError("Importing resource not implemented yet")


def load_table_cloud(cluster_id: str, resource: CloudIoResource, target: CloudIoTarget):
    """
    export CRATEDB_CLOUD_CLUSTER_ID=95998958-4d96-46eb-a77a-a894e7dde128
    ctk load table https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_weather.csv.gz

    https://console.cratedb.cloud
    """
    cluster_info = get_cluster_info(cluster_id=cluster_id)
    cio = CloudIo(cluster_id=cluster_id)

    try:
        job_info, success = cio.load_resource(resource=resource, target=target)
        jd(job_info)
        # TODO: Explicitly report about `failed_records`, etc.
        texts = GuidingTexts(
            admin_url=cluster_info.cloud["url"],
            table_name=job_info["destination"]["table"],
        )
        if success:
            print(texts.success(), file=sys.stderr)  # noqa: T201
        else:
            print(texts.error(), file=sys.stderr)  # noqa: T201
            sys.exit(1)

    # When exiting so, it is expected that error logging has taken place appropriately.
    except CroudException:
        logger.exception("Unknown error")
        sys.exit(1)


def load_table_cratedb(sqlalchemy_url: str, resource_url: str):
    """
    export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
    ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
    """
    if resource_url.startswith("influxdb"):
        from influxio.core import copy

        source_url = resource_url.replace("influxdb2", "http")
        target_url = sqlalchemy_url
        copy(source_url, target_url, progress=True)
    else:
        raise NotImplementedError("Importing resource not implemented yet")
