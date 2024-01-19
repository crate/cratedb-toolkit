import sys

import click

from cratedb_toolkit.job.croud import jobs_list
from cratedb_toolkit.util.croud import get_croud_output_formats

try:
    output_formats = get_croud_output_formats()
except ImportError:
    output_formats = ["UNKNOWN"]


@click.command(name="list-jobs")
@click.option(
    "--cluster-id", envvar="CRATEDB_CLOUD_CLUSTER_ID", type=str, required=True, help="CrateDB Cloud cluster identifier"
)
@click.option(
    "--format",
    "format_",
    type=click.Choice(output_formats, case_sensitive=False),
    required=False,
    help="The output format for the result of the operation",
)
@click.pass_context
def cli_list_jobs(ctx: click.Context, cluster_id: str, format_: str):
    """
    List CrateDB Cloud jobs.
    """
    print(jobs_list(cluster_id, output_format=format_, decode_output=False), file=sys.stdout)  # noqa: T201
