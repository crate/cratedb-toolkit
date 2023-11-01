import click

from cratedb_toolkit.job.croud import jobs_list
from cratedb_toolkit.util import jd


@click.command(name="list-jobs")
@click.option(
    "--cluster-id", envvar="CRATEDB_CLOUD_CLUSTER_ID", type=str, required=True, help="CrateDB Cloud cluster identifier"
)
@click.pass_context
def cli_list_jobs(ctx: click.Context, cluster_id: str):
    """
    List running jobs.
    """
    jd(jobs_list(cluster_id))
