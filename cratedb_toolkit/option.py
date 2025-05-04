import click

option_cluster_id = click.option(
    "--cluster-id", envvar="CRATEDB_CLUSTER_ID", type=str, required=False, help="CrateDB Cloud cluster identifier"
)
option_cluster_name = click.option(
    "--cluster-name", envvar="CRATEDB_CLUSTER_NAME", type=str, required=False, help="CrateDB Cloud cluster name"
)
option_cluster_url = click.option(
    "--cluster-url", envvar="CRATEDB_CLUSTER_URL", type=str, required=False, help="CrateDB SQLAlchemy or HTTP URL"
)
option_username = click.option(
    "--username", envvar="CRATEDB_USERNAME", type=str, required=False, help="Username for CrateDB cluster"
)
option_password = click.option(
    "--password", envvar="CRATEDB_PASSWORD", type=str, required=False, help="Password for CrateDB cluster"
)
option_schema = click.option(
    "--schema",
    envvar="CRATEDB_SCHEMA",
    type=str,
    required=False,
    help="Default schema for statements if schema is not explicitly stated within queries",
)
