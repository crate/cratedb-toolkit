import click

option_cluster_id = click.option(
    "--cluster-id", envvar="CRATEDB_CLOUD_CLUSTER_ID", type=str, required=False, help="CrateDB Cloud cluster identifier"
)
option_cluster_name = click.option(
    "--cluster-name", envvar="CRATEDB_CLOUD_CLUSTER_NAME", type=str, required=False, help="CrateDB Cloud cluster name"
)
option_sqlalchemy_url = click.option(
    "--sqlalchemy-url", envvar="CRATEDB_SQLALCHEMY_URL", type=str, required=False, help="CrateDB SQLAlchemy URL"
)
option_http_url = click.option(
    "--http-url", envvar="CRATEDB_HTTP_URL", type=str, required=False, help="CrateDB HTTP URL"
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
