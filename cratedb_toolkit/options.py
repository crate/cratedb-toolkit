import click

option_cluster_id = click.option(
    "--cluster-id", envvar="CRATEDB_CLOUD_CLUSTER_ID", type=str, required=False, help="CrateDB Cloud cluster identifier"
)
option_cluster_name = click.option(
    "--cluster-name", envvar="CRATEDB_CLOUD_CLUSTER_NAME", type=str, required=False, help="CrateDB Cloud cluster name"
)
option_sqlalchemy_url = click.option(
    "--cratedb-sqlalchemy-url", envvar="CRATEDB_SQLALCHEMY_URL", type=str, required=False, help="CrateDB SQLAlchemy URL"
)
option_http_url = click.option(
    "--cratedb-http-url", envvar="CRATEDB_HTTP_URL", type=str, required=False, help="CrateDB HTTP URL"
)
