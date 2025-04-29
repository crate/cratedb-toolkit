:::{rubric} Cluster address
:::
The program provides various options for addressing the database cluster
in different situations, managed or not.

:CLI options:
  `--cluster-id`, `--cluster-name`, `--sqlalchemy-url`, `--http-url`
:Environment variables:
  `CRATEDB_CLOUD_CLUSTER_ID`, `CRATEDB_CLOUD_CLUSTER_NAME`, `CRATEDB_SQLALCHEMY_URL`, `CRATEDB_HTTP_URL`

:::{note}
- All address options are mutually exclusive.
- CLI options take precedence over environment variables.
- The cluster identifier takes precedence over the cluster name.
- Environment variables can be stored into an `.env` file in your working directory.
:::
