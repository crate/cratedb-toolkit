:::{rubric} Cluster address
:::

The program provides various options for addressing the database cluster
in different situations, managed or not.

:CLI options:
  `--cluster-id`, `--cluster-name`, `--cluster-url`
:Environment variables:
  `CRATEDB_CLUSTER_ID`, `CRATEDB_CLUSTER_NAME`, `CRATEDB_CLUSTER_URL`

:::{note}
- All address options are mutually exclusive.
- CLI options take precedence over environment variables.
- The cluster identifier takes precedence over the cluster name.
- The cluster url takes precedence over the cluster id and name.
- Environment variables can be stored into an `.env` file in your working directory.
:::
