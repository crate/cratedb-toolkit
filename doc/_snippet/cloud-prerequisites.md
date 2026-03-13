## Prerequisites

Before using CrateDB Cloud services, authenticate and select
your target database cluster.

### Authenticate

When working with [CrateDB Cloud], you can select between two authentication variants.
Either _interactively authorize_ your terminal session using `croud login`,
```shell
# Replace YOUR_IDP with one of: cognito, azuread, github, google.
croud login --idp YOUR_IDP
```
or provide API access credentials per environment variables for _headless/unattended
operations_ after creating them using the [CrateDB Cloud Console] or
`croud api-keys create`.
```shell
# Provide CrateDB Cloud API authentication tokens.
export CRATEDB_CLOUD_API_KEY='<YOUR_API_KEY>'
export CRATEDB_CLOUD_API_SECRET='<YOUR_API_SECRET>'
```

### Select cluster

Discover the list of available database clusters.
```shell
croud clusters list
```

Select the designated target database cluster using one of three variants,
either by using CLI options or environment variables.
- All address options are mutually exclusive.
- CLI options take precedence over environment variables.
- Environment variables can be stored into an `.env` file in your working directory.

:CLI options:
  `--cluster-id`, `--cluster-name`, `--cluster-url`
:Environment variables:
  `CRATEDB_CLUSTER_ID`, `CRATEDB_CLUSTER_NAME`, `CRATEDB_CLUSTER_URL`

Before invoking any of the next steps, address the CrateDB Cloud Cluster
you are aiming to connect to, for example by defining the cluster id
using the `CRATEDB_CLUSTER_ID` environment variable.
```shell
export CRATEDB_CLUSTER_ID='<YOUR_CLUSTER_ID>'
```
