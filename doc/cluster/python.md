(cluster-api-python)=

# CrateDB Cluster Python API

The `cratedb_toolkit.ManagedCluster` class provides the higher level API/SDK
entrypoints to start/deploy/resume a database cluster, inquire information
about it, and stop/suspend it again.

The subsystem is implemented on top of the {ref}`croud:index` application,
which gets installed along the lines and is used later on this page.

## Install

We recommend using the [uv] package manager to install the framework per
`uv pip install`, or add it to your application using `uv add`.
Otherwise, using `pip install` is a viable alternative.
```shell
uv pip install --upgrade 'cratedb-toolkit'
```

## Authenticate

When working with [CrateDB Cloud], you can select between two authentication variants.
Either _interactively authorize_ your terminal session using `croud login`,
```shell
croud login --idp {cognito,azuread,github,google}
```
or provide API access credentials per environment variables for _headless/unattended
operations_ after creating them using the [CrateDB Cloud Console] or
`croud api-keys create`.
```shell
# CrateDB Cloud API credentials.
export CRATEDB_CLOUD_API_KEY='<YOUR_API_KEY_HERE>'
export CRATEDB_CLOUD_API_SECRET='<YOUR_API_SECRET_HERE>'
```

## Configure

`ManagedCluster` accepts configuration settings per constructor parameters or
environment variables.

:Environment variables:
  `CRATEDB_CLUSTER_ID`, `CRATEDB_CLUSTER_NAME`, `CRATEDB_CLUSTER_URL`

:::{note}
- All address options are mutually exclusive.
- The cluster identifier takes precedence over the cluster name.
- The cluster url takes precedence over the cluster id and name.
- Environment variables can be stored into an `.env` file in your working directory.
:::

## Usage

Acquire a database cluster handle, and run database workload.
```python
from pprint import pprint
from cratedb_toolkit import ManagedCluster

# Connect to CrateDB Cloud and run the database workload.
with ManagedCluster.from_env() as cluster:
    pprint(cluster.query("SELECT * from sys.summits LIMIT 2;"))
```

By default, the cluster will spin up, but not shut down after exiting the
context manager. If you want to do it, use the `stop_on_exit=True` option.
```python
from cratedb_toolkit import ManagedCluster

with ManagedCluster.from_env(stop_on_exit=True) as cluster:
    # ...
```

:::{seealso}
{ref}`cluster-api-tutorial` includes a full end-to-end tutorial.
:::


[CrateDB Cloud]: https://cratedb.com/docs/cloud/
[CrateDB Cloud Console]: https://console.cratedb.cloud/
[uv]: https://docs.astral.sh/uv/
