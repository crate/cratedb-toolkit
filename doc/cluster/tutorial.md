(cluster-api-tutorial)=

# CrateDB Cluster CLI/API Tutorial

This tutorial outlines end-to-end examples connecting to the CrateDB Cloud
API and the CrateDB database cluster. It includes examples about both the
{ref}`cluster-api-cli` and the {ref}`cluster-api-python`.

## Configure

It needs all relevant access credentials and configuration settings outlined
below. This example uses environment variables stored into an `.env` file.

```shell
cat > .env << EOF
# Connect to managed CrateDB.

# CrateDB Cloud API credentials.
CRATEDB_CLOUD_API_KEY='<YOUR_API_KEY_HERE>'
CRATEDB_CLOUD_API_SECRET='<YOUR_API_SECRET_HERE>'

# CrateDB Cloud cluster identifier (id or name).
# CRATEDB_CLUSTER_ID='<YOUR_CLUSTER_ID_HERE>'
CRATEDB_CLUSTER_NAME='<YOUR_CLUSTER_NAME_HERE>'
EOF
```

## CLI

Use the {ref}`cluster-api`'s {ref}`ctk cluster <cluster-api-cli>` command to deploy a database cluster,
the `ctk load table` command of the {ref}`io-subsystem` to import data,
and the {ref}`shell` command for executing an SQL statement.
```shell
ctk cluster start
ctk load table "https://cdn.crate.io/downloads/datasets/cratedb-datasets/machine-learning/timeseries/nab-machine-failure.csv"
ctk shell --command 'SELECT * FROM "nab-machine-failure" LIMIT 10;'
ctk cluster stop
```

## Python API

Use the Python API to deploy, import, and query data.
```python
from pprint import pprint
from cratedb_toolkit import InputOutputResource, ManagedCluster

# Define data source.
url = "https://cdn.crate.io/downloads/datasets/cratedb-datasets/machine-learning/timeseries/nab-machine-failure.csv"
source = InputOutputResource(url=url)

# Connect to CrateDB Cloud.
with ManagedCluster.from_env() as cluster:

    # Invoke the import job.
    cluster.load_table(source=source)

    # Query imported data.
    results = cluster.query('SELECT * FROM "nab-machine-failure" LIMIT 10;')
    pprint(results)
```
