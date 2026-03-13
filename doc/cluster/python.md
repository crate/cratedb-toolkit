(cluster-api-python)=

# CrateDB Cluster Python API

:::{include} /_snippet/links.md
:::

The `cratedb_toolkit.ManagedCluster` class provides the higher level API/SDK
entrypoints to start/deploy/resume a database cluster, inquire information
about it, and stop/suspend it again.

The subsystem is implemented on top of the {ref}`croud:index` application,
which gets installed along the lines and is used later on this page.

:::{include} /_snippet/install-ctk.md
:::

:::{include} /_snippet/cloud-prerequisites.md
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
