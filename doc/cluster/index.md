(cluster-api)=

# Cluster API

CrateDB Toolkit provides a convenient API and SDK for managing CrateDB clusters
per `ctk cluster` CLI command, or using a fluent Python API.

```{toctree}
:maxdepth: 1
:hidden:

ctk cluster CLI <cli>
Python API <python>
Tutorial CLI+API <tutorial>
Backlog <backlog>
```

::::{grid} 2
:gutter: 4

:::{grid-item-card} {material-outlined}`terminal;2em` CLI
:link: cluster-api-cli
:link-type: ref
:link-alt: CrateDB Cluster CLI

Higher level CLI entrypoints for cluster operations.
+++
**What's inside:**
The `ctk cluster {start,info,stop}` subcommands.
:::

:::{grid-item-card} {material-outlined}`api;2em` Python API
:link: cluster-api-python
:link-type: ref
:link-alt: CrateDB Cluster Python API

Higher level API/SDK components for cluster operations.
+++
**What's inside:**
The `cratedb_toolkit.ManagedCluster` class.
:::

:::{grid-item-card} {material-outlined}`school;2em` Tutorials
:link: cluster-api-tutorial
:link-type: ref
:link-alt: Tutorials
:columns: 12

End-to-end examples connecting to the CrateDB Cloud
API and the CrateDB database cluster.
+++
**What's inside:**
CLI and SDK examples.
:::

::::
