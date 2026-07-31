(cluster-info)=
# CrateDB Cluster Information

A bundle of information inquiry utilities, for diagnostics and more.

:::{tip}
**Which command do I want?**
- `ctk info cluster` / `ctk info jobs` — a curated, one-shot snapshot of hand-picked health,
  shard, and query metrics. Good for a quick look at what's going on right now.
- `ctk cfr info record` — the same snapshot, persisted over time. See {ref}`cfr-info`.
- `ctk cfr jobstats collect` / `view` — a separate, continuously-collected time series of
  query statistics, not the same data as `ctk info jobs`. See {ref}`cfr-jobstats`.
- `ctk cfr sys-export` — a true raw dump of every system table, no interpretation. This is
  the one to reach for when collecting diagnostics for a CrateDB support case.
  See {ref}`cfr-systable`.
:::

## Install
```shell
pip install --upgrade 'cratedb-toolkit'
```
:::{tip}
Alternatively, use the Docker image per `ghcr.io/crate/cratedb-toolkit`.
For more information about installing CrateDB Toolkit, see {ref}`install`.
:::

## Synopsis

Define CrateDB database cluster address per command-line option. Choose one of both alternatives.
```shell
ctk cfr --cluster-url "https://username:password@localhost:4200/?schema=ext" jobstats collect
```
```shell
ctk cfr --cluster-url "crate://username:password@localhost:4200/?schema=ext&ssl=true" jobstats collect
```

Define CrateDB database cluster address per environment variable. Choose one of both alternatives.
```shell
export CRATEDB_CLUSTER_URL=https://username:password@localhost:4200/?schema=ext
```
```shell
export CRATEDB_CLUSTER_URL=crate://username:password@localhost:4200/?schema=ext&ssl=true
```
:::{note}
For some commands, both options might not be available yet, just one of them.
:::


### One shot commands
Display system and database cluster information. Output is mostly raw SQL result rows;
a handful of elements (`cluster_name`, `cluster_nodes_count`, `max_checkpoint_delta`,
`shard_not_started_count`, `shard_total_count`, `translog_uncommitted_size`) are reduced
to a single value.
```shell
ctk info cluster
```

:::{rubric} Elements
:::
Health:
- **Cluster name** — the cluster's name
- **Cluster Health** — overall cluster health, including missing and underreplicated shard counts
- **Total number of cluster nodes** — node count
- **Cluster Nodes** — telemetry information for all cluster nodes
- **Table Health** — table health short summary
- **Recent Backups** — most recent 10 backups

Shards:
- **Shard Allocation** — support identifying issues with shard allocation
- **Table Allocations** — table allocation across nodes, shards, and partitions
- **Shard Distribution** — shard distribution across nodes
- **Table Shard Count** — total number of shards per table
- **Shard Rebalancing Progress** — information about rebalancing progress
- **Shard Rebalancing Status** — information about rebalancing activities
- **Shards not started** — information about shards which have not been started
- **Number of shards not started** — total number of shards which have not been started
- **Delta between local and global checkpoint** — a significantly large delta can mean shard
  replication has stalled or slowed down
- **Number of shards** — total number of shards
- **Uncommitted Translog** — compares `flush_threshold_size` with the `uncommitted_size` of a
  shard, to check translogs are being committed properly
- **Total uncommitted translog size** — a large uncommitted total can indicate issues with
  shard replication

Display database cluster job information: a one-shot, ad hoc snapshot, not persisted over
time. Contrast with `ctk cfr jobstats collect`, which collects the same kind of information
continuously, see {ref}`cfr-jobstats`.
```shell
ctk info jobs
```

:::{rubric} Elements
:::
- **Query age range** — timestamps of first and last job
- **Queries by user** — total number of queries per user
- **Query Duration Distribution (Buckets)** — distribution of query durations, bucketed
- **Query Duration Distribution (Percentiles)** — distribution of query durations, percentiles
- **Query History** — statements and durations of the 100 most recent queries / jobs
- **Query History Count** — total number of queries on this node
- **Query performance 15min** — query performance within the last 15 minutes: queries per
  second, and query speed (ms)
- **Currently Running Queries** — statements and durations of currently running queries / jobs
- **Number of running queries** — total number of currently running queries
- **Query frequency** — the 100 most frequent queries
- **Individual Query Duration** — the 100 queries by individual duration (ms)
- **Total Query Duration** — the 100 queries by total duration (ms)

Display database cluster log messages: a raw, limited passthrough of the most recent
`sys.jobs_log` rows, filtered to exclude queries against `sys.*`/`information_schema.*`.
```shell
ctk info logs
```

Display the most recent entries of the `sys.jobs_log` table,
optionally polling it for updates by adding `--follow`.
For more information, see [](#tail).
```shell
ctk tail -n 3 sys.jobs_log
```


## HTTP API

Install.
```shell
pip install --upgrade 'cratedb-toolkit[service]'
```

Expose collected status information. An HTTP wrapper around the same data as
`ctk info cluster`, the only endpoint is `GET /info/all`.

```shell
ctk info serve
```
Consume cluster information via HTTP.
```shell
http http://127.0.0.1:4242/info/all
```

Make the service listen on a specific address.
```shell
ctk info serve --listen 0.0.0.0:8042
```

:::{note}
The `--reload` option is suitable for development scenarios where you intend
to have the changes to the code become available while editing, in near
real-time.
```shell
ctk info --debug serve --reload
```
:::
