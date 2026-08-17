(cluster-info)=
# CrateDB Cluster Information

A bundle of information inquiry utilities, for diagnostics and more.

:::{tip}
**Which command do I want?**
- `ctk info cluster` / `ctk info jobs` — a curated, one-shot snapshot of hand-picked health,
  shard, and query metrics. Good for a quick look at what's going on right now.
- `ctk cfr info record` — the same snapshot, persisted over time. See {ref}`cfr-info`.
- `ctk cfr jobstats collect` / `view` — query statistics accumulated continuously over time.
  See {ref}`jobs-vs-jobstats` for how it compares to `ctk info jobs`.
- `ctk cfr sys-export` — a raw dump of every system table plus your own table and
  view definitions, with a manifest describing what was collected, delivered as
  one shareable file. No interpretation — that happens on the support side.
  For a support case, this is the one to reach for; the commands above are not a
  substitute for it. See {ref}`cfr-systable`.
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
ctk info --cluster-url "https://username:password@localhost:4200/" jobs
```
```shell
ctk info --cluster-url "crate://username:password@localhost:4200/?ssl=true" jobs
```

Define CrateDB database cluster address per environment variable. Choose one of both alternatives.
```shell
export CRATEDB_CLUSTER_URL=https://username:password@localhost:4200/
```
```shell
export CRATEDB_CLUSTER_URL=crate://username:password@localhost:4200/?ssl=true
```

On CrateDB Cloud, address the cluster by name or by identifier instead, using
`--cluster-name` / `CRATEDB_CLUSTER_NAME`, or `--cluster-id` / `CRATEDB_CLUSTER_ID`.
```shell
ctk info --cluster-name hotzenplotz jobs
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

Display database cluster log messages: a raw, limited passthrough of the 100 most recent
`sys.jobs_log` rows, filtered to exclude queries against `sys.*`/`information_schema.*`.
The row limit is not adjustable.
```shell
ctk info logs
```

Display the most recent entries of the `sys.jobs_log` table,
optionally polling it for updates by adding `--follow`.
For more information, see [](#tail).
```shell
ctk tail -n 3 sys.jobs_log
```


## Job information

Display database cluster job information: a one-shot, ad hoc snapshot, computed on the
spot and not persisted anywhere.
```shell
ctk info jobs
```

Every element is an individual SQL statement against `sys.jobs_log` (jobs which have
finished) or `sys.jobs` (jobs which are still running), evaluated when you invoke the
command. All durations are reported in milliseconds.

:::{rubric} Elements
:::
| Output key | Label | Source | Result |
|---|---|---|---|
| `age_range` | Query age range | `sys.jobs_log` | `first_job`, `last_job` |
| `by_user` | Queries by user | `sys.jobs_log` | `username`, `count` per user |
| `duration_buckets` | Query Duration Distribution (Buckets) | `sys.jobs_log` | `bucket`, `count`, `duration` per percentile bucket |
| `duration_percentiles` | Query Duration Distribution (Percentiles) | `sys.jobs_log` | `min`, `p50`, `p90`, `p99`, `max` |
| `history` | Query History | `sys.jobs_log` | the 100 most recent jobs, oldest first: `time`, `stmt`, `duration`, `username` |
| `history_count` | Query History Count | `sys.jobs_log` | single value: total number of recorded jobs |
| `performance15min` | Query performance 15min | `sys.jobs_log` | per 10-second interval and query type: `qps`, `duration` |
| `running` | Currently Running Queries | `sys.jobs` | `time`, `stmt`, `duration`, `username` |
| `running_count` | Number of running queries | `sys.jobs` | single value: number of running jobs |
| `top100_count` | Query frequency | `sys.jobs_log` | the 100 most frequent statements: `stmt`, `stmt_count`, `min_duration`, `max_duration`, `avg_duration`, `p99` |
| `top100_duration_individual` | Individual Query Duration | `sys.jobs_log` | the 100 slowest single executions: `duration`, `stmt` |
| `top100_duration_total` | Total Query Duration | `sys.jobs_log` | the 100 statements with the highest total duration: `total_duration`, `stmt`, `stmt_count` |

`history` and `running` omit statements mentioning `snapshot`, to keep backup activity out
of the query history.

:::{rubric} Output
:::
The command emits a single JSON document with two sections. `data` holds one entry per
element, keyed by its output key. `meta` describes the elements, including the SQL
statement each one ran, so you can re-run an individual element by hand.
```json
{
  "meta": {
    "system_time": "2026-08-11T12:00:00.000000",
    "application_name": "CrateDB Toolkit",
    "application_version": "0.0.0",
    "elements": {
      "running_count": {
        "name": "running_count",
        "label": "Number of running queries",
        "sql": "SELECT\n  COUNT(*) AS job_count\nFROM\n    sys.jobs;",
        "description": "Total number of currently running queries.",
        "transform": "functools.partial(...)",
        "unit": null
      }
    }
  },
  "data": {
    "running_count": 1,
    "by_user": [{"username": "crate", "count": 42}]
  }
}
```

:::{note}
`sys.jobs_log` is a bounded, in-memory record of finished jobs. It is governed by the
`stats.enabled`, `stats.jobs_log_size`, and `stats.jobs_log_expiration` cluster settings,
and it does not survive a node restart. With statistics disabled, `ctk info jobs` reports
empty results. Because the table only ever holds a recent window, a snapshot cannot answer
questions about last week — that is what the continuous collector is for.
:::

(jobs-vs-jobstats)=
### Comparison with `ctk cfr jobstats`

Both commands report on the jobs of a cluster, but they are separate implementations with
different purposes. See {ref}`cfr-jobstats` for the collector.

| | `ctk info jobs` | `ctk cfr jobstats collect` / `view` |
|---|---|---|
| Mode | one shot, stateless | polls the cluster continuously, `--once` for a single sample |
| Source | `sys.jobs_log` and `sys.jobs` | `sys.jobs_log` |
| Statements considered | all; `history` and `running` skip `snapshot` statements | skips statements against `sys.*` and `information_schema.*` |
| Persistence | none, JSON on stdout | tables in the configured schema, default `stats` |
| Retention | whatever `sys.jobs_log` currently holds | unbounded, keeps history beyond `sys.jobs_log` |
| Aggregation | computed in SQL by CrateDB, per invocation | accumulated in the collector: call counters, duration buckets, decaying average |
| Anonymization | not available | `--anonymize` / `--deanonymize` |
| Cluster address | `--cluster-url`, `--cluster-name`, `--cluster-id` | `--cluster-url` |
| Output | `meta` / `data`, one entry per element | `meta` / `data.stats`, one entry per statement |

Rules of thumb:
- Use `ctk info jobs` to inspect a cluster you are looking at right now, or to hand a
  self-contained snapshot to someone else.
- Use `ctk cfr jobstats collect` when you need query statistics to outlive `sys.jobs_log`,
  for example to find out which statements are slow over the course of a week.


## HTTP API

Install.
```shell
pip install --upgrade 'cratedb-toolkit[service]'
```

Expose collected status information. An HTTP wrapper around the same data as
`ctk info cluster`, the only endpoint is `GET /info/all`. Job information is not
served over HTTP.

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
The HTTP service reads the cluster address from the `CRATEDB_CLUSTER_URL` environment
variable only. The `--cluster-name` and `--cluster-id` options are not honored here.
:::

:::{note}
The `--reload` option is suitable for development scenarios where you intend
to have the changes to the code become available while editing, in near
real-time.
```shell
ctk info --debug serve --reload
```
:::
