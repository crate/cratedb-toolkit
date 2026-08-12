(cfr-jobstats)=
# Job statistics collector

Collect query statistics from `sys.jobs_log` continuously, and keep them beyond the
retention of that table. `collect` and `view` handle the raw statistics, `report` and `ui`
launch an interpreted, interactive dashboard on top of the same collected data.

This is distinct from the one-shot `ctk info jobs` snapshot. For a side-by-side comparison
of both, see {ref}`jobs-vs-jobstats`.

## Install
```shell
pip install --upgrade 'cratedb-toolkit[cfr]'
```
:::{tip}
Alternatively, use the Docker image per `ghcr.io/crate/cratedb-toolkit`.
For more information about installing CrateDB Toolkit, see {ref}`install`.
:::

## Synopsis

The collector stores its statistics in the schema of the cluster URL, `stats` by default.
```shell
export CRATEDB_CLUSTER_URL=crate://crate@localhost:4200/?schema=stats
```

Collect statistics, then display or explore them.
```shell
ctk cfr jobstats collect
```
```shell
ctk cfr jobstats view
```
```shell
ctk cfr jobstats report
```
```shell
ctk cfr jobstats ui
```

:::{note}
Please collect statistics first using `ctk cfr jobstats collect`, then use the other
commands to display or explore them. `view` creates its tables on demand, so an empty
result means nothing has been collected into that schema yet.
:::

## How collection works

`collect` polls `sys.jobs_log` for jobs which finished since the last poll, and folds them
into per-statement statistics. Statements against `sys.*` and `information_schema.*` are
skipped, so the collector does not account for its own queries.

How far the collector has come is recorded as a watermark, so a restarted collector picks
up where it left off, instead of counting the same jobs again. Each cycle considers jobs
which ended after the watermark, up to and including the current moment.

Per distinct statement, the collector maintains:
- `calls` — how often the statement has been executed
- `bucket` — a histogram of the execution durations, in milliseconds. A duration is counted
  into the first bucket whose threshold it stays below. The thresholds are 10, 50, 100, 500,
  1000, 2000, 5000, 10000, 15000, and 20000, plus `INF` for everything slower.
- `avg_duration` — a *decaying* average, updated as `(previous + current) / 2` per
  execution. Recent executions therefore weigh much more heavily than an arithmetic mean
  over all executions would.
- `nodes` — the nodes which have run the statement, without duplicates
- `last_used` — when the statement was last seen
- `username`, `query_type` — as reported by CrateDB

## Tables

Two tables are created in the configured schema.

`"<schema>".jobstats_statements` holds one record per distinct statement:

| Column | Type | Description |
|---|---|---|
| `id` | `TEXT` | identifier assigned by the collector |
| `stmt` | `TEXT` | the statement, anonymized when collected with `--anonymize` |
| `calls` | `INT` | number of executions counted so far |
| `bucket` | `OBJECT` | duration histogram, keyed by threshold |
| `last_used` | `TIMESTAMP` | when the statement was last seen |
| `username` | `TEXT` | user which ran the statement |
| `query_type` | `TEXT` | statement classification reported by CrateDB |
| `avg_duration` | `FLOAT` | decaying average duration, in milliseconds |
| `nodes` | `ARRAY(TEXT)` | nodes which have run the statement, as JSON strings of the `node` object of `sys.jobs_log` |

`"<schema>".jobstats_last` holds a single record, the watermark:

| Column | Type | Description |
|---|---|---|
| `last_execution` | `TIMESTAMP` | up to when jobs have been collected |

## Configuration

The cluster address is taken from `--cluster-url` / `CRATEDB_CLUSTER_URL`, and the schema
from its `?schema=` parameter, defaulting to `stats`. Additionally, these environment
variables are recognized.

- `INTERVAL` — how long to sleep between two collection cycles, in seconds. Default: `10`.
- `INITIAL_LOOKBACK_SECONDS` — how far back to look for jobs when no watermark has been
  recorded yet, in seconds. Default: `600`.
- `STMT_TABLE` — full-qualified name of the statistics table, overriding the default
  `"<schema>".jobstats_statements`.
- `LAST_EXEC_TABLE` — full-qualified name of the watermark table, overriding the default
  `"<schema>".jobstats_last`.

:::{rubric} Options
:::
`ctk cfr jobstats collect`:
- `--once` — record only one sample, then exit, instead of collecting continuously
- `--reportdb` / `-r` — a separate database URL to store the statistics in
  (`crate://crate@localhost:4200/?schema=stats&sslmode=require`). Jobs are read from the
  cluster URL, and written to this one.
- `--anonymize` — path to a decoder dictionary file for anonymizing SQL statements before
  they are stored; using the flag without a value defaults to `decoder_dictionary.json` in
  the current working directory

`ctk cfr jobstats view`:
- `--reportdb` / `-r` — a separate database URL to read the statistics from
- `--deanonymize` — path to the decoder dictionary file used to reverse `--anonymize`,
  to view statements in their original form

`ctk cfr jobstats report` and `ctk cfr jobstats ui` read the statistics from the schema of
the cluster URL. They do not accept `--reportdb`, so when `collect --reportdb` was used,
address that database per `--cluster-url` here. `ui` serves the dashboard on
`localhost:7777`.

## Anonymization

With `--anonymize`, statements are anonymized before they are stored, and the substitutions
are recorded in the decoder dictionary file. Keep that file: it is the only way to make the
collected statements legible again, using `view --deanonymize`.

```shell
ctk cfr jobstats collect --once --anonymize ./decoder_dictionary.json
```
```shell
ctk cfr jobstats view --deanonymize ./decoder_dictionary.json
```

:::{warning}
The decoder dictionary maps anonymized tokens back to the original identifiers and string
literals. Treat it as confidential, do not ship it together with the collected statistics.
:::

:::{note}
Anonymization fails closed: when a statement cannot be anonymized, it is stored as
`<redacted: <digest>>` rather than in clear text. Statistics per distinct statement remain
meaningful, but such statements cannot be recovered with `--deanonymize`. The event is
reported as a warning.
:::
