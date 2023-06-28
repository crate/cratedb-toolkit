# Data roll-up, expiration, and retention manager for CrateDB

[![Tests](https://github.com/crate-workbench/cratedb-rollup/actions/workflows/main.yml/badge.svg)](https://github.com/crate-workbench/cratedb-rollup/actions/workflows/main.yml)

## About

A data roll-up, expiration, and retention management subsystem for CrateDB,
implementing different strategies.

The application manages the life-cycle of data stored in CrateDB, handling
concerns of data expiry, size reduction, and archival. Within a system storing
and processing large amounts of data, it is crucial to manage data flows between
hot and cold storage types better than using ad hoc solutions.

Data retention policies can be flexibly configured by adding records to the
`retention_policies` database table, which is also stored within CrateDB.

### Background

> A [roll-up], as an OLAP data operation, involves summarizing the data along a
> dimension. The summarization rule might be an aggregate function, such as
> computing totals along a hierarchy or by applying a set of formulas.

With other databases, this technique, or variants thereof, is known as [rolling up
historical data], [downsampling a time series data stream], [downsampling and data
retention], or just [downsampling].

It is useful to reduce the storage size of historical data by decreasing its
resolution, if you don't need it.

When rolling up timeseries-data, [time bucketing] is often used for grouping records
into equal-sized time ranges, before applying a resampling function on them.

### Details

> The `retention_policies` database table is also stored within CrateDB.

By default, the `ext` schema is used for that, so the effective full-qualified database
table name is `"ext"."retention_policies"`. It is configurable by using the `--schema`
command-line option, or the `CRATEDB_EXT_SCHEMA` environment variable.


## Strategies

This section enumerates the different data roll-up and retention strategies implemented.
Other strategies can be added.

### DELETE

A basic retention policy algorithm that drops records from expired partitions.

```sql
-- A policy using the DELETE strategy.
INSERT INTO retention_policies
  (table_schema, table_name, partition_column, retention_period, strategy)
VALUES
  ('doc', 'raw_metrics', 'ts_day', 1, 'delete');
```

[implementation](cratedb_rollup/strategy/delete.py) | [tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913) 

### REALLOCATE

A retention policy algorithm that reallocates expired partitions from hot nodes
to cold nodes.

Because each cluster member is assigned a designated node type by using the
`-Cnode.attr.storage=hot|cold` parameter, this strategy is only applicable in
cluster/multi-node scenarios.

On the data expiration run, corresponding partitions will get physically moved to
cluster nodes of the `cold` type, which are mostly designated archive nodes, with
large amounts of storage space.

```sql
-- A policy using the REALLOCATE strategy.
INSERT INTO retention_policies VALUES
  ('doc', 'raw_metrics', 'ts_day', 60, 'storage', 'cold', NULL, 'reallocate');
```

[implementation](cratedb_rollup/strategy/reallocate.py) | [tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-building-a-hot-cold-storage-data-retention-policy/934)

### SNAPSHOT

A retention policy algorithm that snapshots expired partitions to a repository,
and prunes them from the database afterwards. It is suitable for long-term
data archival purposes.

In CrateDB jargon, a repository is a bucket on an S3-compatible object store,
where data in form of snapshots can be exported to, and imported from.

```sql
-- A policy using the SNAPSHOT strategy.
INSERT INTO retention_policies
  (table_schema, table_name, partition_column, retention_period, target_repository_name, strategy)
VALUES
  ('doc', 'sensor_readings', 'time_month', 365, 'export_cold', 'snapshot');
```

[implementation](cratedb_rollup/strategy/snapshot.py) | [tutorial](https://community.crate.io/t/building-a-data-retention-policy-for-cratedb-with-apache-airflow/1001)


## Install

Install package.
```shell
pip install --upgrade git+https://github.com/crate-workbench/cratedb-rollup
```

Install retention policy bookkeeping tables.
```shell
cratedb-rollup setup "crate://localhost/"
```


## Usage

Define a few retention policy rules using SQL.
```shell
# A policy using the DELETE strategy.
docker run --rm -i --network=host crate crash <<SQL
    INSERT INTO retention_policies (
      table_schema, table_name, partition_column, retention_period, strategy)
    VALUES ('doc', 'raw_metrics', 'ts_day', 1, 'delete');
SQL
```

Run the roll-up job.
```shell
cratedb-rollup run --cutoff-day=2023-06-27 --strategy=delete "crate://localhost"
```


## Development

It is recommended to use a Python virtualenv for the subsequent operations.
If you something gets messed up during development, it is easy to nuke the
installation, and start from scratch.
```shell
python3 -m venv .venv
source .venv/bin/activate
```

Acquire sources.
```shell
git clone https://github.com/crate-workbench/cratedb-rollup
cd cratedb-rollup
```

Install project in sandbox mode.
```shell
pip install --editable=.[develop,test]
```

Run tests. `TC_KEEPALIVE` keeps the auxiliary service containers running, which
speeds up runtime on subsequent invocations. Note that the test suite uses the
`testdrive-ext` schema for storing the retention policy table.
```shell
export TC_KEEPALIVE=true
poe check
```

Format code.
```shell
poe format
```


[downsampling]: https://docs.victoriametrics.com/#downsampling
[downsampling a time series data stream]: https://www.elastic.co/guide/en/elasticsearch/reference/current/downsampling.html
[downsampling and data retention]: https://docs.influxdata.com/influxdb/v1.8/guides/downsample_and_retain/
[rolling up historical data]: https://www.elastic.co/guide/en/elasticsearch/reference/current/rollup-overview.html
[roll-up]: https://en.wikipedia.org/wiki/OLAP_cube#Operations
[time bucketing]: https://community.crate.io/t/resampling-time-series-data-with-date-bin/1009
