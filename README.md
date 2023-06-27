# Data roll-up and retention manager for CrateDB

[![Tests](https://github.com/crate-workbench/cratedb-rollup/actions/workflows/main.yml/badge.svg)](https://github.com/crate-workbench/cratedb-rollup/actions/workflows/main.yml)

## About

A data roll-up and retention management subsystem for CrateDB, implementing different
strategies.

> A [roll-up], as an OLAP data operation, involves summarizing the data along a
> dimension. The summarization rule might be an aggregate function, such as
> computing totals along a hierarchy or by applying a set of formulas.

From other database vendors, this technique, or variants thereof, are called [rolling
up historical data], [downsampling a time series data stream], [downsampling and data
retention], or just [downsampling].


## Strategies

This section enumerates the different data roll-up and retention strategies implemented.
Other strategies can be added.

### DELETE

Implements a retention policy algorithm that drops records from expired partitions.

[implementation](cratedb_rollup/strategy/delete.py) | [tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913) 

### REALLOCATE

Implements a retention policy algorithm that reallocates expired partitions from
hot nodes to cold nodes.

[implementation](cratedb_rollup/strategy/reallocate.py) | [tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-building-a-hot-cold-storage-data-retention-policy/934)

### SNAPSHOT

Implements a retention policy algorithm that snapshots expired partitions to a repository.

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

Define a retention policy rule using SQL.
```shell
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
speeds up runtime on subsequent invocations.
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
