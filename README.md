# Data retention and expiration manager for CrateDB

[![Tests](https://github.com/crate-workbench/cratedb-retention/actions/workflows/main.yml/badge.svg)](https://github.com/crate-workbench/cratedb-retention/actions/workflows/main.yml)

## About

A data retention and expiration management subsystem for CrateDB, implementing
different retention strategies.

The application manages the life-cycle of data stored in CrateDB, handling
concerns of data expiry, size reduction, and archival. Within a system storing
and processing large amounts of data, it is crucial to manage data flows between
hot and cold storage types better than using ad hoc solutions.

Data retention policies can be flexibly configured by adding records to the
retention policy database table, which is also stored within CrateDB.

### Background

With other databases, this technique, or variants thereof, are known as [rolling up
historical data], [downsampling a time series data stream], [downsampling and data
retention], or just [downsampling].

They are useful to reduce the storage size of historical data by decreasing its
resolution, where this is acceptable related to your needs.

> ES: The Rollup functionality summarizes old, high-granularity data into a reduced
> granularity format for long-term storage. By "rolling" the data up, historical
> data can be compressed greatly compared to the raw data.

### Side looks

In classical OLAP operations,
> a [roll-up] involves summarizing the data along a dimension. The summarization
> rule might be an aggregate function, such as computing totals along a hierarchy
> or by applying a set of formulas.

In classical dashboarding applications, reducing the amount of timeseries-data
_on-demand_ is also applicable. 
> When rendering data to your screen, and its density is larger than the amount
> of pixels available to display, data is reduced by using [time bucketing] for
> grouping records into equal-sized time ranges, before applying a resampling
> function on it. The width of those time ranges is usually automatically derived
> from the zoom level, i.e. the total time range the user is looking at.

Contrary to other techniques which may compute data _on-demand_, the operations
performed by this application **permanently** reduce the amount, size, or
resolution of data.

### Details

> The retention policy database table is also stored within CrateDB.

By default, the `ext` schema is used for that, so the effective full-qualified database
table name is `"ext"."retention_policy"`. It is configurable by using the `--schema`
command-line option, or the `CRATEDB_EXT_SCHEMA` environment variable.


## Strategies

This section enumerates the available data retention and expiration strategies.
More strategies can be added, and corresponding contributions are welcome.

### DELETE

A basic retention policy algorithm that drops records from expired partitions.

```sql
-- A policy using the DELETE strategy.
INSERT INTO "ext"."retention_policy"
  (table_schema, table_name, partition_column, retention_period, strategy)
VALUES
  ('doc', 'raw_metrics', 'ts_day', 1, 'delete');
```

[implementation](cratedb_retention/strategy/delete.py) | [tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913) 

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
INSERT INTO "ext"."retention_policy"
VALUES
  ('doc', 'raw_metrics', 'ts_day', 60, 'storage', 'cold', NULL, 'reallocate');
```

[implementation](cratedb_retention/strategy/reallocate.py) | [tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-building-a-hot-cold-storage-data-retention-policy/934)

### SNAPSHOT

A retention policy algorithm that snapshots expired partitions to a repository,
and prunes them from the database afterwards. It is suitable for long-term
data archival purposes.

In CrateDB jargon, a repository is a bucket on an S3-compatible object store,
where data in form of snapshots can be exported to, and imported from.

```sql
-- A policy using the SNAPSHOT strategy.
INSERT INTO "ext"."retention_policy"
  (table_schema, table_name, partition_column, retention_period, target_repository_name, strategy)
VALUES
  ('doc', 'sensor_readings', 'time_month', 365, 'export_cold', 'snapshot');
```

[implementation](cratedb_retention/strategy/snapshot.py) | [tutorial](https://community.crate.io/t/building-a-data-retention-policy-for-cratedb-with-apache-airflow/1001)


## Install

The CrateDB data retention and expiration toolkit program `cratedb-retention` can be
installed by using the corresponding Python package, or it can alternatively be invoked
on the command line using Podman or Docker.

Install package.
```shell
pip install --upgrade git+https://github.com/crate-workbench/cratedb-retention
```

Run with Docker.
```shell
docker run --rm "ghcr.io/crate-workbench/cratedb-retention" cratedb-retention --version
```


## Usage

The toolkit can be used both as a standalone program, and as a library.

### Command-line use

This section outlines how to connect to, and run data retention jobs, both
on a database cluster running on your premises, and on [CrateDB Cloud].

The steps are the same, only the connection parameters are different.
```{todo}
Note that, currently, the Python driver and the crash database
shell need to obtain slightly different parameters. 
```

#### Workstation / on-premise

```shell
export CRATEDB_URI='crate://localhost/'
export CRATEDB_HOST='http://localhost:4200/'
```

#### CrateDB Cloud

```shell
export CRATEDB_URI='crate://admin:<PASSWORD>@<CLUSTERNAME>.aks1.eastus2.azure.cratedb.net:4200?ssl=true'
export CRATEDB_HOST='https://admin:<PASSWORD>@<CLUSTERNAME>.aks1.eastus2.azure.cratedb.net:4200/'
```

#### General

Install retention policy bookkeeping tables.
```shell
cratedb-retention setup "${CRATEDB_URI}"
```

Add a retention policy rule using SQL.
```shell
# A policy using the DELETE strategy.
crash --hosts "${CRATEDB_HOST}" <<SQL
    INSERT INTO "ext"."retention_policy"
      (table_schema, table_name, partition_column, retention_period, strategy)
    VALUES
      ('doc', 'raw_metrics', 'ts_day', 1, 'delete');
SQL
```

Invoke the data retention job, using a specific cut-off date.
```shell
cratedb-retention run --cutoff-day=2023-06-27 --strategy=delete "${CRATEDB_URI}"
```

#### Podman or Docker

This section offers a complete walkthrough, how work with the `cratedb-retention`
program interactively using Podman or Docker, step by step. It starts a single-
node CrateDB instance on your workstation for demonstration purposes. Please note
that some retention policy strategies will not work on single-node installations.

```shell
export CRATEDB_URI='crate://localhost/'
export CRATEDB_HOST='http://localhost:4200/'
export OCI_IMAGE='ghcr.io/crate-workbench/cratedb-retention:nightly'
```

Display version number.
```shell
docker run --rm -i --network=host "${OCI_IMAGE}" \
cratedb-retention --version
```

Start CrateDB.
```shell
docker run --rm -it \
  --name=cratedb \
  --publish=4200:4200 --publish=5432:5432 \
  --env=CRATE_HEAP_SIZE=4g crate \
  -Cdiscovery.type=single-node
```

Install retention policy bookkeeping tables.
```shell
docker run --rm -i --network=host "${OCI_IMAGE}" \
cratedb-retention setup "${CRATEDB_URI}"
```

Add a retention policy rule using SQL.
```shell
# A policy using the DELETE strategy.
docker run --rm -i --network=host "${OCI_IMAGE}" \
crash --hosts "${CRATEDB_HOST}" <<SQL
    INSERT INTO "ext"."retention_policy"
      (table_schema, table_name, partition_column, retention_period, strategy)
    VALUES
      ('doc', 'raw_metrics', 'ts_day', 1, 'delete');
SQL
```

Invoke the data retention job, using a specific cut-off date.
```shell
docker run --rm -i --network=host "${OCI_IMAGE}" \
cratedb-retention run --cutoff-day=2023-06-27 --strategy=delete "${CRATEDB_URI}"
```


### Library use

This section outlines how to use the toolkit as a library within your own
applications. The code displayed below is a stripped-down version of the
runnable example program [`examples/basic.py`], located within this repository.

```python
from cratedb_retention.core import RetentionJob
from cratedb_retention.model import DatabaseAddress, JobSettings, RetentionStrategy
from cratedb_retention.setup.schema import setup_schema
from cratedb_retention.util.database import run_sql


# Define the database URI to connect to.
DBURI = "http://localhost/"


# A. Initialize the subsystem, and create a data retention policy.
settings = JobSettings(
    database=DatabaseAddress.from_string(DBURI),
)
setup_schema(settings=settings)

sql = """
-- A policy using the DELETE strategy.
INSERT INTO "ext"."retention_policy"
  (table_schema, table_name, partition_column, retention_period, strategy)
VALUES
  ('doc', 'raw_metrics', 'ts_day', 1, 'delete');
"""
run_sql(DBURI, sql)


# B. Define job settings, and invoke the data retention job.
settings = JobSettings(
    database=DatabaseAddress.from_string(DBURI),
    strategy=RetentionStrategy.DELETE,
    cutoff_day="2023-06-27",
)

job = RetentionJob(settings=settings)
job.start()
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
git clone https://github.com/crate-workbench/cratedb-retention
cd cratedb-retention
```

Install project in sandbox mode.
```shell
pip install --editable=.[develop,test]
```

Run tests. `TC_KEEPALIVE` keeps the auxiliary service containers running, which
speeds up runtime on subsequent invocations. Note that the test suite uses the
`testdrive-ext` schema for storing the retention policy table, and the
`testdrive-data` schema for storing data tables.
```shell
export TC_KEEPALIVE=true
poe check
```

Format code.
```shell
poe format
```


[CrateDB Cloud]: https://console.cratedb.cloud/
[downsampling]: https://docs.victoriametrics.com/#downsampling
[downsampling a time series data stream]: https://www.elastic.co/guide/en/elasticsearch/reference/current/downsampling.html
[downsampling and data retention]: https://docs.influxdata.com/influxdb/v1.8/guides/downsample_and_retain/
[`examples/basic.py`]: examples/basic.py
[rolling up historical data]: https://www.elastic.co/guide/en/elasticsearch/reference/current/rollup-overview.html
[roll-up]: https://en.wikipedia.org/wiki/OLAP_cube#Operations
[time bucketing]: https://community.crate.io/t/resampling-time-series-data-with-date-bin/1009
