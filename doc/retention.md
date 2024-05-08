# Retention and Expiration

## About

A data retention and expiration management subsystem for CrateDB, implementing
multiple strategies.

### Details

The application manages the life-cycle of data stored in CrateDB, handling
concerns of data expiry, size reduction, and archival. Within a system storing
and processing large amounts of data, it is crucial to manage data flows between
"hot", "warm", and "cold" storage types better than using ad hoc solutions.

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

By default, the `ext` schema is used for that, so the effective full-qualified
database table name is `"ext"."retention_policy"`. The schema is configurable
by using the `--schema` command-line option, or the `CRATEDB_EXT_SCHEMA`
environment variable.


## Strategies

This section enumerates the available data retention and expiration strategies.
More strategies can be added, and corresponding contributions are welcome.

### DELETE

A retention policy algorithm that deletes records from expired partitions.

```shell
cratedb-retention create-policy --strategy=delete \
  --table-schema=doc --table-name=raw_metrics \
  --partition-column=ts_day --retention-period=1 \
  "${CRATEDB_URI}"
```


This retention policy implements the following directive.

> **Delete** all data from the `"doc"."raw_metrics"` table, on partitions defined by
> the column `ts_day`, which is older than **1** day at the given cutoff date when
> running the retention job.

[implementation][strategy-delete] | [tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913) 


### REALLOCATE

A retention policy algorithm that reallocates expired partitions from "hot" nodes
to "warm" nodes.

Because each cluster member is assigned a designated node type by using the
`-Cnode.attr.storage=hot|warm` parameter, this strategy is only applicable in
cluster/multi-node scenarios.

On the data expiration run, corresponding partitions will get physically moved to
cluster nodes of the `warm` type, which are mostly designated archive nodes, with
large amounts of storage space.

```shell
cratedb-retention create-policy --strategy=reallocate \
  --table-schema=doc --table-name=raw_metrics \
  --partition-column=ts_day --retention-period=60 \
  --reallocation-attribute-name=storage --reallocation-attribute-value=warm \
  "${CRATEDB_URI}"
```

This retention policy implements the following directive.

> **Reallocate** data from the `"doc"."raw_metrics"` table, on partitions defined by
> the column `ts_day`, which is older than **60** days at the given cutoff date, to
> nodes tagged with the `storage=warm` attribute.

[implementation][strategy-reallocate] | [tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-building-a-hot-cold-storage-data-retention-policy/934)


### SNAPSHOT

A retention policy algorithm that snapshots expired partitions to a repository,
and prunes them from the database afterwards. It is suitable for long-term
data archival purposes.

In CrateDB jargon, a repository is a storage unit, where data in form of snapshots
can be exported to, and imported from. A repository can be located on a filesystem,
or on a supported object storage backend. CrateDB is able to use buckets on S3-compatible
storage backends, or on Azure blob storage, using the `CREATE REPOSITORY ... TYPE = 
s3|azure|fs` SQL statement.

```sql
CREATE REPOSITORY
    export_cold
TYPE
    s3
WITH (
    protocol   = 'https',
    endpoint   = 's3-store.example.org:443',
    access_key = '<USERNAME>',
    secret_key = '<PASSWORD>',
    bucket     = 'cratedb-cold-storage'
);
```
```shell
cratedb-retention create-policy --strategy=snapshot \
  --table-schema=doc --table-name=sensor_readings \
  --partition-column=time_month --retention-period=365 \
  --target-repository-name=export_cold \
  "${CRATEDB_URI}"
```

This retention policy implements the following directive.

> Run a **snapshot** on the data within the `"doc"."sensor_readings"` table, on partitions
> defined by the column `time_month`, which is older than **365** days at the given cutoff
> date, to a previously created repository called `export_cold`. Delete the corresponding
> data afterwards.

[implementation][strategy-snapshot] | [tutorial](https://community.crate.io/t/building-a-data-retention-policy-for-cratedb-with-apache-airflow/1001)


## Data model

### SQL DDL schema

The database schema for the retention policy table is defined like this.
Each record represents a rule how to expire and retain data per table.
At runtime, each record will correspond to a dedicated retention job task.

```sql
-- Set up the retention policy database table schema.
CREATE TABLE IF NOT EXISTS "ext"."retention_policy" (

    -- Strategy to apply for data retention.
    "strategy" TEXT NOT NULL,

    -- Tags: For grouping, multi-tenancy, and more.
    "tags" OBJECT(DYNAMIC),

    -- Source: The database table operated upon.
    "table_schema" TEXT,                        -- The source table schema.
    "table_name" TEXT,                          -- The source table name.
    "partition_column" TEXT NOT NULL,           -- The source table column name used for partitioning.

    -- Retention parameters.
    "retention_period" INTEGER NOT NULL,        -- Retention period in days. The number of days data gets
                                                -- retained before applying the retention policy.

    -- Target: Where data is moved/relocated to.

    -- Targeting specific nodes.
    -- You may want to designate dedicated nodes to be responsible for "hot" or "warm" storage types.
    -- To do that, you can assign attributes to specific nodes, effectively tagging them.
    -- https://crate.io/docs/crate/reference/en/latest/config/node.html#custom-attributes
    "reallocation_attribute_name" TEXT,         -- Name of the node-specific custom attribute.
    "reallocation_attribute_value" TEXT,        -- Value of the node-specific custom attribute.

    -- Targeting a repository.
    "target_repository_name" TEXT,              -- The name of a repository created with `CREATE REPOSITORY ...`.

    PRIMARY KEY ("table_schema", "table_name", "strategy")
)
CLUSTERED INTO 1 SHARDS;
```

### Record examples
You can add a retention policy by using an SQL statement like this.

```sql
-- Add a retention policy using the DELETE strategy.
INSERT INTO "ext"."retention_policy"
  (strategy, table_schema, table_name, partition_column, retention_period)
VALUES
  ('delete', 'doc', 'raw_metrics', 'ts_day', 1);
```

Using the corresponding CLI command is equivalent to the SQL statement.
```shell
cratedb-retention create-policy --strategy=delete \
  --table-schema=doc --table-name=raw_metrics \
  --partition-column=ts_day --retention-period=1 \
  "${CRATEDB_URI}"
```
After running that command, and creating a record, the program reports
about its identifier.
```
Created new retention policy: adb90608-c20f-4de7-be98-93588d8358dc
```
You can use it to delete the rule again.
```shell
cratedb-retention delete-policy --id=adb90608-c20f-4de7-be98-93588d8358dc \
  "${CRATEDB_URI}"
```

For enumerating all policies, use the `list-policies` subcommand.
```shell
cratedb-retention list-policies "${CRATEDB_URI}"
```


### Tags

By using tags, you can conveniently define groups of retention policies. This
policy is being tagged with both `foo`, and `bar`.
```shell
cratedb-retention create-policy --strategy=delete \
  --tags=foo,bar \
  --table-schema=doc --table-name=raw_metrics \
  --partition-column=ts_day --retention-period=1 \
  "${CRATEDB_URI}"
```

In order to select retention policies tagged with `foo`, use such an SQL statement.
```sql
SET error_on_unknown_object_key=false;
SELECT * FROM "ext"."retention_policy" WHERE tags['foo'] IS NOT NULL;
```

Delete all retention policy records tagged with `foo` and `bar`.
```sql
DELETE FROM "ext"."retention_policy"
       WHERE tags['foo'] IS NOT NULL
       AND   tags['bar'] IS NOT NULL;
```
```shell
cratedb-retention delete-policy --tags=foo,bar "${CRATEDB_URI}"
```

In order to use tags when running retention jobs, the program accepts an optional
command-line option like `--tags=foo,bar`.

It will limit the retention policies to all records which contain both tags, i.e.
they are combined using `AND`. Currently, there is no interface to obtain multiple
tag values, and combine them using `OR`. Running retention jobs for multiple tags
needs multiple invocations.

```shell
cratedb-retention run --strategy=delete --tags=foo,bar "${CRATEDB_URI}"
```


## Install

The CrateDB data retention and expiration toolkit program `cratedb-retention` can be
installed by using the corresponding Python package, or it can alternatively be invoked
on the command line using Podman or Docker.

For installing the package, please refer to the [install documentation
section](#install).

```shell
cratedb-retention --version
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

By using the `--verbose` option, directly after the main command, before the
subcommand, you can inspect the SQL statements issued to the database.
```shell
cratedb-retention --verbose setup "${CRATEDB_URI}"
```

By using the `--dry-run` option, after the subcommand, the program will not
issue altering or modifying SQL statements to the database. You can use it
to learn about which actions would be performed.
```shell
cratedb-retention setup --dry-run "${CRATEDB_URI}"
```

The `DBURI` command-line argument can be omitted when defining the `CRATEDB_URI`
environment variable.
```shell
export CRATEDB_URI='crate://localhost/'
```

There are also a few shortcuts for subcommands. For example, the shortest form of
incantation for deleting a retention policy would be:
```shell
cratedb-retention rm --tags=baz
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

Add a retention policy rule.
```shell
cratedb-retention create-policy --strategy=delete \
  --table-schema=doc --table-name=raw_metrics \
  --partition-column=ts_day --retention-period=1 \
  "${CRATEDB_URI}"
```

Invoke the data retention job, expiring all data older than one day.
```shell
cratedb-retention run --strategy=delete "${CRATEDB_URI}"
```

Invoke the data retention job, expiring all data older than one day before
a specific cutoff date.
```shell
cratedb-retention run --cutoff-day=2023-06-27 --strategy=delete "${CRATEDB_URI}"
```

Simulate the data retention job, display SQL statements only.
```shell
cratedb-retention run --dry-run --strategy=delete "${CRATEDB_URI}"
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

Add a retention policy rule using the DELETE strategy.
```shell
docker run --rm -i --network=host "${OCI_IMAGE}" \
cratedb-retention create-policy --strategy=delete \
  --table-schema=doc --table-name=raw_metrics \
  --partition-column=ts_day --retention-period=1 \
  "${CRATEDB_URI}"
```

Invoke the data retention job.
```shell
docker run --rm -i --network=host "${OCI_IMAGE}" \
cratedb-retention run --strategy=delete "${CRATEDB_URI}"
```


### Library use

This section outlines how to use the toolkit as a library within your own
applications. The code displayed below is a stripped-down version of the
runnable example program [`examples/retention_retire_cutoff.py`], located
within this repository.

```python
from cratedb_toolkit.retention.core import RetentionJob
from cratedb_toolkit.retention import JobSettings, RetentionPolicy, RetentionStrategy
from cratedb_toolkit.retention.setup import setup_schema
from cratedb_toolkit.retention.store import RetentionPolicyStore
from cratedb_toolkit.model import DatabaseAddress

# Define the database URI to connect to.
DBURI = "crate://localhost/"

# A. Set up adapter to retention policy store.
settings = JobSettings(
    database=DatabaseAddress.from_string(DBURI),
)
store = RetentionPolicyStore(settings=settings)

# B. Initialize the subsystem, and create a data retention policy.
# TODO: Refactor to `RetentionPolicyStore`.
setup_schema(settings=settings)

# Add a basic retention policy.
policy = RetentionPolicy(
    strategy=RetentionStrategy.DELETE,
    table_schema="doc",
    table_name="raw_metrics",
    partition_column="ts_day",
    retention_period=1,
)
identifier = store.create(policy, ignore="DuplicateKeyException")

# C. Define job settings, and invoke the data retention job.
settings = JobSettings(
    database=DatabaseAddress.from_string(DBURI),
    strategy=RetentionStrategy.DELETE,
)

job = RetentionJob(settings=settings)
job.start()
```


[CrateDB]: https://crate.io/products/cratedb
[CrateDB Cloud]: https://console.cratedb.cloud/
[downsampling]: https://docs.victoriametrics.com/#downsampling
[downsampling a time series data stream]: https://www.elastic.co/guide/en/elasticsearch/reference/current/downsampling.html
[downsampling and data retention]: https://docs.influxdata.com/influxdb/v1.8/guides/downsample_and_retain/
[`examples/retention_retire_cutoff.py`]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/examples/retention_retire_cutoff.py
[rolling up historical data]: https://www.elastic.co/guide/en/elasticsearch/reference/current/rollup-overview.html
[roll-up]: https://en.wikipedia.org/wiki/OLAP_cube#Operations
[strategy-delete]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/cratedb_toolkit/retention/strategy/delete.py
[strategy-reallocate]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/cratedb_toolkit/retention/strategy/reallocate.py
[strategy-snapshot]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/cratedb_toolkit/retention/strategy/snapshot.py
[time bucketing]: https://community.crate.io/t/resampling-time-series-data-with-date-bin/1009
