(io)=
(io-subsystem)=

# CrateDB I/O Subsystem

:::{include} /_snippet/links.md
:::

:::{div} sd-text-muted
Import and export data into/from CrateDB.
:::

## About

:::{div}
A versatile data I/O framework and command-line application to copy
data between CrateDB and a curated set of sources and destinations,
with support for multiple data loading strategies out of the box.

The curated adapters let you migrate data into [CrateDB] or [CrateDB Cloud]
to consolidate infrastructure and save operational costs.

The polyglot pipeline subsystem covers data transfer from and to
[AWS DMS], [DynamoDB], [InfluxDB], [MongoDB], and [MongoDB Atlas],
with [CrateDB] and [CrateDB Cloud].
For a full list of integrations,
see {ref}`I/O adapter coverage <io-coverage>`.
:::

## Synopsis

You can run jobs from the command-line or by using the Python API.

### CLI

The CLI entrypoints to the I/O subsystem are the `ctk load`
and `ctk save` commands.

Load data from external resource into CrateDB.
```shell
ctk load \
  'protocol://username:password@hostname:port/resource' \
  'crate://crate:crate@cratedb.example.org:4200/schema/table'
```

Save data from CrateDB to external resource.
```shell
ctk save \
  'crate://crate:crate@cratedb.example.org:4200/schema/table' \
  'protocol://username:password@hostname:port/resource'
```

### Python API

Alternatively, use the Python API to import or export data.

```python
from cratedb_toolkit import DatabaseCluster, InputOutputResource

# Connect to CrateDB database cluster.
with DatabaseCluster.from_params(cluster_url="crate://crate:crate@cratedb.example.org:4200/schema/table") as cluster:

    # Load data from external resource into CrateDB.
    cluster.load_table(source=InputOutputResource(url="protocol://username:password@hostname:port/resource"))

    # Save data from CrateDB to external resource.
    cluster.save_table(target=InputOutputResource(url="protocol://username:password@hostname:port/resource"))
```

:::{include} /_snippet/install-ctk.md
:::

:::{rubric} Special considerations
:::

Individual I/O adapters need different sets of dependency packages, please
consult relevant installation notes in the corresponding documentation
sections. Support for the curated I/O adapter types is bundled into the
Python package extra `io-curated`.

Support for files, open table formats, InfluxDB, and MongoDB.
```shell
uv tool install --upgrade 'cratedb-toolkit[io-curated]'
```

Support for streaming data via Amazon Kinesis (see {ref}`Streams <io-stream>`).
```shell
uv tool install --upgrade 'cratedb-toolkit[kinesis]'
```

Alternatively, use Docker or Podman to invoke the container image.
```shell
docker run --rm ghcr.io/crate/cratedb-toolkit ctk --version
```
```shell
docker run --rm ghcr.io/crate/cratedb-toolkit-ingest ctk --version
```

## General notes

:::{rubric} URLs everywhere
:::

The I/O subsystem uses URLs across the board to address data sources and sinks.

:::{rubric} Authentication
:::

**External:** Different data pipeline elements use their specific way to
configure access credentials or tokens, using individual parameters.

**CrateDB:** CrateDB as a pipeline source or sink element uses the same way to
configure credentials across the board.
Please note you **must** specify a password. If your account does not use a
password, use a random string or just `na`.

:::{rubric} Address arbitrary resources
:::

The resource address will be picked from the resource locator URL path
`/resource`, which has different semantics based on the adapter type.
It can be a table name, a bucket name and object path, or anything else
that identifies an URL-based resource uniquely within the namespace of
the base URL.

Some adapter types also accept the `?table=` URL query parameter that
can optionally encode two components separated by a dot, like
`database.table` or `database.collection`. Others encode the database
name into the `hostname` fragment of the URL.

Please consult individual adapter documentation pages to
learn about available URL parameters and differences.

:::{rubric} Address CrateDB schema and table
:::

CrateDB schema and table names will be picked from the resource locator
URL path `/schema/table`.

When addressing CrateDB as a data sink, and omitting those parameters, the
target table address will be derived from the address of the data source.
When addressing CrateDB as a data source, the source table parameter is
obligatory.

If you would like to specify the table name differently, use the `?table=` URL
query parameter, the `--table` command line option, or the `CRATEDB_TABLE`
environment variable.

If you want to target a different database schema, use the `?schema=` URL
query parameter, the `--schema` command line option, or the `CRATEDB_SCHEMA`
environment variable. If this parameter is not defined, CrateDB's default
schema `doc` will be used.

:::{rubric} Connect to CrateDB using SSL
:::

Use the `?ssl=true` query parameter, and replace username, password, and
hostname with values matching your environment. Also use this variant to
connect to CrateDB Cloud.
```text
'crate://crate:crate@cratedb.example.org:4200/schema/table?ssl=true'
```

:::{rubric} Transfer multiple resources
:::

Currently, the pipeline system can transfer single resources / tables with most
of the I/O adapter types, and multiple resources / catalogs / collections with
some adapter types. A few file-based adapters provide file globbing, and
the MongoDB I/O adapter permits transfer of whole MongoDB databases,
including multiple collections.

This detail (resource globbing and selection) will be improved in future
iterations across the board. In the meanwhile, please iterate all sibling
resources in a loop where multi-resource selection is not possible yet,
i.e. transfer table by table.

:::{rubric} Incremental loading
:::

Incremental loading (appending, merging, or delete+insert of only the new rows
from the source table) is currently not supported. The table-oriented I/O adapters
are full-load only for now; incremental loading is tracked as backlog. This does not
apply to the streaming adapters (see {ref}`Streams <io-stream>`), which process records
continuously rather than as a one-shot full load.

:::{rubric} Remote scheduling
:::

To schedule your workload on a remote dask cluster, define the
`DASK_SCHEDULER_ADDRESS` environment variable.
```shell
export DASK_SCHEDULER_ADDRESS='tcp://127.0.0.1:8786'
```

(io-coverage)=
## Coverage

Supported data formats, database types, data platforms, and analytics engines.

:**File formats**:
  CSV, JSONL/NDJSON, Parquet

:**Open table formats**:
  Apache Iceberg, DeltaLake

:**Cloud storage**:
 Amazon S3, Azure Cloud Storage, Google Cloud Storage (GCS)

:**Databases**:
  InfluxDB, MongoDB, MongoDB Atlas

:**Streams**:
  Amazon Kinesis (via AWS DMS)

:::{note}
Until recently, this list also advertised a much wider catalog of databases,
data warehouses, streams, and services (Salesforce, Snowflake, BigQuery, MySQL,
PostgreSQL, Kafka, Elasticsearch, Databricks, and about 50 more), reachable
through a bundled `ingestr` dependency. Auditing that integration found real,
tested `cratedb-toolkit` code for exactly one of them: PostgreSQL. The rest only
appeared to work because `ingestr`'s own source factory silently accepted the URL
scheme, without any `cratedb-toolkit`-specific code or tests behind it.

`ingestr` has since been removed, along with the documentation pages for those
never-implemented sources. Generic SQL-source ingest — including the PostgreSQL
full-load that `ingestr` used to provide — was dropped with it and not rebuilt.
The whole catalog is backlog: a dedicated adapter (and its documentation) will be
built for a given source once a real request comes in, rather than ported
speculatively ahead of time.
:::



```{toctree}
:maxdepth: 1
:hidden:

file/index
database/index
stream/index
open-table/index
```
```{toctree}
:maxdepth: 1
:hidden:
managed/index
```


[AWS DMS]: https://aws.amazon.com/dms/
[DynamoDB]: https://aws.amazon.com/dynamodb/
[InfluxDB]: https://github.com/influxdata/influxdb
[MongoDB]: https://github.com/mongodb/mongo
[MongoDB Atlas]: https://www.mongodb.com/atlas
