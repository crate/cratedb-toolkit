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
data between any source and any destination. It supports many data
sources, destinations, and data loading strategies out of the box.

Adapters for CrateDB let you migrate data from any proprietary enterprise
data warehouse or database to [CrateDB] or [CrateDB Cloud], to consolidate
infrastructure and save operational costs.

The polyglot pipeline subsystem covers data transfer from and to
[AWS DMS], [Databricks], [DuckDB], [DynamoDB], [InfluxDB],
[MongoDB], [MongoDB Atlas], [MotherDuck], [PostgreSQL],
and many more streaming sources, databases, and data platforms or
services with [CrateDB] and [CrateDB Cloud].
For a full list of integrations,
see {ref}`I/O adapter coverage <io-coverage>`.
:::

## Synopsis

You can run jobs from the command-line or by using the Python API.

### CLI

The CLI entrypoints to the I/O subsystem are the `ctk load table`
and `ctk save table` commands.

Load data from external resource into CrateDB.
```shell
ctk load table \
  'protocol://username:password@hostname:port/resource' \
  --cluster-url='crate://crate:crate@cratedb.example.org:4200/schema/table'
```

Save data from CrateDB to external resource.
```shell
ctk save table \
  --cluster-url='crate://crate:crate@cratedb.example.org:4200/schema/table' \
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
sections. Support for I/O adapter types is currently divided into two
families defined by Python package extras `io-curated` and `io-ingest`,
which are mutually exclusive to each other.

Support for files, open table formats, InfluxDB, and MongoDB.
```shell
uv tool install --upgrade 'cratedb-toolkit[io-curated]'
```

Support for other databases, streams, platforms, and services.
```shell
uv tool install --upgrade 'cratedb-toolkit[io-ingest]'
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
configure credentials across the board, for example by using the
`--cluster-url` CLI option.
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
--cluster-url='crate://crate:crate@cratedb.example.org:4200/schema/table?ssl=true'
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

Adapters of the `io-ingest` family support [incremental loading], which means
you can choose to append, merge, or delete+insert data into the destination
table using different strategies.

Incremental loading allows you to ingest only the new rows from the source
table into the destination table, which means that you do not have
to load the entire table every time you run the data migration procedure.

This comes at a minor cost that a few bookkeeping columns exist in the target
table, however that is rarely an issue.

(io-coverage)=
## Coverage

Supported data formats, database types, data platforms, analytics engines,
and other services.

:**File formats**:
  CSV, JSONL/NDJSON, Parquet

:**Open table formats**:
  Apache Iceberg, DeltaLake

:**Cloud storage**:
 Amazon S3, Azure Cloud Storage, Google Cloud Storage (GCS)

:**Databases**:
  Actian Data Platform, Actian X, Amazon Athena, Amazon Redshift,
  Apache Drill, Apache Druid, Apache Hive and Presto, Apache Solr,
  Clickhouse, CockroachDB, CrateDB, Databend, Databricks, Denodo, DuckDB, EXASOL DB,
  Elasticsearch, Firebird, Firebolt, Google BigQuery, Google Sheets, Greenplum,
  HyperSQL (hsqldb), IBM DB2 and Informix, IBM Netezza Performance Server, Impala, Ingres,
  Kinetica, Microsoft Access, Microsoft SQL Server, MonetDB, MongoDB, MySQL and MariaDB,
  OpenGauss, OpenSearch, Oracle, PostgreSQL, Rockset, SAP ASE, SAP HANA,
  SAP Sybase SQL Anywhere, Snowflake, SQLite, Teradata Vantage, TiDB, Vector, YDB,
  YugabyteDB

:**Streams**:
  Amazon Kinesis, Apache Kafka (Amazon MSK, Confluent Kafka, Redpanda, RobustMQ)

:**Services**:
  Airtable, Asana, Facebook Ads, GitHub, Google Ads, Google Analytics,
  Google Sheets, Jira, HubSpot, Linear, LinkedIn Ads, Mailchimp, Mixpanel,
  Notion, Personio, Pinterest, Pipedrive, Salesforce, Shopify, Slack, Stripe,
  TikTok Ads, Zendesk, Zoom, etc.



```{toctree}
:maxdepth: 1
:hidden:

file/index
database/index
stream/index
service/index
open-table/index
```
```{toctree}
:maxdepth: 1
:hidden:
managed/index
```


[AWS DMS]: https://aws.amazon.com/dms/
[CrateDB]: https://github.com/crate/crate
[Databricks]: https://www.databricks.com/
[DuckDB]: https://github.com/duckdb/duckdb
[DynamoDB]: https://aws.amazon.com/dynamodb/
[incremental loading]: https://bruin-data.github.io/ingestr/getting-started/incremental-loading.html
[InfluxDB]: https://github.com/influxdata/influxdb
[MongoDB]: https://github.com/mongodb/mongo
[MongoDB Atlas]: https://www.mongodb.com/atlas
[MotherDuck]: https://motherduck.com/
[PostgreSQL]: https://www.postgresql.org/
