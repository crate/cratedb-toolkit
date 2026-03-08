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
A polyglot and universal I/O pipeline subsystem that covers data transfer from
and to [AWS DMS], [Databricks], [DuckDB], [DynamoDB], [InfluxDB],
[MongoDB], [MongoDB Atlas], [MotherDuck], [PostgreSQL], and many more
streaming sources, databases, and data platforms or services with
[CrateDB] and [CrateDB Cloud].
:::

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

Use the Python API to import or export data.

```python
from cratedb_toolkit import DatabaseCluster, InputOutputResource

with DatabaseCluster.from_params(cluster_url="crate://crate:crate@cratedb.example.org:4200/schema/table") as cluster:
    cluster.load_table(source=InputOutputResource(url="protocol://username:password@hostname:port/resource"))
    cluster.save_table(target=InputOutputResource(url="protocol://username:password@hostname:port/resource"))
```


## General notes

:::{rubric} URLs everywhere
:::

The I/O subsystem uses URLs across the board to address data sources and sinks.

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
```
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
[InfluxDB]: https://github.com/influxdata/influxdb
[MongoDB]: https://github.com/mongodb/mongo
[MongoDB Atlas]: https://www.mongodb.com/atlas
[MotherDuck]: https://motherduck.com/
[PostgreSQL]: https://www.postgresql.org/
