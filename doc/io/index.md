(io)=
(io-subsystem)=

# I/O Subsystem

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

Individual I/O adapters need different sets of dependency packages, please
consult relevant installation notes in the corresponding documentation
sections. To cover most I/O adapter type families with two single installation
commands, use the `io-curated` or `io-ingest` extra when installing CrateDB
Toolkit. Both variants are currently mutually exclusive to each other.

Support for files, open table formats, InfluxDB, and MongoDB.
```shell
uv tool install --upgrade 'cratedb-toolkit[io-curated]'
```

Support for other databases, streams, platforms, and services.
```shell
uv tool install --upgrade 'cratedb-toolkit[io-ingest]'
```

## Synopsis

You can run jobs from the command-line or by using the Python API.

### CLI

The CLI entrypoints to the I/O subsystem are the `ctk load table`
and `ctk save table` commands.

Load data from external resource into CrateDB.
```shell
ctk load table \
  'protocol://username:password@hostname:port/catalog/resource' \
  --cluster-url='crate://crate:crate@cratedb.example.org:4200/schema/table'
```

Save data from CrateDB to external resource.
```shell
ctk save table \
  --cluster-url='crate://crate:crate@cratedb.example.org:4200/schema/table' \
  'protocol://username:password@hostname:port/catalog/resource'
```

### Python API

Use the Python API to import or export data.

```python
from cratedb_toolkit import DatabaseCluster, InputOutputResource

with DatabaseCluster.from_params(cluster_url="crate://crate:crate@cratedb.example.org:4200/schema/table") as cluster:
    cluster.load_table(source=InputOutputResource(url="protocol://username:password@hostname:port/catalog/resource"))
    cluster.save_table(target=InputOutputResource(url="protocol://username:password@hostname:port/catalog/resource"))
```


## General notes

:::{rubric} URLs everywhere
:::

The I/O subsystem uses URLs across the board to address data sources and sinks.

:::{rubric} Address arbitrary resources
:::

The resource address will be picked from the URL path of the corresponding
resource locator `/catalog/resource`. Based on the type of the data source
or sink, those parameters have different semantic meanings. Sometimes, also
the `hostname` portion needs to be considered.

:::{rubric} Address CrateDB schema and table
:::

The CrateDB schema and table names will be picked from the URL path
of the corresponding resource locator `/schema/table`.
When addressing CrateDB as a data sink, and omitting those parameters, the
target table address will be derived from the address of the data source.

If you would like to specify the table name differently, use the `--table`
command line option, or the `CRATEDB_TABLE` environment variable.

When aiming to write into a table in a different database schema, use the
`--schema` command line option, or the `CRATEDB_SCHEMA` environment variable.
If this parameter is not defined, CrateDB's default schema `doc` will be used.

:::{rubric} Address CrateDB with SSL
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
of the I/O adapter types. Only the MongoDB I/O adapter permits transfer of
MongoDB collections to CrateDB, including multiple tables. This detail will be
improved in future iterations.



```{toctree}
:maxdepth: 2
:hidden:

AWS DMS <dms/index>
DeltaLake <deltalake/index>
DynamoDB <dynamodb/index>
Iceberg <iceberg/index>
InfluxDB <influxdb/index>
Ingestr <ingestr/index>
MongoDB <mongodb/index>
PostgreSQL <postgresql/index>
```
```{toctree}
:maxdepth: 2
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
