(io)=
(io-subsystem)=

# I/O Subsystem

Load and extract data into/from CrateDB.

## About

Transfer data from [AWS DMS], [Databricks], [DuckDB], [DynamoDB], [InfluxDB],
[MongoDB], [MongoDB Atlas], [MotherDuck], [PostgreSQL], and many more databases,
platforms, and services to [CrateDB] or [CrateDB Cloud].

## What's inside

A one-stop command, `ctk load table`, to load data into database tables.

## Installation

Latest release.
```shell
pip install --upgrade 'cratedb-toolkit[all]'
```

Development version.
```shell
pip install --upgrade 'cratedb-toolkit[all] @ git+https://github.com/crate/cratedb-toolkit.git'
```

## General Notes

By default, the table name will be derived from the name of the input resource.
If you would like to specify a different table name, use the `--table` command
line option, or the `CRATEDB_TABLE` environment variable.

When aiming to write into a table in a different database schema, use the
`--schema` command line option, or the `CRATEDB_SCHEMA` environment variable.
When omitting this parameter, the default value `doc` will be used.


## Cloud Import API

Using the [CrateDB Cloud] Import API, you can import files in CSV, JSON, and
Parquet formats.

### Prerequisites
Authenticate with CrateDB Cloud using one of those identity providers:
Cognito, Azure AD, GitHub, Google.
```shell
croud login --idp azuread
```

To discover the list of available database clusters.
```shell
croud clusters list
```

Define the cluster id of your CrateDB Cloud Cluster you are aiming to connect
to, and its connection credentials.
```shell
export CRATEDB_CLUSTER_ID='<YOUR_CLUSTER_ID>'
```

### Usage
Load data into database table.
```shell
ctk load table 'https://cdn.crate.io/downloads/datasets/cratedb-datasets/cloud-tutorials/data_weather.csv.gz'
ctk load table 'https://cdn.crate.io/downloads/datasets/cratedb-datasets/cloud-tutorials/data_marketing.json.gz'
ctk load table 'https://cdn.crate.io/downloads/datasets/cratedb-datasets/timeseries/yc.2019.07-tiny.parquet.gz'
```

Query and aggregate data using SQL.
```shell
ctk shell --command="SELECT * FROM data_weather LIMIT 10;"
ctk shell --command="SELECT * FROM data_weather LIMIT 10;" --format=csv
ctk shell --command="SELECT * FROM data_weather LIMIT 10;" --format=json
```


```{toctree}
:maxdepth: 2
:hidden:

AWS DMS <dms/index>
DynamoDB <dynamodb/index>
InfluxDB <influxdb/index>
Ingestr <ingestr/index>
MongoDB <mongodb/index>
PostgreSQL <postgresql/index>
```


[AWS DMS]: https://aws.amazon.com/dms/
[CrateDB]: https://github.com/crate/crate
[CrateDB Cloud]: https://console.cratedb.cloud/
[Databricks]: https://www.databricks.com/
[DuckDB]: https://github.com/duckdb/duckdb
[DynamoDB]: https://aws.amazon.com/dynamodb/
[InfluxDB]: https://github.com/influxdata/influxdb
[MongoDB]: https://github.com/mongodb/mongo
[MongoDB Atlas]: https://www.mongodb.com/atlas
[MotherDuck]: https://motherduck.com/
[PostgreSQL]: https://www.postgresql.org/
