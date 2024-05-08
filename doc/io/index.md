# I/O Subsystem

Load and extract data into/from CrateDB.

## About
Using the InfluxDB and MongoDB I/O subsystems, you can transfer data from
[InfluxDB] and [MongoDB] to [CrateDB] and [CrateDB Cloud].

## What's inside
A one-stop command `ctk load table` to load data into database tables.


## Installation

Latest release.
```shell
pip install --upgrade 'cratedb-toolkit[all]'
```

Development version.
```shell
pip install --upgrade 'cratedb-toolkit[all] @ git+https://github.com/crate-workbench/cratedb-toolkit.git'
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
export CRATEDB_CLOUD_CLUSTER_ID='e1e38d92-a650-48f1-8a70-8133f2d5c400'
```

Define authentication credentials, to satisfy the `ctk shell` command you will
use to inspect the database content after transferring data.
```shell
export CRATEDB_USERNAME='admin'
export CRATEDB_PASSWORD='3$MJ5fALP8bNOYCYBMLOrzd&'
```

### Usage
Load data into database table.
```shell
ctk load table 'https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_weather.csv.gz'
ctk load table 'https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_marketing.json.gz'
ctk load table 'https://github.com/crate/cratedb-datasets/raw/main/timeseries/yc.2019.07-tiny.parquet.gz'
```

Query and aggregate data using SQL.
```shell
ctk shell --command="SELECT * FROM data_weather LIMIT 10;"
ctk shell --command="SELECT * FROM data_weather LIMIT 10;" --format=csv
ctk shell --command="SELECT * FROM data_weather LIMIT 10;" --format=json
```

### Backlog
- Exercise data imports from AWS S3 and other Object Storage providers.


```{toctree}
:maxdepth: 2
:hidden:

InfluxDB <influxdb/index>
MongoDB <mongodb/index>
```


[CrateDB]: https://github.com/crate/crate
[CrateDB Cloud]: https://console.cratedb.cloud/
[InfluxDB]: https://github.com/influxdata/influxdb
[MongoDB]: https://github.com/mongodb/mongo
