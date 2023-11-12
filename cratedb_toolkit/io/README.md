# Load and extract data into/from CrateDB


## About

A one-stop command `ctk load table` to load data into CrateDB database tables.


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
Load data.
```shell
ctk load table https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_weather.csv.gz
ctk load table https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_marketing.json.gz
ctk load table https://github.com/daq-tools/skeem/raw/main/tests/testdata/basic.parquet
```

Inquire data.
```shell
ctk shell --command="SELECT * FROM data_weather LIMIT 10;"
ctk shell --command="SELECT * FROM data_weather LIMIT 10;" --format=json
```

### Backlog
- Exercise data imports from AWS S3 and other Object Storage providers.


## InfluxDB

Using the adapter to [influxio], you can transfer data from InfluxDB to CrateDB.

Import two data points into InfluxDB.
```shell
export INFLUX_ORG=example
export INFLUX_TOKEN=token
export INFLUX_BUCKET_NAME=testdrive
export INFLUX_MEASUREMENT=demo
influx bucket create
influx write --precision=s "${INFLUX_MEASUREMENT},region=amazonas temperature=42.42,humidity=84.84 1556896326"
influx write --precision=s "${INFLUX_MEASUREMENT},region=amazonas temperature=45.89,humidity=77.23,windspeed=5.4 1556896327"
influx query "from(bucket:\"${INFLUX_BUCKET_NAME}\") |> range(start:-100y)"
```

Transfer data.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
crash --command "SELECT * FROM testdrive.demo;"
```

Todo: More convenient table querying.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk shell --command "SELECT * FROM testdrive.demo;"
ctk show table "testdrive.demo"
```


[CrateDB Cloud]: https://console.cratedb.cloud/
[influxio]: https://github.com/daq-tools/influxio
