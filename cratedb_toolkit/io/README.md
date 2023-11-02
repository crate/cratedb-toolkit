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


[CrateDB Cloud]: https://console.cratedb.cloud/
