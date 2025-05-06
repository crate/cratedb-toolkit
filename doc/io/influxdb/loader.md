(influxdb-loader)=
# InfluxDB Table Loader

## About
Load data from InfluxDB into CrateDB using one-stop commands
`ctk load table ...`, in order to facilitate convenient
data transfers to be used within data pipelines or ad hoc operations.

## Synopsis
- Load from InfluxDB server: `ctk load table influxdb2://...`
- Load from InfluxDB line protocol: `ctk load table file://observations.lp`

## Details
The InfluxDB table loader is based on the [influxio] package. Please also check
its documentation to learn about more of its capabilities, supporting you when
working with InfluxDB.

## Install
```shell
pip install --upgrade 'cratedb-toolkit[influxdb]'
```

## Usage

Prepare subsequent commands by defining the database address of your
CrateDB database cluster.
```shell
export CRATEDB_CLUSTER_URL=crate://crate@localhost:4200/testdrive/demo
```

### InfluxDB 2 API

An exemplary walkthrough, copying data from InfluxDB to CrateDB, both services
expected to be listening on `localhost`.

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

Transfer data from InfluxDB bucket/measurement into CrateDB schema/table.
```shell
ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
```

Query data in CrateDB.
```shell
ctk shell --command "SELECT * FROM testdrive.demo;"
ctk show table "testdrive.demo"
```

:::{todo}
- More convenient table querying.
:::

### InfluxDB Line protocol file (ILP)

Import ILP file from local filesystem.
```shell
ctk load table "file://influxdb-export.lp"
```

Import ILP file from a remote resource.
```shell
ctk load table \
    "https://github.com/influxdata/influxdb2-sample-data/raw/master/air-sensor-data/air-sensor-data.lp"
```

### Cloud

A canonical invocation for copying data from InfluxDB Cloud to CrateDB Cloud.
Please note the `ssl=true` query parameter at the end of both database
connection URLs.

```shell
ctk load table \
  "influxdb2://9fafc869a91a3517:T268DVLDHD8...oPic4A==@eu-central-1-1.aws.cloud2.influxdata.com/testdrive/demo?ssl=true" \
  --cluster-url="crate://admin:dZ...6LqB@green-shaak-ti.eks1.eu-west-1.aws.cratedb.net:4200/testdrive/demo?ssl=true"
```

## Parameters

### `if-exists`

The target table will be created automatically, if it does not exist. If it
does exist, the `if-exists` URL query parameter can be used to configure this
behavior. The default value is `fail`, the possible values are:

* `fail`: Raise a ValueError.
* `replace`: Drop the table before inserting new values.
* `append`: Insert new values to the existing table.

:::{rubric} Example usage
:::
In order to always replace the target table, i.e. to drop and re-create it
prior to inserting data, use `?if-exists=replace`.
```shell
export CRATEDB_CLUSTER_URL="crate://crate@localhost:4200/testdrive/demo?if-exists=replace"
ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
```


[influxio]: inv:influxio:*:label#index
