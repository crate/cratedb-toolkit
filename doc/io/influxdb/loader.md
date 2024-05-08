(influxdb-loader)=
# InfluxDB Table Loader

## About
Load data from InfluxDB into CrateDB using a one-stop command
`ctk load table influxdb2://...`, in order to facilitate convenient
data transfers to be used within data pipelines or ad hoc operations.

## Details
The InfluxDB table loader is based on the [influxio] package. Please also check
its documentation to learn about more of its capabilities, supporting you when
working with InfluxDB.

## Install
```shell
pip install --upgrade 'cratedb-toolkit[influxdb]'
```

## Example
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
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
crash --command "SELECT * FROM testdrive.demo;"
```

Query data in CrateDB.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk shell --command "SELECT * FROM testdrive.demo;"
ctk show table "testdrive.demo"
```

:::{todo}
- More convenient table querying.
:::


[influxio]: inv:influxio:*:label#index
