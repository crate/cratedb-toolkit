(databricks)=

# Databricks

:::{div} sd-text-muted
Load data from Databricks into CrateDB.
:::

[Databricks] is a cloud-based data warehouse platform for big data analytics
and artificial intelligence based on Apache Spark and Delta Lake.

## Prerequisites

Use Docker or Podman to run all components. This approach works consistently
across Linux, macOS, and Windows.
You also need an account on https://databricks.com/.

## Install
Install the most recent Python package [cratedb-toolkit],
or evaluate {ref}`alternative installation methods <install>`.
```shell
uv tool install --upgrade 'cratedb-toolkit[io-ingest]'
```

## Tutorial

5-minute step-by-step instructions about how
to work with Databricks and CrateDB.

### Authentication

For accessing Databricks SQL warehouses, you need an **access token**,
your **server hostname**, and the **HTTP path** (endpoint of the warehouse).

On the Databricks platform, in your settings dialog (top right corner),
navigate to "User » Developer » Access tokens » Manage", and select
"Generate new token". Then, copy the generated token as suggested on
the result dialog (example: `dapi367e0b1....`), and use it in your
command below.

-- `https://<instance>.cloud.databricks.com/settings/user/developer`

Within the primary navigation, navigate to "SQL » SQL Warehouses",
select the "Serverless Starter Warehouse" or any other, navigate to
"Connection Details", and use the values in "Server hostname" and
"HTTP path" in your command below.

-- `https://<instance>.cloud.databricks.com/sql/warehouses/<warehouse>/connectionDetails`

### Services

Run CrateDB using Docker or Podman.
```shell
docker run --rm --name=cratedb \
  --publish=4200:4200 --publish=5432:5432 --env=CRATE_HEAP_SIZE=2g \
  docker.io/crate:latest '-Cdiscovery.type=single-node'
```

### Populate data

The Databricks starter warehouse includes a few example tables.
You can use them for data transfer explorations, so you don't need to
load any data into the Databricks SQL warehouse before.

### Load data

Use {ref}`index` to load data from Databricks SQL warehouse into CrateDB table.

#### `samples.nyctaxi.trips`
Transfer data.
```shell
ctk load table \
    "databricks://token:<access_token>@<instance>.cloud.databricks.com:443/?http_path=/sql/1.0/warehouses/<warehouse>&catalog=samples&table=nyctaxi.trips" \
    --cluster-url="crate://crate:crate@localhost:4200/testdrive/nyctaxi_trips"
```
Query data using [crash](https://pypi.org/project/crash/).
```shell
crash -c 'SHOW CREATE TABLE testdrive.nyctaxi_trips'
```
```shell
crash -c 'SELECT * FROM testdrive.nyctaxi_trips LIMIT 5'
```

#### `samples.accuweather.forecast_hourly_metric`
Transfer data.
```shell
ctk load table \
    "databricks://token:<access_token>@<instance>.cloud.databricks.com:443/?http_path=/sql/1.0/warehouses/<warehouse>&catalog=samples&table=accuweather.forecast_hourly_metric" \
    --cluster-url="crate://crate:crate@localhost:4200/testdrive/accuweather_forecast_hourly_metric"
```
Query data.
```shell
crash -c 'SHOW CREATE TABLE testdrive.accuweather_forecast_hourly_metric'
```
```shell
crash -c 'SELECT * FROM testdrive.accuweather_forecast_hourly_metric LIMIT 5'
```

## Documentation

The Databricks table name can be provided by using the `&table=` query parameter.

:::{rubric} Databricks URL parameters
:::

- `server_hostname`: Host name of the Databricks instance.
- `http_path`: Path to the Databricks instance.
- `catalog`: Database catalog name.
- `schema`: Database schema name.

:::{rubric} Databricks access token authentication
:::

- `access_token`: Access token for Databricks.

A fully qualified traditional URL template for Databricks looks like this.
```shell
databricks://token:${DATABRICKS_ACCESS_TOKEN}@${DATABRICKS_SERVER_HOSTNAME}?http_path=${DATABRICKS_HTTP_PATH}&catalog=${DATABRICKS_CATALOG}&schema=${DATABRICKS_SCHEMA}
```

:::{rubric} Databricks OAuth M2M authentication (service principal)
:::

- `client_id`: OAuth service principal's client ID (application ID).
- `client_secret`: OAuth secret for service principal.

A fully qualified M2M URL template for Databricks looks like this.
```shell
databricks://@${DATABRICKS_SERVER_HOSTNAME}?http_path=${DATABRICKS_HTTP_PATH}&client_id=${DATABRICKS_CLIENT_ID}&client_secret=${DATABRICKS_CLIENT_SECRET}&catalog=${DATABRICKS_CATALOG}&schema=${DATABRICKS_SCHEMA}
```

To set up OAuth M2M authentication:

1. Create a service principal in your Databricks workspace.
1. Generate an OAuth secret for the service principal.
1. Ensure the service principal has the necessary permissions to
   access your workspace resources.

To learn more details, you can read about [Databricks OAuth M2M authentication]
on the original documentation.

See also the documentation about [SQLAlchemy's Databricks dialect].

:::{rubric} CrateDB URL parameters
:::

Please make sure to replace username, password, and
hostname with values matching your environment.

- `ssl`: Use the `?ssl=true` query parameter to enable SSL. Also use this when
  connecting to CrateDB Cloud.
  ```text
  --cluster-url='crate://crate:crate@cratedb.example.org:4200/schema/table?ssl=true'
  ```

## See also

:::{include} /_snippet/ingest-see-also.md
:::


[cratedb-toolkit]: https://pypi.org/project/cratedb-toolkit/
[Databricks]: https://www.databricks.com/
[Databricks OAuth M2M authentication]: https://getbruin.com/docs/ingestr/supported-sources/databricks.html
[SQLAlchemy's Databricks dialect]: https://docs.databricks.com/aws/en/dev-tools/sqlalchemy
