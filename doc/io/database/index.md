(io-database)=

# Databases

:::{div} sd-text-muted
Import and export data into/from database systems.
:::

## Integrations

:::::{grid} 2 3 3 4
:gutter: 2
:padding: 0

::::{grid-item-card}
:link: athena
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/athena.svg
:height: 80px
:alt:
```
+++
Athena
::::

::::{grid-item-card}
:link: aws-dms
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/aws-dms.svg
:height: 80px
:alt:
```
+++
AWS DMS
::::

::::{grid-item-card}
:link: clickhouse
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/clickhouse.png
:height: 80px
:alt:
```
+++
ClickHouse
::::

::::{grid-item-card}
:link: couchbase
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/couchbase.svg
:height: 80px
:alt:
```
+++
Couchbase
::::

::::{grid-item-card}
:link: duckdb
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/duckdb.svg
:height: 80px
:alt:
```
+++
DuckDB
::::

::::{grid-item-card}
:link: dynamodb
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/dynamodb.svg
:height: 80px
:alt:
```
+++
DynamoDB
::::

::::{grid-item-card}
:link: exasol
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/exasol.svg
:height: 80px
:alt:
```
+++
Exasol
::::

::::{grid-item-card}
:link: ibm-db2
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/ibm-db2.svg
:height: 80px
:alt:
```
+++
IBM Db2
::::

::::{grid-item-card}
:link: influxdb
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/influxdb.svg
:height: 80px
:alt:
```
+++
InfluxDB
::::

::::{grid-item-card}
:link: mariadb
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/mariadb.svg
:height: 80px
:alt:
```
+++
MariaDB
::::

::::{grid-item-card}
:link: mongodb
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/mongodb.svg
:height: 80px
:alt:
```
+++
MongoDB
::::

::::{grid-item-card}
:link: mssql
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/mssql.svg
:height: 80px
:alt:
```
+++
Microsoft SQL
::::

::::{grid-item-card}
:link: mysql
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/mysql.svg
:height: 80px
:alt:
```
+++
MySQL
::::

::::{grid-item-card}
:link: oracle
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/oracle.svg
:height: 80px
:alt:
```
+++
Oracle
::::

::::{grid-item-card}
:link: postgresql
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/postgresql.svg
:height: 80px
:alt:
```
+++
PostgreSQL
::::

::::{grid-item-card}
:link: spanner
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/spanner.svg
:height: 80px
:alt:
```
+++
Spanner
::::

::::{grid-item-card}
:link: sqlite
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/sqlite.svg
:height: 80px
:alt:
```
+++
SQLite
::::

:::::


```{toctree}
:maxdepth: 1
:hidden:

AWS DMS <dms/index>
DynamoDB <dynamodb/index>
InfluxDB <influxdb/index>
MongoDB <mongodb/index>
PostgreSQL <postgresql/index>
```

### Examples

(athena)=
:::{rubric} Amazon Athena
:::
Load data from Amazon Athena into CrateDB.
```shell
ctk load table \
    "athena://?bucket=<your-destination-bucket>&access_key_id=<your-aws-access-key-id>&secret_access_key=<your-aws-secret-access-key>&region_name=<your-aws-region>&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/athena"
```

(couchbase)=
:::{rubric} Couchbase
:::
Load data from Couchbase into CrateDB.
```shell
ctk load table \
    "couchbase://username:password@host/bucket?ssl=true&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/couchbase"
```

(clickhouse)=
:::{rubric} ClickHouse
:::
Load data from ClickHouse into CrateDB.
```shell
ctk load table \
    "clickhouse://<username>:<password>@<host>:<port>?secure=<secure>&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/clickhouse"
```

:::{rubric} CrateDB
:::
Load data from CrateDB into CrateDB.
```shell
ctk load table \
    "crate://crate:na@localhost:4200/?table=sys.summits" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/cratedb_summits"
```
```shell
ctk load table \
    "crate://crate:na@localhost:4200/?table=information_schema.tables" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/cratedb_tables"
```
```shell
ctk load table \
    "crate://crate:na@localhost:4200/?table=query:SELECT * FROM sys.summits WHERE height > 4000" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/cratedb_summits"
```

(duckdb)=
:::{rubric} DuckDB
:::
Load data from DuckDB into CrateDB.
```shell
ctk load table \
    "duckdb:////path/to/demo.duckdb?table=information_schema.tables" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/duckdb_tables"
```

(exasol)=
:::{rubric} Exasol
:::
Load data from EXASOL DB into CrateDB.
```shell
ctk load table \
    "exa+websocket://sys:exasol@127.0.0.1:8888?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/exasol"
```

(ibm-db2)=
:::{rubric} IBM Db2
:::
Load data from IBM Db2 into CrateDB.
```shell
ctk load table \
    "db2://<username>:<password>@<host>:<port>/<database-name>&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/ibmdb2"
```

(mssql)=
:::{rubric} Microsoft SQL Server
:::
Load data from Microsoft SQL Server into CrateDB.
```shell
alias ctk-ingest='docker run --rm -it --network=host ghcr.io/crate/cratedb-toolkit-ingest:latest ctk'
USER=$(az account show --query user.name -o tsv)
TOKEN=$(az account get-access-token --resource https://database.windows.net/ --query accessToken -o tsv)
ctk-ingest load table \
    "mssql://$USER:$TOKEN@<server>.database.windows.net/<database>?table=demo&Authentication=ActiveDirectoryAccessToken" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/mssql"
```

(mysql)=
(mariadb)=
:::{rubric} MySQL
:::
Load data from MySQL or MariaDB into CrateDB.
```shell
ctk load table \
    "mysql://<username>:<password>@host:port/dbname?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/mysql"
```

(oracle)=
:::{rubric} Oracle
:::
Load data from Oracle into CrateDB.
```shell
ctk load table \
    "oracle+cx_oracle://<username>:<password>@<hostname>:<port>/dbname?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/oracle"
```

:::{rubric} PostgreSQL
:::
Load data from PostgreSQL into CrateDB.
```shell
ctk load table \
    "postgresql://<username>:<password>@postgresql.example.org:5432/postgres?table=information_schema.tables" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/postgresql_tables"
```

(spanner)=
:::{rubric} Spanner
:::
Load data from Spanner into CrateDB.
```shell
ctk load table \
    "spanner://?project_id=<project_id>&instance_id=<instance_id>&database=<database>&credentials_path=</path/to/service/account.json>&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/spanner"
```
```shell
ctk load table \
    "spanner://?project_id=<project_id>&instance_id=<instance_id>&database=<database>&credentials_base64=<base64_encoded_credentials>&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/spanner"
```

(sqlite)=
:::{rubric} SQLite
:::
Load data from SQLite into CrateDB.
```shell
ctk load table \
    "sqlite:////path/to/demo.sqlite?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/sqlite"
```
