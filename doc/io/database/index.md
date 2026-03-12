(io-database)=

# Databases

:::{div} sd-text-muted
Import and export data into/from database systems.
:::

Database I/O adapters are provided in two groups that support
different types of data source and data sink pipeline elements
and data formats. Please select the right one based on your needs.

## Group »curated«

Support for files, open table formats, InfluxDB, and MongoDB.

:::{rubric} Install
:::
```shell
uv tool install --upgrade 'cratedb-toolkit[io-curated]'
```

:::{rubric} Integrations
:::
```{toctree}
:maxdepth: 1

AWS DMS <dms/index>
DynamoDB <dynamodb/index>
InfluxDB <influxdb/index>
MongoDB <mongodb/index>
PostgreSQL <postgresql/index>
```

## Group »ingest«

Support for files, RDBMS databases and search servers, streams, platforms,
and services.

:::{rubric} Install
:::
```shell
uv tool install --upgrade 'cratedb-toolkit[io-ingest]'
```

:::{rubric} Coverage
:::

- [All databases supported by SQLAlchemy]
- CSV, JSON, and Parquet files
- Message and event brokers

:::{rubric} Integrations
:::

Load data from ClickHouse into CrateDB.
```shell
ctk load table \
    "clickhouse://<username>:<password>@<host>:<port>?secure=<secure>&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/clickhouse_demo"
```

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


Load data from DuckDB into CrateDB.
```shell
ctk load table \
    "duckdb:////path/to/demo.duckdb?table=information_schema.tables" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/duckdb_tables"
```

Load data from EXASOL DB into CrateDB.
```shell
ctk load table \
    "exa+websocket://sys:exasol@127.0.0.1:8888?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/exasol_demo"
```

Load data from MySQL or MariaDB into CrateDB.
```shell
ctk load table \
    "mysql://<username>:<password>@host:port/dbname?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/mysql_demo"
```

Load data from Oracle into CrateDB.
```shell
ctk load table \
    "oracle+cx_oracle://<username>:<password>@<hostname>:<port>/dbname?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/oracle_demo"
```

Load data from PostgreSQL into CrateDB.
```shell
ctk load table \
    "postgresql://<username>:<password>@postgresql.example.org:5432/postgres?table=information_schema.tables" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/postgresql_tables"
```

Load data from SQLite into CrateDB.
```shell
ctk load table \
    "sqlite:////path/to/demo.sqlite?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/sqlite_demo"
```



[All databases supported by SQLAlchemy]: https://docs.sqlalchemy.org/en/20/dialects/
