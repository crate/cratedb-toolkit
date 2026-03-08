# Databases

:::{div} sd-text-muted
Import and export data into/from databases and data warehouses.
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

- CSV, JSON, and Parquet files
- [Databases supported by SQLAlchemy]
- Event brokers

:::{rubric} Batch size
:::
Because the underlying framework uses [dlt], you will configure parameters like
batch size in your `.dlt/config.toml`.
```toml
[data_writer]
buffer_max_items=1_000
file_max_items=100_000
file_max_bytes=50_000
```

:::{rubric} Custom queries
:::
The feature [custom queries for SQL sources] can be used for partial table
loading to limit the import to use just a subset of the source columns,
or for general data filtering and aggregation purposes.
Use the `query:` prefix to the source table name to provide an SQL statement.
Example:
```shell
ctk load table \
    "crate://crate:na@localhost:4200/?table=query:SELECT * FROM sys.summits WHERE height > 4000" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/cratedb_summits"
```

:::{rubric} Integrations
:::

Load data from Google BigQuery into CrateDB.
```shell
ctk load table \
    "bigquery://<project-name>?credentials_path=/path/to/service/account.json&location=<location>?table=<table-name>" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/bigquery_demo"
```

Load data from Databricks into CrateDB.
```shell
ctk load table \
    "databricks://token:<access_token>@<server_hostname>?http_path=<http_path>&catalog=<catalog>&schema=<schema>&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/databricks_demo"
```

Load data from PostgreSQL into CrateDB.
```shell
ctk load table \
    "postgresql://<username>:<password>@postgresql.example.org:5432/postgres?table=information_schema.tables" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/postgresql_tables"
```

Load data from Amazon Redshift into CrateDB.
```shell
ctk load table \
    "redshift+psycopg2://<username>:<password>@host.amazonaws.com:5439/database?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/redshift_demo"
```

Load data from Apache Solr into CrateDB.
```shell
ctk load table \
    "solr://<username>:<password>@<host>:<port>/solr/<collection>?table=<collection>" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/solr_demo"
```

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

Load data from Databricks into CrateDB.
```shell
ctk load table \
    "databricks://token:<access_token>@<server_hostname>?http_path=<http_path>&catalog=<catalog>&schema=<schema>&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/databricks_demo"
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

Load data from Elasticsearch into CrateDB.
```shell
ctk load table \
    "elasticsearch://<username>:<password>@es.example.org:9200?secure=false&verify_certs=false&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/elastic_demo"
```

Load data from Google BigQuery into CrateDB.
```shell
ctk load table \
    "bigquery://<project-name>?credentials_path=/path/to/service/account.json&location=<location>?table=<table-name>" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/bigquery_demo"
```

Load data from Google Sheets into CrateDB.
```shell
ctk load table \
    "gsheets://?credentials_path=/path/to/service/account.json&table=fkdUQ2bjdNfUq2CA.Sheet1" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/gsheets_demo"
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

Load data from Snowflake into CrateDB.
```shell
ctk load table \
    "snowflake://<username>:<password>@account/dbname?warehouse=COMPUTE_WH&role=data_scientist&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/snowflake_demo"
```

Load data from SQLite into CrateDB.
```shell
ctk load table \
    "sqlite:////path/to/demo.sqlite?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/sqlite_demo"
```

Load data from Teradata into CrateDB.
```shell
ctk load table \
    "teradatasql://guest:please@teradata.example.com/?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/teradata_demo"
```



[custom queries for SQL sources]: https://bruin-data.github.io/ingestr/supported-sources/custom_queries.html
[databases supported by SQLAlchemy]: https://docs.sqlalchemy.org/en/20/dialects/
