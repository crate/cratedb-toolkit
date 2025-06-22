# Ingestr

## About

[Ingestr] is a versatile data I/O framework and command-line application
to copy data between any source and any destination. It supports many data
sources and destinations out of the box.

Adapters for CrateDB let you migrate data from any proprietary enterprise
warehouse or database to [CrateDB] or [CrateDB Cloud], to consolidate
infrastructure and save operational costs.

## Coverage

Ingestr supports migration from 20-plus databases, data platforms, analytics
engines, and other services, see [sources supported by Ingestr] and [databases
supported by SQLAlchemy].

:::{rubric} Databases
:::
Actian Data Platform, Vector, Actian X, Ingres, Amazon Athena, Amazon Redshift,
Amazon S3, Apache Drill, Apache Druid, Apache Hive and Presto, Apache Solr, 
Clickhouse, CockroachDB, CrateDB, Databend, Databricks, Denodo, DuckDB, EXASOL DB,
Elasticsearch, Firebird, Firebolt, Google BigQuery, Google Sheets, Greenplum, 
HyperSQL (hsqldb), IBM DB2 and Informix, IBM Netezza Performance Server, Impala, 
Kinetica, Microsoft Access, Microsoft SQL Server, MonetDB, MongoDB, MySQL and MariaDB, 
OpenGauss, OpenSearch, Oracle, PostgreSQL, Rockset, SAP ASE, SAP HANA,
SAP Sybase SQL Anywhere, Snowflake, SQLite, Teradata Vantage, TiDB, YDB, YugabyteDB.

:::{rubric} Brokers
:::
Amazon Kinesis, Apache Kafka (Amazon MSK, Confluent Kafka, Redpanda, RobustMQ)

:::{rubric} File formats
:::
CSV, JSONL/NDJSON, Parquet

:::{rubric} Object stores
:::
Amazon S3, Google Cloud Storage

:::{rubric} Services
:::
Airtable, Asana, GitHub, Google Ads, Google Analytics, Google Sheets, HubSpot,
Notion, Personio, Salesforce, Slack, Stripe, Zendesk, etc.

## What's inside

The CrateDB destination adapter for ingestr is implemented using [dlt].

<iframe width="240" src="https://www.youtube-nocookie.com/embed/iNxRemknAdQ?si=J0w5yG56Ld4fIXfm" title="dlt (data load tool) - Python data extraction / loading tool" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>

## Usage

Using the framework is straight-forward. Either use the [`ingestr ingest`] CLI
directly, or use `ctk load table`.

:::{tip}
Ingestr uses four parameters to address source and destination, while
`ctk` uses just two, by embedding the table names into the address URLs.

When using `ingestr ingest`, please note you must address CrateDB like
PostgreSQL, but using the `cratedb://` URL scheme instead of `postgresql://`.
:::

### Amazon Kinesis to CrateDB
Template: `kinesis://?aws_access_key_id=<aws-access-key-id>&aws_secret_access_key=<aws-secret-access-key>&region_name=<region-name>&table=arn:aws:kinesis:<region-name>:<aws-account-id>:stream/<stream_name>`
```shell
ctk load table \
    "kinesis://?aws_access_key_id=<aws-access-key-id>&aws_secret_access_key=<aws-secret-access-key>&region_name=eu-central-1&table=arn:aws:kinesis:eu-central-1:831394476016:stream/testdrive" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/kinesis_demo"
```

### Amazon Redshift to CrateDB
```shell
ctk load table \
    "redshift+psycopg2://username@host.amazonaws.com:5439/database?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/redshift_demo"
```

### Amazon S3 to CrateDB
Template: `s3://?access_key_id=<your_access_key_id>&secret_access_key=<your_secret_access_key>&table=<bucket-name>/<file-glob>`
```shell
ctk load table \
    "s3://?access_key_id=${AWS_ACCESS_KEY_ID}&secret_access_key=${AWS_ACCESS_KEY}&table=openaq-fetches/realtime/2023-02-25/1677351953_eea_2aa299a7-b688-4200-864a-8df7bac3af5b.ndjson#jsonl" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/s3_ndjson_demo"
```

### Apache Kafka to CrateDB
```shell
ctk load table \
    "kafka://?bootstrap_servers=localhost:9092&group_id=test_group&security_protocol=SASL_SSL&sasl_mechanisms=PLAIN&sasl_username=example_username&sasl_password=example_secret&batch_size=1000&batch_timeout=3&table=my-topic" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/kafka_demo"
```

### Apache Solr to CrateDB
```shell
ctk load table \
    "solr://<username>:<password>@<host>:<port>/solr/<collection>?table=<collection>" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/solr_demo"
```

### Clickhouse to CrateDB
```shell
ctk load table \
    "clickhouse://<username>:<password>@<host>:<port>?secure=<secure>&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/clickhouse_demo"
```

### CrateDB to CrateDB
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

### CSV to CrateDB
```shell
ctk load table \
    "csv://./examples/cdc/postgresql/diamonds.csv?table=sample" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/csv_diamonds"
```

### Databricks to CrateDB
```shell
ctk load table \
    "databricks://token:<access_token>@<server_hostname>?http_path=<http_path>&catalog=<catalog>&schema=<schema>&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/databricks_demo"
```

### DuckDB to CrateDB
```shell
ctk load table \
    "duckdb:////path/to/demo.duckdb?table=information_schema.tables" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/duckdb_tables"
```

### EXASOL DB to CrateDB
```shell
ctk load table \
    "exa+websocket://sys:exasol@127.0.0.1:8888?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/exasol_demo"
```

### Elasticsearch to CrateDB
```shell
ctk load table \
    "elasticsearch://username:password@es.example.org:9200?secure=false&verify_certs=false?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/elastic_demo"
```

### GitHub to CrateDB
```shell
ctk load table \
    "github://?access_token=${GH_TOKEN}&owner=crate&repo=cratedb-toolkit&table=issues" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/github_ctk_issues"
```

### Google Cloud Storage to CrateDB
```shell
ctk load table \
    "gs://?credentials_path=/path/to/service-account.json?table=<bucket-name>/<file-glob>" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/gcs_demo"
```

### Google BigQuery to CrateDB
```shell
ctk load table \
    "bigquery://<project-name>?credentials_path=/path/to/service/account.json&location=<location>?table=<table-name>" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/bigquery_demo"
```

### Google Sheets to CrateDB
```shell
ctk load table \
    "gsheets://?credentials_path=/path/to/service/account.json&table=fkdUQ2bjdNfUq2CA.Sheet1" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/gsheets_demo"
```

### HubSpot to CrateDB
```shell
ctk load table \
    "hubspot://?api_key=<api-key-here>&table=deals" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/hubspot_deals"
```
See [HubSpot entities] about any labels you can use for the `table` parameter.

### MongoDB to CrateDB
```shell
ctk load table \
    "mongodb://user:password@host:port/?table=database.collection" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/mongodb_demo"
```

### MySQL to CrateDB
```shell
ctk load table \
    "mysql://user:password@host:port/dbname?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/mysql_demo"
```

### Oracle to CrateDB
```shell
ctk load table \
    "oracle+cx_oracle://user:password@host:port/dbname?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/oracle_demo"
```

### PostgreSQL to CrateDB
```shell
ctk load table \
    "postgresql://pguser:secret11@postgresql.example.org:5432/postgres?table=information_schema.tables" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/postgresql_tables"
```

### Salesforce to CrateDB
```shell
ctk load table \
    "salesforce://?username=<username>&password=<password>&token=<token>&table=opportunity" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/salesforce_opportunity"
```
See [Salesforce entities] about any labels you can use for the `table` parameter.

### Slack to CrateDB
```shell
ctk load table \
    "slack://?api_key=${SLACK_TOKEN}&table=channels" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/slack_channels"
```
See [Slack entities] about any labels you can use for the `table` parameter.

### Snowflake to CrateDB
```shell
ctk load table \
    "snowflake://user:password@account/dbname?warehouse=COMPUTE_WH&role=data_scientist&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/snowflake_demo"
```

### SQLite to CrateDB
```shell
ctk load table \
    "sqlite:////path/to/demo.sqlite?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/sqlite_demo"
```

### Teradata to CrateDB
```shell
ctk load table \
    "teradatasql://guest:please@teradata.example.com/?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/teradata_demo"
```


[CrateDB]: https://github.com/crate/crate
[CrateDB Cloud]: https://console.cratedb.cloud/
[databases supported by SQLAlchemy]: https://docs.sqlalchemy.org/en/20/dialects/
[dlt]: https://dlthub.com/
[HubSpot entities]: https://bruin-data.github.io/ingestr/supported-sources/hubspot.html#tables
[Ingestr]: https://bruin-data.github.io/ingestr/
[`ingestr ingest`]: https://bruin-data.github.io/ingestr/commands/ingest.html
[Salesforce entities]: https://bruin-data.github.io/ingestr/supported-sources/salesforce.html#tables
[Slack entities]: https://bruin-data.github.io/ingestr/supported-sources/slack.html#tables
[sources supported by Ingestr]: https://bruin-data.github.io/ingestr/supported-sources/
