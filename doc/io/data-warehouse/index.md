(io-warehouse)=

# Data warehouses

:::{div} sd-text-muted
Import and export data into/from data warehouses (DWH).
:::

Data warehouses are strong but expensive.
Adapters listed here support you to offload data into CrateDB,
for cost-effective data analytics.

## Integrations

```{toctree}
:maxdepth: 1

BigQuery <bigquery/index>
databricks/index
HANA <hana/index>
motherduck/index
Redshift <redshift/index>
snowflake/index
teradata/index
```

## Synopsis

Load data from BigQuery table into CrateDB table.
```shell
ctk load table \
    "bigquery://<project-name>?credentials_path=/path/to/service/account.json&location=<location>?table=<table-name>" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/bigquery_demo"
```

Load data from Databricks table into CrateDB table.
```shell
ctk load table \
    "databricks://token:<access_token>@<server_hostname>?http_path=<http_path>&catalog=<catalog>&schema=<schema>&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/databricks_demo"
```

Load data from Redshift table into CrateDB table.
```shell
ctk load table \
    "redshift+psycopg2://<username>:<password>@host.amazonaws.com:5439/database?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/redshift_demo"
```

Load data from Snowflake table into CrateDB table.
```shell
ctk load table \
    "snowflake://<username>:<password>@account/dbname?warehouse=COMPUTE_WH&role=data_scientist&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/snowflake_demo"
```

Load data from Teradata table into CrateDB table.
```shell
ctk load table \
    "teradatasql://guest:please@teradata.example.com/?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/teradata_demo"
```
