(io-warehouse)=

# Data warehouses

:::{div} sd-text-muted
Import and export data into/from data warehouses (DWH).
:::

Data warehouses are powerful but expensive.
I/O adapters listed here support you to offload data into CrateDB,
for cost-effective data analytics.

## Integrations

:::::{grid} 3
:gutter: 2
:padding: 0

::::{grid-item-card}
:link: redshift
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/redshift.svg
:height: 80px
:alt:
```
+++
Amazon Redshift
::::

::::{grid-item-card}
:link: databricks
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/databricks.svg
:height: 80px
:alt:
```
+++
Databricks
::::

::::{grid-item-card}
:link: bigquery
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/bigquery.svg
:height: 80px
:alt:
```
+++
Google BigQuery
::::

::::{grid-item-card}
:link: motherduck
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/motherduck.svg
:height: 80px
:alt:
```
+++
MotherDuck
::::

::::{grid-item-card}
:link: hana
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/sap.svg
:height: 80px
:alt:
```
+++
SAP HANA
::::

::::{grid-item-card}
:link: snowflake
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/snowflake.svg
:height: 80px
:alt:
```
+++
Snowflake
::::

::::{grid-item-card}
:link: teradata
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/teradata.svg
:height: 80px
:alt:
```
+++
Teradata
::::

:::::

```{toctree}
:maxdepth: 1
:hidden:

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
ctk load \
    "bigquery://<project-name>?credentials_path=/path/to/service/account.json&location=<location>&table=<table-name>" \
    "crate://crate:na@localhost:4200/testdrive/bigquery"
```

Load data from Databricks table into CrateDB table.
```shell
ctk load \
    "databricks://token:<access_token>@<server_hostname>?http_path=<http_path>&catalog=<catalog>&schema=<schema>&table=demo" \
    "crate://crate:na@localhost:4200/testdrive/databricks"
```

Load data from SAP HANA table into CrateDB table.
```shell
ctk load \
    "hana://SYSTEM:HXEHana1@localhost:39017/SYSTEMDB?table=sys.adapters" \
    "crate://crate:na@localhost:4200/testdrive/hana"
```

Load data from MotherDuck table into CrateDB table.
```shell
ctk load \
    "motherduck://<database-name>?token=<your-token>&table=<schema-name>.<table-name>" \
    "crate://crate:na@localhost:4200/testdrive/motherduck"
```

Load data from Redshift table into CrateDB table.
```shell
ctk load \
    "redshift+psycopg2://<username>:<password>@host.amazonaws.com:5439/database?table=demo" \
    "crate://crate:na@localhost:4200/testdrive/redshift"
```

Load data from Snowflake table into CrateDB table.
```shell
ctk load \
    "snowflake://<username>:<password>@account/dbname?warehouse=COMPUTE_WH&role=data_scientist&table=demo" \
    "crate://crate:na@localhost:4200/testdrive/snowflake"
```

Load data from Teradata table into CrateDB table.
```shell
ctk load \
    "teradatasql://guest:please@teradata.example.com/?table=demo" \
    "crate://crate:na@localhost:4200/testdrive/teradata"
```
