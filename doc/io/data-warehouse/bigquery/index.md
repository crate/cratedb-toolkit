(bigquery)=

# Google BigQuery

:::{div} sd-text-muted
Load data from Google BigQuery into CrateDB.
:::

[BigQuery] is a cloud-based data warehouse platform for big data analytics
and artificial intelligence based on Apache Spark and Delta Lake.

```shell
ctk load table \
    "bigquery://<project-name>?credentials_path=/path/to/service/account.json&location=<location>?table=<table-name>" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/bigquery_demo"
```


[BigQuery]: https://cloud.google.com/bigquery
