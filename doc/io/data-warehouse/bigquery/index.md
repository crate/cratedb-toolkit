(bigquery)=

# Google BigQuery

:::{div} sd-text-muted
Load data from Google BigQuery into CrateDB.
:::

[BigQuery] is Google's fully-managed, serverless data warehouse
designed for scalable analytics and machine learning workloads.

```shell
ctk load \
    "bigquery://<project-name>?credentials_path=/path/to/service/account.json&location=<location>&table=<table-name>" \
    "crate://crate:na@localhost:4200/testdrive/bigquery"
```


[BigQuery]: https://cloud.google.com/bigquery
