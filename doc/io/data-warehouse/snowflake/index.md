(snowflake)=

# Snowflake

:::{div} sd-text-muted
Load data from Snowflake into CrateDB.
:::

[Snowflake] is a cloud-based data warehouse and platform
offering the ability to handle analytic workloads on big data sets stored
by a column-oriented DBMS principle.

```shell
ctk load \
    "snowflake://<username>:<password>@account/dbname?warehouse=COMPUTE_WH&role=data_scientist&table=demo" \
    "crate://crate:na@localhost:4200/testdrive/snowflake"
```


[Snowflake]: https://www.snowflake.com/
