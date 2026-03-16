(redshift)=

# Amazon Redshift

:::{div} sd-text-muted
Load data from Amazon Redshift into CrateDB.
:::

[Redshift] is a managed data warehouse product by Amazon Web Services,
offering the ability to handle analytic workloads on big data sets stored
by a column-oriented DBMS principle.

```shell
ctk load \
    "redshift+psycopg2://<username>:<password>@host.amazonaws.com:5439/database?table=demo" \
    "crate://crate:na@localhost:4200/testdrive/redshift"
```


[Redshift]: https://aws.amazon.com/redshift/
