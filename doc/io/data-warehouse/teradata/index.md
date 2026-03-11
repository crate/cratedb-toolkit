(teradata)=

# Teradata

:::{div} sd-text-muted
Load data from Teradata into CrateDB.
:::

[Teradata] is a managed data warehouse product by Amazon Web Services,
offering the ability to handle analytic workloads on big data sets stored
by a column-oriented DBMS principle.

```shell
ctk load table \
    "teradatasql://guest:please@teradata.example.com/?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/teradata_demo"
```


[Teradata]: https://www.teradata.com/
