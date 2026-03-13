(teradata)=

# Teradata

:::{div} sd-text-muted
Load data from Teradata into CrateDB.
:::

[Teradata] is an enterprise data warehouse platform providing high-performance
analytics and data management capabilities for large-scale data workloads.

```shell
ctk load table \
    "teradatasql://guest:please@teradata.example.com/?table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/teradata"
```


[Teradata]: https://www.teradata.com/
