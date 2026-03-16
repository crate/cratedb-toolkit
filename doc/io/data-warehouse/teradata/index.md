(teradata)=

# Teradata

:::{div} sd-text-muted
Load data from Teradata into CrateDB.
:::

[Teradata] is an enterprise data warehouse platform providing high-performance
analytics and data management capabilities for large-scale data workloads.

```shell
ctk load \
    "teradatasql://guest:please@teradata.example.com/?table=demo" \
    "crate://crate:na@localhost:4200/testdrive/teradata"
```


[Teradata]: https://www.teradata.com/
