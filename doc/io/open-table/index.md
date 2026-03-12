(io-opentable)=

# Open table formats

:::{div} sd-text-muted
Import and export data into/from open table formats on filesystem and cloud storage.
:::

:::{include} ../_install-curated.md
:::

## Integrations

I/O adapters for [Apache Iceberg] tables [^iceberg] and [Delta Lake] tables
[^deltalake], to enhance interoperability with [open table formats].
They build upon [Apache Parquet] data files, a free and open-source
column-oriented data storage format, effectively succeeding and superseding
[Apache Hive] use cases from the [Hadoop] era.

```{toctree}
:maxdepth: 1

DeltaLake <deltalake/index>
Iceberg <iceberg/index>
```


[^deltalake]: Delta Lake ([paper][armbrust-paper]) is the optimized storage layer that [provides the foundation and default format for all table operations on Databricks]. It was developed for tight integration with Structured Streaming, allowing you to easily use a single copy of data for both batch and streaming operations and providing incremental processing at scale.
[^iceberg]: [Iceberg is a specification] and high-performance format for huge analytic tables, making it possible for engines like Spark, Trino, Flink, Presto, Hive and Impala to safely work with the same tables, at the same time. Apache Iceberg is its reference implementation.


[armbrust-paper]: https://www.databricks.com/wp-content/uploads/2020/08/p975-armbrust.pdf
[Apache Hive]: https://hive.apache.org/
[Apache Iceberg]: https://iceberg.apache.org/
[Apache Parquet]: https://en.wikipedia.org/wiki/Apache_Parquet
[Delta Lake]: https://delta.io/
[Hadoop]: https://hadoop.apache.org/
[Iceberg is a specification]: https://iceberg.apache.org/spec/
[open table formats]: https://www.baeldung.com/cs/open-table-formats
[provides the foundation and default format for all table operations on Databricks]: https://docs.databricks.com/aws/en/delta/
