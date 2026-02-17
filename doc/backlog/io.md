# Backlog for `ctk {load,save} [table]`

## Apache Iceberg

- Exercise access with Iceberg REST catalogs
  https://altinity.com/blog/introducing-altinity-ice-a-simple-toolset-for-iceberg-rest-catalogs

- Exercise end-to-end integration with e.g. Google Cloud
  https://opensource.googleblog.com/2026/01/explore-public-datasets-with-apache-iceberg-and-biglake.html
  https://cloud.google.com/blog/products/data-analytics/announcing-bigquery-tables-for-apache-iceberg

- Verify on Windows
  Example: `--conf spark.sql.catalog.my_catalog.warehouse=file:///D:/sparksetup/iceberg/spark_warehouse`
  https://medium.com/itversity/iceberg-catalogs-a-guide-for-data-engineers-a6190c7bf381

- Compression!

- Auth!
  https://github.com/apache/iceberg/issues/13550
