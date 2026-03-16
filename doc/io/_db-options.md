## Options

### Batch size

The source URL accepts the `batch_size` option to configure pagination.
For maximum throughput, you will need to experiment to find an optimal
value. The default value is mostly 50_000, but might be different for
individual adapters.
```shell
ctk load "protocol://?batch_size=5000"
```

### Custom queries

The source URL accepts the `?table=query:<expression>` option that
can be used for partial table loading by using an SQL query expression
on the source database. For example, limit the import to use just a subset
of the source columns, a subset of all records, or for any other aggregation
queries. See also [custom queries for SQL sources]. Example:
```shell
ctk load "protocol://?table=query:SELECT * FROM sys.summits WHERE height > 4000"
```


[custom queries for SQL sources]: https://bruin-data.github.io/ingestr/supported-sources/custom_queries.html
