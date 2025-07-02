(io-postgresql)=

# PostgreSQL I/O

Load and continuously replicate data from PostgreSQL to CrateDB,
building upon the [ingestr i/o subsystem].

## Install

```shell
uv tool install --upgrade 'cratedb-toolkit[io-ingestr]'
```

## Synopsis

Invoke `full-load` operation.
```shell
ctk load table \
    "postgresql://pguser:secret11@postgresql.example.org:5432/postgres?table=information_schema.tables" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/postgresql_tables"
```

## Configure

Because the underlying framework uses [dlt], you will configure parameters like
batch size in your `.dlt/config.toml`.
```toml
[data_writer]
buffer_max_items=1_000
file_max_items=100_000
file_max_bytes=50_000
```


```{toctree}
:hidden:

research
```


[dlt]: https://github.com/dlt-hub/dlt
[ingestr i/o subsystem]: project:#ingestr
