(postgresql)=

# PostgreSQL I/O

:::{div} sd-text-muted
Load data from PostgreSQL into CrateDB.
:::

## Install

```shell
uv tool install --upgrade 'cratedb-toolkit[io-ingest]'
```

## Synopsis

Invoke `full-load` operation.
```shell
ctk load table \
    "postgresql://pguser:secret11@postgresql.example.org:5432/postgres?table=information_schema.tables" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/postgresql_tables"
```

```{include} ../../_db-options.md
```


```{toctree}
:hidden:

Research <research>
```
