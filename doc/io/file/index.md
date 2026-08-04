(io-file)=

# Files

:::{div} sd-text-muted
Import data from files on filesystem and cloud storage into CrateDB.
:::

:::{note}
The former `ctk load "<scheme>://..."` CLI examples for files (Amazon S3, Google Cloud
Storage, and the `csv://` scheme) were reachable only through a bundled `ingestr`
dependency, which has since been removed (see {ref}`I/O adapter coverage <io-coverage>`).
None of them had `cratedb-toolkit`-specific code or tests behind them, so they have been
dropped rather than carried over as non-working documentation.
:::

## CSV

CSV loading is available through the Python API. Use
`cratedb_toolkit.util.database.DatabaseAdapter.import_csv_pandas` or `import_csv_dask` --
the tested, code-level way to load a CSV file into CrateDB:

```python
from cratedb_toolkit.util.database import DatabaseAdapter

adapter = DatabaseAdapter(dburi="crate://crate:na@localhost:4200/?schema=testdrive")
adapter.import_csv_pandas(
    filepath="./examples/cdc/postgresql/diamonds.csv",
    tablename="csv_diamonds",
    if_exists="replace",
)
```

:::{note}
The `tablename` argument is passed straight to `pandas.DataFrame.to_sql` as the table
name, so a value like `"testdrive.csv_diamonds"` would create a table literally named
`testdrive.csv_diamonds` rather than table `csv_diamonds` in schema `testdrive`. Select
the target schema through the `?schema=` query parameter on the adapter URL instead.

`if_exists` defaults to `"replace"`, which **drops** an existing target table before
loading. Pass `if_exists="fail"` or `"append"` if you do not want the table replaced.
:::
