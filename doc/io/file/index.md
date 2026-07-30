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

adapter = DatabaseAdapter(dburi="crate://crate:na@localhost:4200/")
adapter.import_csv_pandas(filepath="./examples/cdc/postgresql/diamonds.csv", tablename="testdrive.csv_diamonds")
```
