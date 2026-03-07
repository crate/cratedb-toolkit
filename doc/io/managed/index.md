# Managed ETL

:::{include} /_snippet/links.md
:::

:::{div} sd-text-muted
CrateDB Cloud offers a managed ETL subsystem.
:::

:::{div}
Using the [Import API] or the [Export API] of [CrateDB Cloud], you can import
or export data from/into files, or load data, also continuously using [change
data capture], from databases and streaming sources.

- File formats: CSV, JSON, Parquet
- Databases: DynamoDB, MongoDB; including CDC

This walkthrough describes how to orchestrate managed ETL jobs using either the
command-line, or the Python API. Alternatively, if you prefer an interactive
user interface, please use the web-based [CrateDB Cloud Console].
:::

:::{include} /_snippet/install-ctk.md
:::

:::{include} /_snippet/cloud-prerequisites.md
:::

## Usage

You can run jobs from the command-line or by using the Python API.

### CLI

Use the `ctk load table` command to load data into database tables.
```shell
ctk load table 'https://cdn.crate.io/downloads/datasets/cratedb-datasets/cloud-tutorials/data_weather.csv.gz'
ctk load table 'https://cdn.crate.io/downloads/datasets/cratedb-datasets/cloud-tutorials/data_marketing.json.gz'
ctk load table 'https://cdn.crate.io/downloads/datasets/cratedb-datasets/timeseries/yc.2019.07-tiny.parquet.gz' --table='yc-2019-07-tiny'
```

Use the `ctk shell` command to query and aggregate data using SQL.
```shell
ctk shell --command="SELECT * FROM data_weather LIMIT 10;"
ctk shell --command="SELECT * FROM data_weather LIMIT 10;" --format=csv
ctk shell --command="SELECT * FROM data_weather LIMIT 10;" --format=json
```
:::{note}
`ctk shell` effectively just invokes the [CrateDB Shell], with the additional
benefit of transparently authenticating users from the existing session
established by CTK/croud. Otherwise, you would need to provide authentication
credentials separately.
:::

### Python API

Use the Python API to import and query data.

```python
# Import API classes.
from cratedb_toolkit import InputOutputResource, ManagedCluster

# Define data source.
url = "https://cdn.crate.io/downloads/datasets/cratedb-datasets/machine-learning/timeseries/nab-machine-failure.csv"
source = InputOutputResource(url=url)

# Connect to CrateDB Cloud.
with ManagedCluster.from_env() as cluster:

    # Invoke the import job.
    cluster.load_table(source=source)

    # Query imported data.
    results = cluster.query('SELECT * FROM "nab-machine-failure" LIMIT 10;')
    
    # Display data.
    from pprint import pprint
    pprint(results)
```


[change data capture]: https://en.wikipedia.org/wiki/Change_data_capture
[CrateDB Shell]: https://cratedb.com/docs/crate/crash/
[Export API]: https://cratedb.com/docs/cloud/en/latest/cluster/export.html
[Import API]: https://cratedb.com/docs/cloud/en/latest/cluster/import.html
