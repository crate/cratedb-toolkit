(elasticsearch)=

# Elasticsearch

:::{div} sd-text-muted
Load data from Elasticsearch into CrateDB.
:::

[Elasticsearch] is a source-available search engine developed by Elastic.
It is based on Apache Lucene and provides a distributed, multitenant-
capable full-text search engine with an HTTP web interface and schema-
free JSON documents.

## Prerequisites

Use Docker or Podman to run all components. This approach works consistently
across Linux, macOS, and Windows.

## Install
Install the most recent versions of HTTPie and [cratedb-toolkit],
or evaluate {ref}`alternative installation methods <install>`.
```shell
uv tool install --upgrade 'httpie' 'cratedb-toolkit[io-ingest]'
```

## Tutorial

5-minute step-by-step instructions about how
to work with Elasticsearch and CrateDB.

### Services

Run Elasticsearch and CrateDB using Docker or Podman.
```shell
docker run --rm --name=elasticsearch \
  --publish=9200:9200 --env=discovery.type=single-node \
  docker.elastic.co/elasticsearch/elasticsearch:7.17.29
```
```shell
docker run --rm --name=cratedb \
  --publish=4200:4200 --publish=5432:5432 --env=CRATE_HEAP_SIZE=2g \
  docker.io/crate:latest '-Cdiscovery.type=single-node'
```

### Populate data

Create Elasticsearch index.
```shell
http PUT http://localhost:9200/example
```
Acquire example data.
```shell
wget https://cdn.crate.io/downloads/datasets/cratedb-datasets/academy/chicago-data/taxi_details.csv
```
Import data into Elasticsearch.
```shell
ingestr ingest --yes \
  --source-uri "csv://taxi_details.csv" \
  --source-table "data" \
  --dest-uri "elasticsearch://localhost:9200?secure=false" \
  --dest-table "taxi_details"
```

### Load data

Use {ref}`index` to load data from Elasticsearch index into CrateDB table.

```shell
ctk load table \
    "elasticsearch://localhost:9200?secure=false&table=taxi_details" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/taxi_details"
```

### Query data

Inspect CrateDB tables using [crash].
```sh
crash -c "SHOW CREATE TABLE testdrive.taxi_details"
```
```sh
crash -c "SELECT count(*) FROM testdrive.taxi_details"
```
```sh
crash -c "SELECT * FROM testdrive.taxi_details"
```

## Documentation

The Elasticsearch index name can be provided by using the `&table=` query parameter.

:::{include} ../../_cratedb-options.md
:::

## See also

:::{include} /_snippet/ingest-see-also.md
:::

Use [elasticsearch-compose.yml] and [elasticsearch-demo.sh] for an end-to-end
Elasticsearch+CrateDB-in-a-box example ETL rig using {Docker,Podman} Compose.


[crash]: https://pypi.org/project/crash/
[cratedb-toolkit]: https://pypi.org/project/cratedb-toolkit/
[Elasticsearch]: https://www.Elasticsearch.com/
[elasticsearch-compose.yml]: https://github.com/crate/cratedb-examples/blob/main/application/ingestr/elasticsearch-compose.yml
[elasticsearch-demo.sh]: https://github.com/crate/cratedb-examples/blob/main/application/ingestr/elasticsearch-demo.sh
