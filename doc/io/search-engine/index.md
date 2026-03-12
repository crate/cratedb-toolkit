(io-search-engine)=

# Search engines

:::{div} sd-text-muted
Import and export data into/from search engines.
:::

## Integrations

```{toctree}
:maxdepth: 1

elasticsearch/index
```

## Synopsis

Load data from Apache Solr into CrateDB.
```shell
ctk load table \
    "solr://<username>:<password>@<host>:<port>/solr/<collection>?table=<collection>" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/solr_demo"
```

Load data from Elasticsearch into CrateDB.
```shell
ctk load table \
    "elasticsearch://<username>:<password>@es.example.org:9200?secure=false&verify_certs=false&table=demo" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/elastic_demo"
```
