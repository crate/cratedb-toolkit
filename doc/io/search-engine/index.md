(io-search-engine)=

# Search engines

:::{div} sd-text-muted
Import and export data into/from search engines.
:::

## Integrations

:::::{grid} 3
:gutter: 2
:padding: 0

::::{grid-item-card}
:link: elasticsearch
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/elasticsearch.svg
:height: 80px
:alt:
```
+++
Elasticsearch
::::

:::::

```{toctree}
:maxdepth: 1
:hidden:

elasticsearch/index
```

## Synopsis

Load data from Elasticsearch into CrateDB.
```shell
ctk load \
    "elasticsearch://<username>:<password>@es.example.org:9200?secure=false&table=demo" \
    "crate://crate:na@localhost:4200/testdrive/elastic"
```
