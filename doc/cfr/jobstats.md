(cfr-jobstats)=
# Job statistics collector

Collect and display job statistics. This is a separate, continuously-collected time series
of query statistics, distinct from the one-shot `ctk info jobs` snapshot. 
See {ref}`cluster-info`.

`collect` and `view` return raw, time-series JSON persisted to the `stats` schema. `report`
and `ui` instead launch an interpreted, interactive dashboard on top of the same collected
data.

```shell
export CRATEDB_CLUSTER_URL=crate://crate@localhost:4200/?schema=stats
```
```shell
ctk cfr jobstats collect
ctk cfr jobstats view
ctk cfr jobstats report
ctk cfr jobstats ui
```

Note: Please collect statistics first using `ctk cfr jobstats collect`,
then use the other commands to display or explore them.

:::{rubric} Options
:::
`ctk cfr jobstats collect`:
- `--once` — record only one sample, then exit, instead of collecting continuously
- `--reportdb` / `-r` — a separate database URL to store report data in
  (`crate://crate@localhost:4200/?sslmode=require`)
- `--anonymize` — path to a decoder dictionary file for anonymizing SQL statements before
  they're stored; using the flag without a value defaults to `decoder_dictionary.json`

`ctk cfr jobstats view`:
- `--reportdb` / `-r` — a separate database URL to read report data from
- `--deanonymize` — path to the decoder dictionary file used to reverse `--anonymize`,
  to view statements in their original form
