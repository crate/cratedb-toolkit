(cfr-info)=
# Cluster information recorder

Record complete outcomes of `ctk info cluster` and `ctk info jobs`, the same raw/curated
elements those commands return (see {ref}`cluster-info`), persisted as-is into the
`ext.clusterinfo` and `ext.jobinfo` tables respectively, one row per snapshot.
```shell
ctk cfr info record
```

By default, a new snapshot is recorded every 10 seconds. Use `--once` to record a single
snapshot and exit.
```shell
ctk cfr info record --once
```

:::{tip}
See also {ref}`cluster-info`.
:::
