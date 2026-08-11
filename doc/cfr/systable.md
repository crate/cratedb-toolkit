(cfr-systable)=

# System table exporter

:::{important}
**`ctk cfr sys-export` is the recommended command for collecting diagnostics for
a CrateDB support case.** Run it, then attach the resulting file to your case.

It collects raw, uninterpreted data only. The other commands under {ref}`cfr`
and {ref}`cluster-info` present curated or interpreted views for your own use.
:::

`sys-export` produces a **diagnostics bundle**: a raw copy of every `sys.*` and
`information_schema` table, together with your own table and view definitions,
and a `manifest.json` describing exactly what was and was not collected.
`sys-import` loads the raw tables from such a bundle back into a cluster for
analysis, one schema subtree at a time.

## Install

```shell
pip install --upgrade 'cratedb-toolkit[cfr]'
```
:::{tip}
Alternatively, use the Docker image per `ghcr.io/crate/cratedb-toolkit`.
For more information about installing CrateDB Toolkit, see {ref}`install`.
:::

## Collecting diagnostics for a support case

```shell
ctk cfr --cluster-url="crate://localhost:4200/" \
    sys-export ./diagnostics.tgz
```

Attach the resulting file to your support case. The log output tells where
it was written.

## Synopsis

Export system table information into a timestamped directory using
the pattern `cfr/{clustername}/{timestamp}`.
By default, the working directory is used as the parent folder.
```shell
ctk cfr --cluster-url="crate://localhost:4200/" \
    sys-export file:///var/ctk/cfr
```

Give the target a `.tgz` or `.tar.gz` name to receive a single archive file
instead.

Import a bundle's raw tables back into a cluster for analysis. Point `sys-import`
at one per-schema subdirectory of the bundle — `sys` or `information_schema` —
and give it a target schema to restore into.
```shell
ctk cfr --cluster-url="crate://localhost:4200/?schema=case0815" \
    sys-import file://./cfr/crate/2024-04-18T01-13-41/sys
```

Table names keep their bundle prefix, so `sys.jobs_log` is restored as
`"case0815"."sys-jobs_log"`, and `information_schema.columns` as
`"case0815"."is-columns"`. The `ddl/` subtree is plain SQL for you to read or
replay yourself; `sys-import` does not consume it.

## Bundle layout

```text
{clustername}/{timestamp}/
├── manifest.json            # what this bundle is, and anything that failed
├── sys/                     # raw `sys.*` tables
│   ├── schema/
│   └── data/
├── information_schema/      # raw `information_schema` tables
│   ├── schema/
│   └── data/
└── ddl/
    ├── tables/              # your tables, per `SHOW CREATE TABLE`
    └── views/               # your views
```

`manifest.json` identifies the collection: cluster name, the cluster's CrateDB
version, the toolkit version, and the timestamp. Lists anything that could
not be collected: `schema_failures` for tables whose `.sql` file is missing, and
`data_failures` for tables whose data could not be read, each with the reason.

## What the bundle contains

A bundle contains cluster metadata, not the contents of your tables.
**No rows from your own tables are exported** — the `ddl/` subtree holds table
and view *definitions* only.

Credentials never reach the bundle. CrateDB itself returns
`sys.users.password`, and `access_key` / `secret_key` in
`sys.repositories.settings`, already redacted; JWT entries carry issuer,
audience, and username, but no token material.

One category does warrant a look before sharing: **`sys.jobs_log`, `sys.jobs`,
`sys.operations_log`, and `sys.sessions` record SQL statements as they were
executed, including literal values.** If your queries embed personal or
otherwise sensitive values, those values appear in the bundle. This is the only
place where data from your tables can be present.

Beyond that, the bundle describes your cluster rather than its contents: schema
names, table and column names and comments; user, role, and privilege names;
client addresses of active sessions; and node hostnames, filesystem paths, and
OS details. `manifest.json` lists exactly which schemas and tables were
collected, so a bundle can be reviewed before it is passed on.

:::{note}
The `--scrub` option does not apply to `sys-export`. It blanks out information
about the local machine environment in `ctk cfr info record`, not data collected
from the cluster.
:::

## Configuration

Alternatively to command-line options, you can use the
`CRATEDB_CLUSTER_URL`, `CFR_SOURCE`, and `CFR_TARGET`
environment variables.

Define CrateDB database cluster address using the
`CRATEDB_CLUSTER_URL` environment variable.
```shell
export CRATEDB_CLUSTER_URL=crate://localhost/
```
Alternatively, use `CRATEDB_CLUSTER_NAME` or `CRATEDB_CLUSTER_ID`
to address a CrateDB Cloud database cluster.

## OCI

If you don't want or can't install the program, you can also use its OCI
container image, for example on Docker, Postman, Kubernetes, and friends.

Optionally, start a CrateDB single-node instance for testing purposes.
```shell
docker run --rm -it \
  --name=cratedb --publish=4200:4200 --env=CRATE_HEAP_SIZE=4g \
  crate/crate:nightly -Cdiscovery.type=single-node
```

Define the database URI address, and an alias to the `cfr` program.
```shell
echo "CRATEDB_CLUSTER_URL=crate://localhost/" > .env
alias cfr="docker run --rm -it --network=host --volume=$(PWD)/cfr:/cfr --env-file=.env ghcr.io/crate/cratedb-toolkit:latest ctk cfr"
```

Export system table information.
```shell
cfr sys-export
```

Import system table information.
```shell
cfr sys-import cfr/crate/2024-04-18T01-13-41/sys
```
