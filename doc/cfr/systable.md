(cfr-systable)=

# System table exporter

CFR's `sys-export` and `sys-import` commands support collecting and analyzing
information about CrateDB clusters for support requests and self-service
debugging.

## Install

```shell
pip install --upgrade 'cratedb-toolkit[cfr]'
```
:::{tip}
Alternatively, use the Docker image per `ghcr.io/crate/cratedb-toolkit`.
For more information about installing CrateDB Toolkit, see {ref}`install`.
:::

## Synopsis

Export system table information into timestamped directory using
the pattern `cfr/{clustername}/{timestamp}/sys`.
By default, the working directory is used as the parent folder.
```shell
ctk cfr --cluster-url="crate://localhost:4200/" \
    sys-export file:///var/ctk/cfr
```

Import system table information from directory into given schema.
```shell
ctk cfr --cluster-url="crate://localhost:4200/?schema=case0815" \
    sys-import file://./cfr/crate/2024-04-18T01-13-41/sys
```

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
