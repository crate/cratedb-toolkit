# CrateDB Cluster Flight Recorder (CFR)

Collect required cluster information for support requests
and self-service debugging.


## Synopsis

Define CrateDB database cluster address.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://localhost/
```

Export system table information into timestamped file,
by default into the `cfr/sys` directory.
```shell
ctk cfr sys-export
```


## Usage

Export system table information into given directory.
```shell
ctk cfr sys-export file:///var/ctk/cfr/sys
```

Import system table information from given directory.
```shell
ctk cfr sys-import file://./cfr/sys/2024-04-16T05-43-37
```

In order to define the CrateDB database address on the
command line, use a command like this.
```shell
ctk cfr --cratedb-sqlalchemy-url=crate://localhost/ sys-export
```


## OCI

If you don't want or can't install the program, you can also use its OCI
container image, for example on Docker, Postman, or Kubernetes.

Optionally, start a CrateDB single-node instance for testing purposes.
```shell
docker run --rm -it \
  --name=cratedb --publish=4200:4200 --env=CRATE_HEAP_SIZE=4g \
  crate/crate:nightly -Cdiscovery.type=single-node
```

Define the database URI address, and an alias to the `cfr` program.
```shell
echo "CRATEDB_SQLALCHEMY_URL=crate://localhost/" > .env
alias cfr="docker run --rm -it --network=host --volume=$(PWD)/cfr:/cfr --env-file=.env ghcr.io/crate-workbench/cratedb-toolkit:latest ctk cfr"
```

Verify everything works.
```shell
cfr --help
```
