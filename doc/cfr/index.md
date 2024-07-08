(cfr)=
# CrateDB Cluster Flight Recorder (CFR)

CFR helps to collect information about CrateDB clusters for support requests
and self-service debugging.

## Install
```shell
pip install --upgrade 'cratedb-toolkit[cfr]'
```
Alternatively, use the Docker image at `ghcr.io/crate-workbench/cratedb-toolkit`.

## Synopsis

Define CrateDB database cluster address using the `CRATEDB_SQLALCHEMY_URL`
environment variable.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://localhost/
```

Export system table information into timestamped file, by default into the
current working directory, into a directory using the pattern
`cfr/{clustername}/{timestamp}/sys` directory.
```shell
ctk cfr sys-export
```

Import system table information from given directory.
```shell
ctk cfr sys-import file://./cfr/crate/2024-04-18T01-13-41/sys
```


## Usage

### Target and source directories

The target directory on the export operation, and the source directory on the
import operation, can be specified using a single positional argument on the
command line.

Export system table information into given directory.
```shell
ctk cfr sys-export file:///var/ctk/cfr
```

Import system table information from given directory.
```shell
ctk cfr sys-import file:///var/ctk/cfr/crate/2024-04-18T01-13-41/sys
```

Alternatively, you can use the `CFR_TARGET` and `CFR_SOURCE` environment
variables.

### CrateDB database address

The CrateDB database address can be defined on the command line, using the
`--cratedb-sqlalchemy-url` option, or by using the `CRATEDB_SQLALCHEMY_URL`
environment variable.
```shell
ctk cfr --cratedb-sqlalchemy-url=crate://localhost/ sys-export
```


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
echo "CRATEDB_SQLALCHEMY_URL=crate://localhost/" > .env
alias cfr="docker run --rm -it --network=host --volume=$(PWD)/cfr:/cfr --env-file=.env ghcr.io/crate-workbench/cratedb-toolkit:latest ctk cfr"
```

Export system table information.
```shell
cfr sys-export
```

Import system table information.
```shell
cfr sys-import cfr/crate/2024-04-18T01-13-41/sys
```


```{toctree}
:maxdepth: 1
:hidden:

backlog
```
