# Development Sandbox

## Introduction

It is recommended to use a Python virtualenv for the subsequent operations.
If you something gets messed up during development, it is easy to nuke the
installation, and start from scratch.
```shell
python3 -m venv .venv
source .venv/bin/activate
```

Acquire sources.
```shell
git clone https://github.com/crate-workbench/cratedb-toolkit
cd cratedb-toolkit
```

Install project in sandbox mode.
```shell
pip install --editable=.[develop,test]
```

Run tests. `TC_KEEPALIVE` keeps the auxiliary service containers running, which
speeds up runtime on subsequent invocations. Note that the test suite uses the
`testdrive-ext` schema for storing the retention policy table, and the
`testdrive-data` schema for storing data tables.
```shell
export TC_KEEPALIVE=true
poe check
```

In order to shut down and destroy the CrateDB container, which was started by
the test suite, and was kept running by using `TC_KEEPALIVE`, use this command.
```shell
docker rm --force testcontainers-cratedb
```

Format code.
```shell
poe format
```
