(index)=
# CrateDB Toolkit

[![CI][badge-tests]][project-tests]
[![Coverage Status][badge-coverage]][project-codecov]
[![Documentation][badge-documentation]][project-documentation]
[![License][badge-license]][project-license]
[![Downloads per month][badge-downloads-per-month]][project-downloads]

[![Supported Python versions][badge-python-versions]][project-pypi]
[![Status][badge-status]][project-pypi]
[![Package version][badge-package-version]][project-pypi]

```{include} readme.md
:start-line: 12
```

## About

This software package includes a range of modules and subsystems to work
with CrateDB and CrateDB Cloud efficiently.

You can use CrateDB Toolkit to run data I/O procedures and automation tasks
of different kinds around CrateDB and CrateDB Cloud. It can be used both as
a standalone program, and as a library.

It aims for [DWIM]-like usefulness and [UX], and provides CLI and HTTP
interfaces, and others.


## Features

- **Capable:** Connect to the InfluxDB HTTP API, or read from an InfluxDB
  TSM data directory directly.

- **Versatile:** Use it as a command-line program, pipeline element,
  or as a library within your own applications.

- **Polyglot:** Support I/O operations between InfluxDB, any SQL database
  supported by SQLAlchemy, file formats supported by pandas/Dask, and
  the native InfluxDB line protocol (ILP), on both import and export
  directions.


## Synopsis

```shell

# Export from API to database.
influxio copy \
    "http://example:token@localhost:8086/testdrive/demo" \
    "sqlite://export.sqlite?table=demo"

# Export from data directory to line protocol format.
influxio copy \
    "file:///path/to/influxdb/engine?bucket-id=372d1908eab801a6&measurement=demo" \
    "file://export.lp"
```


## Documentation

Please visit the [README](#readme) document to learn what you can do with
the `influxio` package. Effectively, it is all about the `influxio copy`
primitive, which accepts a range of variants on its `SOURCE` and `TARGET`
arguments, in URL formats.


## Development

Contributions are very much welcome. Please visit the [](#sandbox)
documentation to learn about how to spin up a sandbox environment on your
workstation, or create a [ticket][Issues] to report a bug or share an idea
about a possible feature.



```{toctree}
:maxdepth: 3
:caption: Documentation
:hidden:

install
datasets
io/index
retention
```

```{toctree}
:maxdepth: 1
:caption: Workbench
:hidden:

sandbox
changes
backlog
```


[cratedb-toolkit]: https://cratedb-toolkit.readthedocs.io/
[influxio]: https://influxio.readthedocs.io/

[badge-coverage]: https://codecov.io/gh/crate-workbench/cratedb-toolkit/branch/main/graph/badge.svg
[badge-documentation]: https://img.shields.io/readthedocs/cratedb-toolkit
[badge-downloads-per-month]: https://pepy.tech/badge/cratedb-toolkit/month
[badge-license]: https://img.shields.io/github/license/crate-workbench/cratedb-toolkit.svg
[badge-package-version]: https://img.shields.io/pypi/v/cratedb-toolkit.svg
[badge-python-versions]: https://img.shields.io/pypi/pyversions/cratedb-toolkit.svg
[badge-status]: https://img.shields.io/pypi/status/cratedb-toolkit.svg
[badge-tests]: https://github.com/crate-workbench/cratedb-toolkit/actions/workflows/main.yml/badge.svg
[project-codecov]: https://codecov.io/gh/crate-workbench/cratedb-toolkit
[project-documentation]: https://cratedb-toolkit.readthedocs.io/
[project-downloads]: https://pepy.tech/project/cratedb-toolkit/
[project-license]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/LICENSE
[project-pypi]: https://pypi.org/project/cratedb-toolkit
[project-tests]: https://github.com/crate-workbench/cratedb-toolkit/actions/workflows/main.yml
