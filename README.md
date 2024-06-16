# CrateDB Toolkit

[![Tests](https://github.com/crate-workbench/cratedb-toolkit/actions/workflows/main.yml/badge.svg)](https://github.com/crate-workbench/cratedb-toolkit/actions/workflows/main.yml)
[![Test coverage](https://img.shields.io/codecov/c/gh/crate-workbench/cratedb-toolkit.svg)](https://codecov.io/gh/crate-workbench/cratedb-toolkit/)
[![Python versions](https://img.shields.io/pypi/pyversions/cratedb-toolkit.svg)](https://pypi.org/project/cratedb-toolkit/)

[![License](https://img.shields.io/github/license/crate-workbench/cratedb-toolkit.svg)](https://github.com/crate-workbench/cratedb-toolkit/blob/main/LICENSE)
[![Status](https://img.shields.io/pypi/status/cratedb-toolkit.svg)](https://pypi.org/project/cratedb-toolkit/)
[![PyPI](https://img.shields.io/pypi/v/cratedb-toolkit.svg)](https://pypi.org/project/cratedb-toolkit/)
[![Downloads](https://pepy.tech/badge/cratedb-toolkit/month)](https://pepy.tech/project/cratedb-toolkit/)


» [Documentation]
| [Changelog]
| [Community Forum]
| [PyPI]
| [Issues]
| [Source code]
| [License]
| [CrateDB]


## About

This software package includes a range of modules and subsystems to work
with CrateDB and CrateDB Cloud efficiently.

You can use CrateDB Toolkit to run data I/O procedures and automation tasks
of different kinds around CrateDB and CrateDB Cloud. It can be used both as
a standalone program, and as a library.

It aims for [DWIM]-like usefulness and [UX], and provides CLI and HTTP
interfaces, and others.


## Status

Please note that the `cratedb-toolkit` package contains alpha-, beta- and
incubation-quality code, and as such, is considered to be a work in progress.
Contributions of all kinds are much welcome, in order to make it more solid,
and to add features.

Breaking changes should be expected until a 1.0 release, so version pinning is
strongly recommended, especially when using it as a library.


## Install

Install package.
```shell
pip install --upgrade cratedb-toolkit
```

Verify installation.
```shell
ctk --version
```

Run with Docker.
```shell
alias ctk="docker run --rm ghcr.io/crate-workbench/cratedb-toolkit ctk"
ctk --version
```


## Development

Contributions are very much welcome. Please visit the [](#sandbox)
documentation to learn about how to spin up a sandbox environment on your
workstation, or create a [ticket][Issues] to report a bug or share an idea
about a possible feature.



[Changelog]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/CHANGES.md
[Community Forum]: https://community.crate.io/
[CrateDB]: https://crate.io/products/cratedb
[CrateDB Cloud]: https://console.cratedb.cloud/
[Documentation]: https://cratedb-toolkit.readthedocs.io/
[DWIM]: https://en.wikipedia.org/wiki/DWIM
[Issues]: https://github.com/crate-workbench/cratedb-toolkit/issues
[License]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/LICENSE
[PyPI]: https://pypi.org/project/cratedb-toolkit/
[Source code]: https://github.com/crate-workbench/cratedb-toolkit
[UX]: https://en.wikipedia.org/wiki/User_experience
