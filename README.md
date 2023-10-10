# CrateDB data processing toolkit

[![Tests](https://github.com/crate-workbench/cratedb-retention/actions/workflows/main.yml/badge.svg)](https://github.com/crate-workbench/cratedb-retention/actions/workflows/main.yml)
[![Test coverage](https://img.shields.io/codecov/c/gh/crate-workbench/cratedb-retention.svg)](https://codecov.io/gh/crate-workbench/cratedb-retention/)

» [Documentation]
| [Changelog]
| [Community Forum]
| [PyPI]
| [Issues]
| [Source code]
| [License]
| [CrateDB]


## About

This package is a work in progress, and includes different kinds of modules and
subsystems to work with CrateDB and CrateDB Cloud efficiently.


## Caveat

Please note that the `cratedb-toolkit` package contains alpha- and beta-quality
software, and as such, is considered to be a work in progress. Contributions of
all kinds are very welcome, in order to make it more solid, and to add features.

Breaking changes should be expected until a 1.0 release, so version pinning is
strongly recommended, especially when you use it as a library.


## Install

Install package.
```shell
pip install --upgrade git+https://github.com/crate-workbench/cratedb-retention
```

Verify installation.
```shell
cratedb-retention --version
```

Run with Docker.
```shell
docker run --rm "ghcr.io/crate-workbench/cratedb-retention" cratedb-retention --version
```


## Development

For installing a development sandbox, please refer to the [development sandbox
documentation](./doc/sandbox.md).


[Changelog]: https://github.com/crate-workbench/cratedb-retention/blob/main/CHANGES.md
[Community Forum]: https://community.crate.io/
[CrateDB]: https://crate.io/products/cratedb
[CrateDB Cloud]: https://console.cratedb.cloud/
[Documentation]: https://cratedb-retention.readthedocs.io/
[Issues]: https://github.com/crate-workbench/cratedb-retention/issues
[License]: https://github.com/crate-workbench/cratedb-retention/blob/main/LICENSE
[PyPI]: https://pypi.org/project/cratedb-retention/
[Source code]: https://github.com/crate-workbench/cratedb-retention
