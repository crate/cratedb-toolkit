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
pip install --upgrade cratedb-toolkit
```

Verify installation.
```shell
cratedb-toolkit --version
```

Run with Docker.
```shell
docker run --rm "ghcr.io/crate-workbench/cratedb-toolkit" cratedb-toolkit --version
```


## Development

For installing a development sandbox, please refer to the [development sandbox
documentation].


[Changelog]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/CHANGES.md
[Community Forum]: https://community.crate.io/
[CrateDB]: https://crate.io/products/cratedb
[CrateDB Cloud]: https://console.cratedb.cloud/
[development sandbox documentation]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/doc/sandbox.md
[Documentation]: https://cratedb-toolkit.readthedocs.io/
[Issues]: https://github.com/crate-workbench/cratedb-toolkit/issues
[License]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/LICENSE
[PyPI]: https://pypi.org/project/cratedb-toolkit/
[Source code]: https://github.com/crate-workbench/cratedb-toolkit
