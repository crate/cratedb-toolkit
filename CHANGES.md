# Changelog


## Unreleased

## 2024/07/08 v0.0.15
- IO: Added the `if-exists` query parameter by updating to influxio 0.4.0.
- Rockset: Added CrateDB Rockset Adapter, a HTTP API emulation layer
- MongoDB: Added adapter amalgamating PyMongo to use CrateDB as backend
- SQLAlchemy: Clean up and refactor SQLAlchemy polyfills
  to `cratedb_toolkit.util.sqlalchemy`
- CFR: Build as a self-contained program using PyInstaller
- CFR: Publish self-contained application bundle to GitHub Workflow Artifacts

## 2024/06/18 v0.0.14
- Add `ctk cfr` and `ctk wtf` diagnostics programs
- Remove support for Python 3.7
- SQLAlchemy dialect: Use `sqlalchemy-cratedb>=0.37.0`
  This includes the fix to the `get_table_names()` reflection method.

## 2024/06/11 v0.0.13
- Dependencies: Migrate from `crate[sqlalchemy]` to `sqlalchemy-cratedb`

## 2024/05/30 v0.0.12
- Fix InfluxDB Cloud <-> CrateDB Cloud connectivity by using
  `ssl=true` query argument also for `influxdb2://` source URLs.

## 2024/05/30 v0.0.11
- Fix InfluxDB Cloud <-> CrateDB Cloud connectivity by propagating
  `ssl=true` query argument. Update dependencies to `influxio>=0.2.1,<1`.

## 2024/04/10 v0.0.10
- Dependencies: Unpin upper version bound of `dask`. Otherwise, compatibility
  issues can not be resolved quickly, like with Python 3.11.9.
  https://github.com/dask/dask/issues/11038

## 2024/03/22 v0.0.9
- Dependencies: Use `dask[dataframe]`

## 2024/03/11 v0.0.8
- datasets: Fix compatibility with Python 3.7

## 2024/03/07 v0.0.7
- datasets: Fix dataset loader

## 2024/03/07 v0.0.6

- Added `cratedb_toolkit.datasets` subsystem, for acquiring datasets
  from cratedb-datasets and Kaggle.


## 2024/02/12 v0.0.5

- Do not always activate pytest11 entrypoint to pytest fixture
  `cratedb_service`, as it depends on the `testcontainers` package,
  which is not always installed.


## 2024/02/10 v0.0.4

- Packaging: Use `cloud` extra to install relevant packages
- Dependencies: Add `testing` extra, which installs `testcontainers` only
- Testing: Export `cratedb_service` fixture as pytest11 entrypoint
- Sandbox: Reduce number of extras by just using `all`


## 2024/01/18 v0.0.3

- Add SQL runner utility primitives to `io.sql` namespace
- Add `import_csv_pandas` and `import_csv_dask` utility primitives
- data: Add subsystem for "loading" data.
- Add SDK and CLI for CrateDB Cloud Data Import APIs
  `ctk load table ...`
- Add `migr8` program from previous repository
- InfluxDB: Add adapter for `influxio`
- MongoDB: Add `migr8` program from previous repository
- MongoDB: Improve UX by using `ctk load table mongodb://...`
- load table: Refactor to use more OO
- Add `examples/cloud_import.py`
- Adapt testcontainers to be agnostic of the testing framework.
  Thanks, @pilosus.


## 2023/11/06 v0.0.2

- CLI: Upgrade to `click-aliases>=1.0.2`, fixing erroring out when no group aliases
  are specified.

- Add support for Python 3.12

- SQLAlchemy: Improve UNIQUE constraints polyfill to accept multiple
  column names, for emulating unique composite keys.


## 2023/10/10 v0.0.1

- SQLAlchemy: Add a few patches and polyfills, which do not fit well
  into the vanilla Python driver / SQLAlchemy dialect.

- Retention: Refactor strategies `delete`, `reallocate`, and `snapshot`, to
  standalone variants.

- Retention: Bundle configuration and runtime settings into `Settings` entity,
  and use more OO instead of weak dictionaries: Add `RetentionStrategy`,
  `TableAddress`, and `Settings` entities, to improve information passing
  throughout the application and the SQL templates.

- Retention: Add `--schema` option, and `CRATEDB_EXT_SCHEMA` environment variable,
  to configure the database schema used to store the retention policy
  table. The default value is `ext`.

- Retention: Use full-qualified table names everywhere.

- Retention: Fix: Compensate for `DROP REPOSITORY` now returning `RepositoryMissingException`
  when the repository does not exist. With previous versions of CrateDB, it was
  `RepositoryUnknownException`.


## 2023/06/27 v0.0.0

- Import "data retention" implementation from <https://github.com/crate/crate-airflow-tutorial>.
  Thanks, @hammerhead.
