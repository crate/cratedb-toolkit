# Changes for cratedb-toolkit


## Unreleased


## 2023/10/10 0.0.1

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


## 2023/06/27 0.0.0

- Import "data retention" implementation from <https://github.com/crate/crate-airflow-tutorial>.
  Thanks, @hammerhead.
