# Changes for cratedb-toolkit

## Unreleased

- Refactor strategies `delete`, `reallocate`, and `snapshot`, to
  standalone variants.

- Bundle configuration and runtime settings into `Settings` entity,
  and use more OO instead of weak dictionaries: Add `RetentionStrategy`,
  `TableAddress`, and `Settings` entities, to improve information passing
  throughout the application and the SQL templates.

- Add `--schema` option, and `CRATEDB_EXT_SCHEMA` environment variable,
  to configure the database schema used to store the retention policy
  table. The default value is `ext`.

- Use full-qualified table names everywhere.

- Fix: Compensate for `DROP REPOSITORY` now returning `RepositoryMissingException`
  when the repository does not exist. With previous versions of CrateDB, it was
  `RepositoryUnknownException`.

## 2023/06/27 0.0.0

- Import "data retention" implementation from <https://github.com/crate/crate-airflow-tutorial>.
  Thanks, @hammerhead.
