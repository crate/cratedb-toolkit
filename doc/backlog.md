# Backlog

## Iteration +2
- Address `fix_job_info_table_name`
- Add more items about `ctk load table` to `examples/` folder
  - Python, Bash
- Cloud: Parallelize import jobs?
- Bug: Use CRATEDB_USERNAME=admin from cluster-info
- Cloud: Tests for uploading a local file 
- Cloud: Use `.ini` file and `keyring` for storing CrateDB Cloud Cluster ID and credentials
- Cloud: List RUNNING/FAILED/SUCCEEDED jobs
- Cloud: Sanitize file name `yc.2019.07-tiny.parquet` to be accepted as table name
- `ctk load table`: Accept `offset`/`limit` and `start`/`stop` options
  - Humanized: https://github.com/panodata/aika
- UX: Unlock `testdata://` data sources from `influxio`
- UX: No stack traces when `cratedb_toolkit.util.croud.CroudException: 401 - Unauthorized`
- UX: Explain `cratedb_toolkit.util.croud.CroudException: Another cluster operation is currently in progress, please try again later.`
- UX: Explain `cratedb_toolkit.util.croud.CroudException: Resource not found.` when accessing unknown cluster id.
- UX: Make `ctk list-jobs` respect `"status": "SUCCEEDED"` etc.
- UX: Improve textual report from `ctk load table`
- UX: Accept alias `--format {jsonl,ndjson}` for `--format json_row` 
- Catch recursion errors:
  ```
  CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/
  ```
- CLI: Verify exit codes.
- UX: Rename `ctk cluster info` to `ctk status cluster --id=foo-bar-baz`
- UX: Add `ctk start cluster --id=foo-bar-baz`
- UX: Provide Bash/zsh completion
- Beautify `list-jobs` output
- `ctk list-clusters`
- Store `CRATEDB_CLOUD_CLUSTER_ID` into `cratedb_toolkit.constants`
- Cloud Tests: Verify file uploads
- Docs: Add examples in more languages: Java, JavaScript, Lua, PHP
- Docs:
  - https://pypi.org/project/red-panda/
  - https://pypi.org/project/redpanda/
    https://github.com/amancevice/redpanda
  - https://pypi.org/project/alyeska/
- Kafka:
  - https://github.com/bakdata/streams-bootstrap
  - https://pypi.org/project/kashpy/
- CFR/WTF
  - https://github.com/peekjef72/sql_exporter
- Migrate / I/O adapter
  - https://community.cratedb.com/t/migrating-from-postgresql-or-timescale-to-cratedb/620

## Iteration +2.5
- Retention: Improve retention subsystem CLI API.
  ```shell
  ctk retention create-policy lalala
  ctk materialized create lalala
  ctk schedule add lalala
  ```
- Retention: Make `--cutoff-day` optional, use `today()` as default.
- Retention: Refactor "partition"-based strategies into subfamily/category, in order to
  make room for other types of strategies not necessarily using partitioned
  tables.
- Retention: Add `examples/retention_tags.py`.

## Iteration +3
- CI: Nightly builds, to verify regressions on CrateDB
- CI: Also build OCI images for ARM, maybe only on PRs to `main`, and releases?
- CI: Add code coverage tracking and reporting.
- More subcommands, like `list-policies` (`list`) and `check-policies` (`check`).
- Improve testing for the `reallocate` strategy.
- Provide components for emulating materialized views
  - https://en.wikipedia.org/wiki/Materialized_view
  - https://github.com/crate/crate/issues/10661
  - https://github.com/crate/crate/issues/8806
  - https://github.com/crate/crate/issues/10803
  - Example:
    ```shell
    cratedb-retention create-materialized-view doc.raw_metrics \
      --as='SELECT * FROM <table_name>;'
    cratedb-retention refresh-materialized-view doc.raw_metrics
    ```
- CI Testcontainers
  - https://github.com/testcontainers/testcontainers-python/blob/main/core/tests/test_docker_in_docker.py
  - https://github.com/actions/runner-images/issues/17

## Iteration +4
Add two non-partition-based strategies. Category: `timerange`.

- Add a shortcut interface for adding policies.
  - Provide a TTL-like interface.
    - https://iotdb.apache.org/UserGuide/V1.1.x/Delete-Data/TTL.html
    - https://iotdb.apache.org/UserGuide/V1.1.x/Query-Data/Continuous-Query.html#downsampling-and-data-retention
  - Rename `retention period` to `duration`. It is shorter, and aligns with InfluxDB.
    - https://docs.influxdata.com/influxdb/v1.8/query_language/manage-database/#create-retention-policies-with-create-retention-policy
    - https://docs.influxdata.com/influxdb/v1.8/query_language/spec/#durations
  - Example:
    ```shell
    cratedb-retention set-ttl doc.raw_metrics \
      --strategy=delete --duration=1w
    ```
- Provide a solid (considering best-practices, DWIM) cascaded/multi-level
  downsampling implementation/toolkit, similar to RRDtool or Munin.

  - https://bostik.iki.fi/aivoituksia/projects/influxdb-cascaded-downsampling.html
  - https://community.openhab.org/t/influxdb-in-rrd-style/88395
  - https://github.com/influxdata/influxdb/issues/23108
  - https://forums.percona.com/t/data-retention-archive-some-metrics-like-rrdtool-with-1d-resolution/21437
- Naming things: Generalize to `supertask`.
  - Examples
  ```shell
  # Example for a shortcut form of `supertask create-retention-policy`.
  #      <TABLE>             <STRATEGY>:<TARGET>  <DURATION>
  st ttl doc.sensor_readings snapshot:export_cold 365d
  ```
  When using partition-based retention, previously using the `--partition-column=time_month`
  option, that syntax might be suitable:
  ```shell
  #      <TABLE>             <PARTCOL>  <STRATEGY>:<TARGET>  <DURATION>
  st ttl doc.sensor_readings:time_month snapshot:export_cold 4w
  ```

## Iteration +5
- Periodic/recurrent queries via scheduling.
  - https://github.com/crate/crate/issues/11182
  - https://github.com/crate/crate-insights/issues/75
  - Humanized: https://github.com/panodata/aika

  Either use classic cron, or systemd-timers, or use one of `APScheduler`,
  `schedule`, or `scheduler`.

  ```python
  import datetime as dt
  import pendulum

  @dag(
      start_date=dt.datetime.strptime("2021-11-19"),
      # start_date=pendulum.datetime(2021, 11, 19, tz="UTC"),
      schedule="@daily",
      catchup=False,
  )
  ```

  - https://github.com/agronholm/apscheduler
  - https://github.com/dbader/schedule
  - https://gitlab.com/DigonIO/scheduler
- Document complete "Docker Compose" setup variant, using both CrateDB and `cratedb-retention`
- Generalize from `cutoff_day` to `cutoff_date`?
  For example, use `ms`. See https://iotdb.apache.org/UserGuide/V1.1.x/Delete-Data/TTL.html.
- More battle testing, in sandboxes and on production systems.
- Use storage classes
  - https://github.com/crate/crate/issues/14298
  - https://github.com/crate/crate/pull/14346

## Iteration +6
- Review SQL queries: What about details like `ORDER BY 5 ASC`?
- Use SQLAlchemy as query builder, to prevent SQL injection (S608),
  see `render_delete.py` spike.
- Improve configurability by offering to configure schema names and such.
- Document how to run multi-tenant operations using "tags".
- Add an audit log (`"ext"."jobs_log"`), which records events when retention policy
  rules are changed, or executed.
- Add Webhooks, to connect to other systems
- Document usage with Kubernetes, and Nomad/Waypoint.
- Job progress

## Iteration +7
- More packaging: Use `fpm`
- More packaging: What about an Ubuntu Snap, a Helm chart, or a Nomad Pack?
- Clarify how to interpret the `--cutoff-day` option.
- Add policy rule editor UI.
- Is "day"-granularity fine with all use-cases? Should it better be generalized?
- Currently, the test for the `reallocate` strategy apparently does not remove any
  records. The reason is probably, because the scenario can't easily be simulated
  on a single-node cluster.
- Ship more package variants: rpm, deb, snap, buildpack?
- Verify Docker setup on Windows

## Done
- Use a dedicated schema for retention policy tables, other than `doc`.
- Refactoring: Manifest the "retention policy" as code entity,
  using dataclasses, or SQLAlchemy.
- Document how to connect to CrateDB Cloud
- Add `DatabaseAddress` entity, with `.safe` property to omit eventual passwords
- Document library and Docker use
- README: Add a good header, with links to relevant resources
- Naming things: Use "toolkit" instead of "manager".
- Document the layout of the retention policy
  entity, and the meaning of its attributes.
- CI: Rename OCI workflow build steps.
- Move `strategy` column on first position of retention policy table,
  and update all corresponding occurrences.
- Add "tags" to data model, for grouping, multi-tenancy, and more.
- Improve example
- Introduce database and CLI API for *editing* records
- List all tags
- Examples: Add "full" example to `basic.py`, rename to `full.py`
- Improve tests by using `generate_series`
- Document compact invocation, after applying an alias and exporting an
  environment variable: `cratedb-retention rm --tags=baz`
- Default value for `"${CRATEDB_URI}"` aka. `dburi` argument
- Add additional check if data table(s) exists, or not.
- Dissolve JOIN-based retention task gathering, because, when the application
  does not discover any retention policy job, it can not discriminate between
  "no retention policy" and "no data", and thus, it is not able to report about
  it correspondingly.
- CLI: Provide `--dry-run` option
- Docs: Before running the examples, need to invoke `cratedb-retention setup --schema=examples`
- For testing the snapshot strategy, provide an embedded MinIO S3 instance to the test suite.
- Improve SNAPSHOT testing: Microsoft Azure Blob Storage
  - https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite
  - https://learn.microsoft.com/en-us/azure/storage/blobs/use-azurite-to-run-automated-tests
- Improve SNAPSHOT testing: Filesystem
- UX: Refactoring towards `cratedb-toolkit`.
- UX: `ctk load`: Clearly disambiguate between loading data into
  RDBMS database tables, blob tables, or filesystem objects.
  ```shell
  ctk load table https://s3.amazonaws.com/my.import.data.gz
  ```
  ```shell
  ctk load blob /path/to/image.png
  ```
  ```shell
  ctk load object /local/path/to/image.png /dbfs/assets
  ```
