# Backlog

## Iteration +1
- Add "tags" to data model, for grouping, multi-tenancy, and more.
- Recurrent queries via scheduling.
  Either use classic cron or systemd-timers, or use one of `APScheduler`,
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

## Iteration +2
- Document "Docker Compose" setup variant
- Generalize from `cutoff_day` to `cutoff_date`?
- Refactor SQL queries once more, introducing comment-stripping, and renaming the files.
- Make all tests work completely.
- Battle testing.
- More subcommands, like `list-policies` (`list`) and `check-policies` (`check`).
- Improve how to create a policy, see README and `examples/basic.py`
- Remedy the need to do a `run_sql` step by introducing a subcommand `add-policy`.
- Provide a solid (considering best-practices, DWIM) cascaded/multi-level
  downsampling implementation/toolkit, similar to RRDtool or Munin.

  This probably needs the current strategies to be tagged as `partition`-based
  strategies, because the downsampling strategies do not necessarily need to
  work on/with partitions, right? So, the template variable `policy_dql` becomes
  `partition_policy_dql` (vs. `generic_policy_dql`), and so forth.

  - https://bostik.iki.fi/aivoituksia/projects/influxdb-cascaded-downsampling.html
  - https://community.openhab.org/t/influxdb-in-rrd-style/88395
  - https://github.com/influxdata/influxdb/issues/23108
  - https://forums.percona.com/t/data-retention-archive-some-metrics-like-rrdtool-with-1d-resolution/21437
- OCI: Also build for ARM, maybe only on PRs to `main`, and releases?

## Iteration +3
- Review SQL queries: What about details like `ORDER BY 5 ASC`?
- Use SQLAlchemy as query builder, to prevent SQL injection (S608),
  see `render_delete.py` spike.
- Improve configurability by offering to configure schema names and such.
- Document how to run multi-tenant operations using "tags".
- Add an audit log (`"ext"."jobs_log"`), which records events when retention policy
  rules are changed, or executed.
- Document usage with Kubernetes, and Nomad/Waypoint.
- Job progress

## Iteration +4
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
