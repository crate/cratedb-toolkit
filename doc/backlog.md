# Backlog

## Iteration +1
- Use a dedicated schema for retention policy tables, other than `doc`.
- Document library and Docker usage
- Connect to CrateDB Cloud
- Use SQLAlchemy as query builder, to prevent SQL injection (S608).
- Provide a solid (considering best-practices, DWIM) cascaded/multi-level
  downsampling implementation/toolkit, similar to RRDtool or Munin.
  - https://bostik.iki.fi/aivoituksia/projects/influxdb-cascaded-downsampling.html
  - https://community.openhab.org/t/influxdb-in-rrd-style/88395
  - https://github.com/influxdata/influxdb/issues/23108
  - https://forums.percona.com/t/data-retention-archive-some-metrics-like-rrdtool-with-1d-resolution/21437

## Iteration +2
- Improve configurability by offering to configure schema names and such.
  What about details within SQL queries like `ORDER BY 5 ASC`?
- Outline how to run multi-tenant operations.
- Add an audit log, which records events when retention policy rules are
  changed, and executed.
- Document usage with Kubernetes, and Nomad/Waypoint.

## Iteration +3
- Recurrent queries via scheduling.
  Yes, or no? If yes, use one of `APScheduler`, `schedule`, or `scheduler`.

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

- What about an Ubuntu Snap, a Helm chart, or a Nomad Pack?
- Clarify how to interpret the `--cutoff-day` option.
- Add policy rule editor UI.
- Is "day"-granularity fine with all use-cases? Should it better be generalized?
- Currently, the test for the `reallocate` strategy apparently does not remove any
  records. The reason is probably, because the scenario can't easily be simulated
  on a single-node cluster.
- Ship more package variants: rpm, deb, snap, buildpack?
- Job progress
- Tags for data model
