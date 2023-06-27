# Backlog

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

- Use a dedicated schema for retention policy tables, other than `doc`.
- Document usage with Docker, Kubernetes, and Nomad/Waypoint.
- What about an Ubuntu Snap, a Helm chart, or a Nomad Pack?
- Add an audit log, which records events when retention policy rules are
  changed, and executed.
- Use SQLAlchemy as query builder, to prevent SQL injection.
- Provide solid cascaded downsampling implementation like RRDtool or Munin
  - https://bostik.iki.fi/aivoituksia/projects/influxdb-cascaded-downsampling.html
  - https://community.openhab.org/t/influxdb-in-rrd-style/88395
  - https://github.com/influxdata/influxdb/issues/23108
  - https://forums.percona.com/t/data-retention-archive-some-metrics-like-rrdtool-with-1d-resolution/21437
- Clarify how to interpret the `--cutoff-day` option.
- Add policy rule editor UI.
- Improve configurability by offering to configure schema names and such.
  What about details within SQL queries like `ORDER BY 5 ASC`?
- Is "day"-granularity fine with all use-cases? Should it better be generalized?
- Currently, the test for the `reallocate` strategy apparently does not remove any
  records. The reason is probably, because the scenario can't easily be simulated
  on a single-node cluster.
- Ship more package variants: rpm, deb, snap, buildpack?
- Connect to CrateDB Cloud
- Document library and Docker usage
