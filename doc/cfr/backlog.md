# CrateDB CFR Backlog

## Iteration +1
- Software tests
- Converge output into tar archive
- Combine with `ctk wtf info`
  - On sys-export, add it to the CFR package
  - After sys-import, use it to access the imported data

## Iteration +2
- sys-export: Does the program need capabilities to **LIMIT** cardinality
  on `sys-export` operations, for example, when they are super large?
- sys-import: Accept target database schema.

## Iteration +3
- Wie komme ich ans `crate.yaml`?
- Wie komme ich an die Logfiles? `docker log`?
- Use OpenTelemetry traces in one way or another?
- Possibly tap into profiling, using JFR, [profefe](https://github.com/profefe/profefe),
  and/or [Grafana Pyroscope](https://github.com/grafana/pyroscope).

## Done
- Cluster name muss in `cfr/<name>/sys/<timestamp>`, für multi-tenancy operations.
