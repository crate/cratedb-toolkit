(cfr)=
# CrateDB Cluster Flight Recorder (CFR)

CrateDB Toolkit provides a few utilities about diagnostics and metadata
information collection and recording per `ctk cfr`.

The three areas differ in how raw vs. interpreted their output is:
- `sys-export` / `sys-import` — a true raw copy of every `sys.*` table. See {ref}`cfr-systable`.
- `jobstats collect` / `view` — raw, time-series query statistics, persisted to a schema.
  `report` / `ui` additionally launch an interpreted, interactive dashboard over the same
  collected data. See {ref}`cfr-jobstats`.
- `info record` — a raw snapshot of `ctk info cluster` and `ctk info jobs`. See {ref}`cfr-info`.

```{toctree}
:maxdepth: 1

info
jobstats
systable
```
