(cfr)=
# CrateDB Cluster Flight Recorder (CFR)

CrateDB Toolkit provides a few utilities about diagnostics and metadata
information collection and recording per `ctk cfr`.

:::{important}
**Collecting diagnostics for a CrateDB support case? Use `ctk cfr sys-export`.**
It is the recommended entry point, and produces one shareable file. See
{ref}`cfr-systable`.
:::

The three areas differ in how raw vs. interpreted their output is:
- `sys-export` / `sys-import` — a raw copy of every `sys` and
  `information_schema` table, plus your own table and view definitions and a
  manifest describing the collection. **This is the support-case command.**
  See {ref}`cfr-systable`.
- `jobstats collect` / `view` — raw, time-series query statistics, persisted to a schema.
  `report` / `ui` additionally launch an interpreted view on top. Useful for your
  own ongoing query analysis. See {ref}`cfr-jobstats`.
- `info record` — a raw snapshot of `ctk info cluster` and `ctk info jobs`,
  persisted over time. Built on curated, partly interpreted elements.
  See {ref}`cfr-info`.

```{toctree}
:maxdepth: 1

info
jobstats
systable
```
