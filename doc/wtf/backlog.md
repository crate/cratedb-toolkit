# CrateDB WTF Backlog

## Iteration +1
- Display differences to the standard configuration
- `tail -f` for `sys.jobs_log` and friends
  See [discussion and suggestions](https://github.com/crate-workbench/cratedb-toolkit/pull/88#pullrequestreview-1759838520).
- Check if the patch includes relevant details from here.
  https://community.cratedb.com/t/monitoring-an-on-premises-cratedb-cluster-with-prometheus-and-grafana/1236
- Inform about table sizes, like Admin UI is doing it.
- Inform about [shard imbalances](https://community.cratedb.com/t/cratedb-database-logs-showing-shard-is-now-inactive-and-threads-are-getting-blocked/1617/16).

## Iteration +2
- Make `cratedb-wtf logs` also optionally consider `sys.` tables. 
- cratedb-wtf explore table|shard|partition|node
- High-level analysis, evaluating a set of threshold rules 
- High-level summary reports with heuristics support
- Network diagnostics?
- Provide a GUI?
  https://github.com/davep/pispy

## Iteration +3
- Make it work with CrateDB Cloud.
  ```
  ctk cluster info
  ctk cluster health
  ctk cluster logs --slow-queries
  ```

## Iteration +4
- Expose collected data via Glances-like UI
- Experimental UI using Grafana Scenes

## Done
- Make it work
- Proper marshalling of timestamp values (ISO 8601)
- Expose collected data via HTTP API
  ```
  cratedb-wtf serve
  ```
- Provide `scrub` option also via HTTP
- Complete collected queries and code snippets
- Harvest queries from Admin UI, crash, crate-admin-alt
- Harvest queries from experts
  - https://tools.cr8.net/grafana/d/RkpNJx84z/cratedb-jobs-log?orgId=1&refresh=5m&var-datasource=crate-production
  - https://tools.cr8.net/grafana/d/RkpNJx84z/cratedb-jobs-log?orgId=1&refresh=5m&var-datasource=crate-production&viewPanel=44
- Add `description` and `unit` fields to each `InfoElement` definition
