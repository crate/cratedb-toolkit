# CrateDB Cluster API backlog

A few collected items about this subsystem, listing both obvious _next-steps_
backlog items, and ideas for future iterations. You are encouraged to add your
own ideas any time about how your dream SDK should look and feel like. Thanks! 

Any kind of contribution is very much appreciated: Feel free to pick up
individual backlog items and carry them over into dedicated [issues], if you
want to signal interest about them. If you feel you could whip up and submit
a patch for some item, it will be very much welcome.

[issues]: https://github.com/crate/cratedb-toolkit/issues

## Iteration +1
- `ctk cluster start`: Avoid creating a project on each deploy operation
- Address TODO items in `cratedb_toolkit/cluster/croud.py`:
  - `--product-name=crfree` is not always the right choice. ;]
  - How to select CrateDB nightly, like `--version=nightly`?
    => Add `--channel` option!?

## Iteration +2
- Python API: Make `cluster.query` use the excellent `records` package
- Xon.sh example
- Add Apache Spark / Databricks to client bundle
- Windows support. Powershell?
- `ctk cluster list`: New
- `cluster/core.py`: FIXME: Accept id or name.
- Fix software tests `test_import_managed_csv_local`
- Shell: Currently, because parameters like `username` and `password` are obtained from
  the environment, it is not possible to create multiple instances of a `ManagedCluster`,
  using different credentials for connecting to individual clusters.
- Document `ManagedCluster`'s `stop_on_exit` option
- Implement JWT token expiry handling

## Iteration +3
- `toolkit/api/cli.py`: Also respect cluster ID and name across the board of ex-WTF utilities.
- `ctk cluster info` does not include information about CrateDB yet.
  => Connect to ex-WTF utilities.
- Less verbosity by default for `ctk cluster` operations.
  Possibly display cluster operation and job execution outcomes, and `last_async_operation` details
- Complete UI: `ctk cluster health`
- Alternative UI: Status bar
- `ctk cluster copy` (immediate vs. replicate)
- When resuming a cluster concurrently:

  `cratedb_toolkit.exception.CroudException: Another cluster operation is already in progress.`

  => Evaluate `cloud.info.last_async_operation.status` = `SENT|IN_PROGRESS`.

  See also https://github.com/crate/cratedb-toolkit/actions/runs/14682363239/job/41206608090?pr=81#step:5:384.

## Done
- Update `shunit2`. -- https://github.com/kward/shunit2
- Refactor from `ctk.api` to `ctk.cluster`
- `StandaloneCluster.get_client_bundle()` needs implementation
- Documentation
- Naming things: s/UniversalCluster/DatabaseCluster/`
- CLI: Naming things: Rename `--cratedb-sqlalchemy-url` to `--sqlalchemy-url`, etc.
- CLI: Naming things: Rename `--cluster-id` to `--id`, etc.
- CLI: Naming things: Rename `CRATEDB_CLOUD_CLUSTER_NAME` to `CRATEDB_CLUSTER_NAME`, etc. 
- Unlock JWT authentication
- Python API: Fluent design
- Context manager for `ManagedCluster`
  ```python
  # The context manager stops the cluster automatically on exit.
  with ManagedCluster.from_env().start() as cluster:
      # Do something with the cluster
      results = cluster.query("SELECT * FROM sys.nodes")
  # Cluster is automatically suspended when exiting the block
  ```
