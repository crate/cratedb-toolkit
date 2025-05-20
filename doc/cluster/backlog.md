# CrateDB Cluster API backlog

A few collected items about this subsystem, listing both obvious _next-steps_
backlog items, and ideas for future iterations. You are encouraged to add your
own ideas any time about what your dream SDK should look and feel like. Thanks!

Any kind of contribution is very much appreciated: Feel free to pick up
individual backlog items and carry them over into dedicated [issues], if you
want to signal interest in them. If you feel you could whip up and submit
a patch for some item, it will be very much welcome.

[issues]: https://github.com/crate/cratedb-toolkit/issues

## Iteration +1
- CLI: Less verbosity by default for `ctk cluster` operations
  Possibly display cluster operation and job execution outcomes, and `last_async_operation` details
- CLI: Implement `ctk cluster list`, `ctk cluster delete`

## Iteration +2
- Python API: Make `cluster.query` use the excellent `records` package
- Xon.sh example
- Windows support. Powershell?
- Validate multi-cluster, multi-org operations work well and make sense
- Implement JWT token expiry handling
- Add Apache Spark / Databricks to the client bundle?
- Use universal `DatabaseCluster` across the board. For that to happen, it will
  need to gain an `DatabaseCluster.from_env()` entry point.
- Add `WorkspaceClient` as root entrypoint API?
  - https://github.com/databricks/databricks-sdk-py
  - https://github.com/databricks/databricks-sdk-py/tree/main/examples/workspace/warehouses
  - https://github.com/databricks/databricks-sdk-py/blob/main/examples/workspace/statement_execution/execute_tables.py
  - https://databricks-sdk-py.readthedocs.io/en/latest/dataplane.html
  - https://github.com/databricks/databricks-sdk-py/blob/v0.53.0/databricks/sdk/service/serving.py#L4852-L5000
  - https://docs.databricks.com/aws/en/dev-tools/databricks-utils

## Iteration +3
- Complete UI: `ctk cluster health`
- Alternative UI: Status bar
- `ctk cluster copy` (immediate vs. replicate)
- When resuming a cluster concurrently:

  `cratedb_toolkit.exception.CroudException: Another cluster operation is already in progress.`

  => Evaluate `cloud.info.last_async_operation.status` = `SENT|IN_PROGRESS`.

  See also https://github.com/crate/cratedb-toolkit/actions/runs/14682363239/job/41206608090?pr=81#step:5:384.
- CI: Software tests fail when invoked with the whole test suite, but work in isolation
  `test_import_managed_csv_local`
- Expose `ManagedCluster`'s `stop_on_exit` option per environment variable

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
- `ctk cluster start`: Avoid creating a project on each deploy operation
- `cluster/core.py`: FIXME: Accept id or name.
- CrateDB version: How to select CrateDB nightly, like `--version=nightly`?
  => Add `--channel` option.
- Address TODO items in `cratedb_toolkit/cluster/croud.py`:
  - `--product-name=crfree` is not always the right choice. ;]
- Unless the cluster is not healthy, do not access it
  ```
  "health": {
    "last_seen": "",
    "running_operation": "CREATE",
    "status": "UNREACHABLE"
  },

  'health': {'last_seen': '',
             'running_operation': '',
             'status': 'UNREACHABLE'},

  'health': {'last_seen': '2025-05-02T21:33:17.456000',
             'running_operation': '',
             'status': 'GREEN'},
  ```
- Shell: Currently it is not possible to create multiple instances of a `ManagedCluster`
  because parameters like `username` and `password` are obtained from the environment, 
  using different credentials for connecting to individual clusters.
- Document `ManagedCluster`'s `stop_on_exit` option
- `toolkit/cluster/cli.py`: Also respect cluster ID and name across the board of ex-WTF utilities.
- `ctk cluster info` does not include information about CrateDB yet.
  => Connect to ex-WTF utilities.
- Make CLI options override ENV vars. Otherwise, using `--sqlalchemy-url=crate://`
  will never work if an `.env` file is present.
- SDK: Rename `ctk.cluster` to `ctk.sdk`? => No.
- CLI: Shrink address URLs to single parameter `--cluster-url`
- Changelog: Notify about breaking change with input address parameter names
- Docs: Update guidelines about input address parameter preferences
- Managed: Use `keyring` for caching the JWT token, and compensate for token expiry
