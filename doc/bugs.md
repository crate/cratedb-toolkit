---
orphan: true
---

# Bugs

## 

```
$ croud clusters import-jobs list --cluster-id=$CRATEDB_CLOUD_CLUSTER_ID --format=table
Traceback (most recent call last):
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/bin/croud", line 8, in <module>
    sys.exit(main())
             ^^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/croud/__main__.py", line 1302, in main
    fn(params)
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/croud/clusters/commands.py", line 292, in import_jobs_list
    print_response(
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/croud/printer.py", line 71, in print_response
    print_format(data, output_fmt, keys, transforms)
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/croud/printer.py", line 46, in print_format
    printer.print_rows(rows)
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/croud/printer.py", line 122, in print_rows
    print(self.format_rows(rows))
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/croud/printer.py", line 165, in format_rows
    values = [
             ^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/croud/printer.py", line 166, in <listcomp>
    [
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/croud/printer.py", line 168, in <listcomp>
    row[header]
    ~~~^^^^^^^^
KeyError: 'url'
```


```
$ ctk load table https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_weather.csv.gz --schema foo
==> Info: Status: REGISTERED (Your import job was received and is pending processing.)
==> Info: Status: SENT (Your creation request was sent to the region.)
==> Info: Done importing 70.00K records
==> Success: Operation completed.
Traceback (most recent call last):
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/bin/ctk", line 8, in <module>
    sys.exit(cli())
             ^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
         ^^^^^^^^^^^^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/.venv/lib/python3.11/site-packages/click/decorators.py", line 33, in new_func
    return f(get_current_context(), *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/cratedb_toolkit/io/cli.py", line 90, in load_table
    job_info, success = cio.load_resource(url=url, schema=schema, table=table)
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/cratedb_toolkit/io/croud.py", line 28, in load_resource
    outcome, success, found = self.find_job(job_id=job_id)
                              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/amo/dev/crate/ecosystem/cratedb-retentions/cratedb_toolkit/io/croud.py", line 45, in find_job
    if job["id"] == job_id:
       ~~~^^^^^^
TypeError: string indices must be integers, not 'str'
```
