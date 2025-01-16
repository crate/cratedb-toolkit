(tail)=
# ctk tail

`ctk tail` displays the most recent records of a database table.
It also provides special decoding options for the `sys.jobs_log` table.

:::{rubric} Synopsis
:::
```shell
ctk tail -n 3 sys.summits
```

:::{rubric} Options
:::
You can combine `ctk tail`'s JSON and YAML output with programs like `jq` and `yq`.
```shell
ctk tail -n 3 sys.summits --format=json | jq
ctk tail -n 3 sys.summits --format=yaml | yq
```
Optionally poll the table for new records by using the `--follow` option.
```shell
ctk tail -n 3 doc.mytable --follow
```

:::{rubric} Decoder for `sys.jobs_log`
:::
`ctk tail` provides a special decoder when processing records of the `sys.jobs_log`
table. The default output format `--format=log` prints records in a concise
single-line formatting.
```shell
ctk tail -n 3 sys.jobs_log
```
The `--format=log-pretty` option will format the SQL statements for optimal
copy/paste procedures. Together with the `--follow` option, this provides
optimal support for ad hoc tracing of SQL statements processed by CrateDB.
```shell
ctk tail -n 3 sys.jobs_log --follow --format=log-pretty
```

:::{warning}
Because `ctk tail` works by submitting SQL commands to CrateDB, using its `--follow`
option will spam the `sys.jobs_log` with additional entries. The default interval
is 0.1 seconds, and can be changed using the `--interval` option.
:::
