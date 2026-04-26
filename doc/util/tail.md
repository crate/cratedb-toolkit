(tail)=
# ctk tail

`ctk tail` displays the "most recent" records in a database table.
It also provides special decoding options for the `sys.jobs_log` table.

## General usage

:::{rubric} Synopsis
:::

CrateDB Toolkit is installed on your machine.
```shell
ctk tail -n 3 sys.summits
```
CrateDB Toolkit is not installed on your machine, but [uv] is.
```shell
uvx --from=cratedb-toolkit ctk tail -n 3 sys.summits
```

:::{rubric} Options
:::
You can combine `ctk tail`'s JSON and YAML output with programs like
`jq` and `yq`.
```shell
ctk tail -n 3 sys.summits --format=json | jq
ctk tail -n 3 sys.summits --format=yaml | yq
```

## Feature: Option `--follow`

Optionally poll the table for new records by using the `--follow` option.
```shell
ctk tail -n 3 doc.mytable --follow
```

:::{warning}
Because `ctk tail` works by submitting SQL commands to CrateDB, using its
`--follow` option will spam the `sys.jobs_log` with additional entries.
The default interval is 0.1 seconds, and can be changed using the
`--interval` option. Please handle with care, and don't leave stray
commands running on your database cluster unattended.
:::

## Feature: Decoding `sys.jobs_log`

`ctk tail` provides a special decoder when processing records of the
`sys.jobs_log` table. The default output format `--format=log` prints
records in a concise single-line formatting.
```shell
ctk tail -n 3 sys.jobs_log
```

The `--format=log-pretty` option will format the SQL statements nicely
pretty-printed, ready for copy/paste into other documents.
```shell
ctk tail -n 3 sys.jobs_log --follow --format=log-pretty
```

Together with the `--follow` option, this provides optimal support for
ad hoc tracing of SQL statements processed by CrateDB.

:::{rubric} Example
:::
```shell
docker run --rm --publish='4200:4200' crate '-Cdiscovery.type=single-node'
export CRATEDB_CLUSTER_URL='crate://'
uvx --from=cratedb-toolkit ctk tail -n 3 --follow --format=log-pretty sys.jobs_log
```
```sql
2026-04-26 02:19:45.903 [0:00:01] INFO    : Success SQL:
SELECT last_execution
FROM "stats".jobstats_last
2026-04-26 02:19:45.906 [0:00:04] INFO    : Success SQL:
SELECT id,
       stmt,
       calls,
       avg_duration,
       bucket,
       username,
       query_type,
       nodes,
       last_used
FROM "stats".jobstats_statements
ORDER BY calls DESC,
         avg_duration DESC;
```



[uv]: https://docs.astral.sh/uv/
