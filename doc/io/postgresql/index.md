---
orphan: true
---

# PostgreSQL I/O


## Appendix

### Research

You can shuffle data from PostgreSQL using different methods.

- [Logical Replication]
- [Protocol Replication]

Those replication implementations are relevant.

- [DMS] (logical, plugin: pglogical)
- [DMS] (logical, plugin: wal2json)
- [asyncpg] (logical, native)
- [pgbelt]: (logical, plugin: pglogical, uses asyncpg)
- [psycopg2] (protocol, native)
- [pypg-cdc] (uses psycopg2, modern pgoutput)
- [pypgoutput] (uses psycopg2)
- [python-postgres-cdc] (uses psycopg2, even more modern pgoutput)
- [wal2json] (logical, plugin)

More resources.

- https://github.com/psycopg/psycopg/issues/71
- https://aws.amazon.com/blogs/database/stream-changes-from-amazon-rds-for-postgresql-using-amazon-kinesis-data-streams-and-aws-lambda/


[asyncpg]: https://github.com/MagicStack/asyncpg/issues/91#issuecomment-288085148
[DMS]: https://docs.aws.amazon.com/dms/
[Logical Replication]: https://www.postgresql.org/docs/9.6/logicaldecoding.html
[pgbelt]: https://github.com/Autodesk/pgbelt
[Protocol Replication]: https://www.postgresql.org/docs/9.6/protocol-replication.html
[psycopg2]: https://www.psycopg.org/docs/advanced.html#replication-support
[pypg-cdc]: https://pypi.org/project/pypg-cdc/
[pypgoutput]: https://github.com/dgea005/pypgoutput
[python-postgres-cdc]: https://pypi.org/project/python-postgres-cdc/
[wal2json]: https://github.com/eulerto/wal2json
