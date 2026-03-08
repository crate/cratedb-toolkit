# PostgreSQL to CrateDB loader research

## Full-load

This subsystem is already covered by ingestr/dlt.

## CDC

You can replicate data from PostgreSQL using different methods.

- [Logical Replication]
- [Protocol Replication]

Those replication implementations are relevant.

- [DMS] (logical, plugin: pglogical)
- [DMS] (logical, plugin: wal2json)
- [asyncpg] (logical, native)
- [pgbelt] (logical, plugin: pglogical, uses asyncpg)
- [pg_replicate] (logical, native)
- [psycopg2] (protocol, native)
- [pypg-cdc] (uses psycopg2, modern pgoutput)
- [pypgoutput] (uses psycopg2)
- [python-postgres-cdc] (uses psycopg2, even more modern pgoutput)
- [wal2json] (logical, plugin)

### Evaluation

Better use Apache Flink via [flink-cdc] and the [Flink Postgres CDC Connector]
for protocol replication.

### Alternatives

Python to the rescue?

- [Logical replication with psycopg2 and publishers]
- [cdc_postgres_logical_replication]

Use Kinesis as a data sink.

- [AWS Blog: Stream changes from Amazon RDS for PostgreSQL using Kinesis & Lambda](https://aws.amazon.com/blogs/database/stream-changes-from-amazon-rds-for-postgresql-using-amazon-kinesis-data-streams-and-aws-lambda/)
- [Medium: Stream changes from Amazon RDS for PostgreSQL with Kinesis & Lambda](https://medium.com/@jatinpalsingh81/stream-changes-from-amazon-rds-for-postgresql-using-amazon-kinesis-data-streams-and-aws-lambda-392c557ba046)
- [Commerce Architects: Strategies for replicating data from RDS to Kinesis](https://www.commerce-architects.com/post/exploring-strategies-for-data-replication-from-rds-to-kinesis)

More resources.

- [Psycopg Issue #71: Replication support](https://github.com/psycopg/psycopg/issues/71)
- [asyncpg Issue #91: Logical replication discussion](https://github.com/MagicStack/asyncpg/issues/91)

### supabase/etl

It looks like [pg_replicate] made progress to become a full-blown ETL framework,
now renamed to `supabase/etl`.

- That's a sweet introduction to `pg_replicate` by its author:
  > For the past few months, as part of my job at Supabase, I have been working on
  > [pg_replicate]. pg_replicate lets you easily build applications which can
  > copy data (full table copies and cdc) from Postgres to any other data system.

  -- [pg_replicate is a Rust crate to build Postgres logical replication applications](https://www.reddit.com/r/rust/comments/1eq4ruv/pg_replicate_is_a_rust_crate_to_build_postgres/)


[asyncpg]: https://github.com/MagicStack/asyncpg/issues/91#issuecomment-288085148
[cdc_postgres_logical_replication]: https://github.com/PhantomHunt/cdc_postgres_logical_replication
[DMS]: https://docs.aws.amazon.com/dms/
[flink-cdc]: https://github.com/apache/flink-cdc
[Flink Postgres CDC Connector]: https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/postgres-cdc/
[Logical Replication]: https://www.postgresql.org/docs/9.6/logicaldecoding.html
[Logical replication with psycopg2 and publishers]: https://stackoverflow.com/questions/70120968/logical-replication-with-psycopg2-and-publishers
[pgbelt]: https://github.com/Autodesk/pgbelt
[pg_replicate]: https://github.com/supabase/pg_replicate
[Protocol Replication]: https://www.postgresql.org/docs/9.6/protocol-replication.html
[psycopg2]: https://www.psycopg.org/docs/advanced.html#replication-support
[pypg-cdc]: https://pypi.org/project/pypg-cdc/
[pypgoutput]: https://github.com/dgea005/pypgoutput
[python-postgres-cdc]: https://pypi.org/project/python-postgres-cdc/
[wal2json]: https://github.com/eulerto/wal2json
