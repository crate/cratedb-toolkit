# PostgreSQL example from Ibis

## Source
Files have been taken from [Ibis revision cc40bf5].
```shell
cat ibis/ci/schema/postgres.sql
cat ibis/docker/postgres/Dockerfile
```

## Usage

Start database servers:
```shell
docker compose up --build
```

Users must run `psql` from the directory containing `init-full.sql` and the
CSV files so that `\COPY ... FROM 'xxx.csv'` operations can locate them:
```shell
cd examples/cdc/postgresql
```

For a lightweight setup:
```shell
psql postgresql://postgres@localhost:5433 < init-minimal.sql
```
For a PostgreSQL instance including PostGIS and pgvector extensions:
```shell
psql postgresql://postgres@localhost:5433 < init-full.sql
```


[Ibis revision cc40bf5]: https://github.com/ibis-project/ibis/tree/cc40bf5
