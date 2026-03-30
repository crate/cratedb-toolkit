(kinesis)=

# Amazon Kinesis

:::{div} sd-text-muted
Load data from Amazon Kinesis into CrateDB.
:::

[Amazon Kinesis] is a cloud-based service to collect and process large
[streams] of data records in real time.

## Prerequisites

Use Docker or Podman to run all components. This approach works consistently
across Linux, macOS, and Windows.
The tutorial uses [LocalStack] to spin up a local instance of Amazon Kinesis so
you don't need an AWS account to exercise the pipeline.

## Install
Install the most recent Python packages [awscli] and [cratedb-toolkit],
or evaluate {ref}`alternative installation methods <install>`.
```shell
uv tool install --upgrade 'awscli' 'cratedb-toolkit[io-ingest]'
```

## Tutorial

5-minute step-by-step instructions about how
to work with Amazon Kinesis and CrateDB.

### Services

Run Kinesis from LocalStack and CrateDB using Docker or Podman.
```shell
docker run --rm --name=localstack \
  --publish=4566:4566 \
  docker.io/localstack/localstack:latest
```
```shell
docker run --rm --name=cratedb \
  --publish=4200:4200 --publish=5432:5432 --env=CRATE_HEAP_SIZE=2g \
  docker.io/crate:latest '-Cdiscovery.type=single-node'
```

### Configure AWS clients

LocalStack's default region is `us-east-1`. Let's use it.
```shell
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL="http://localhost:4566"
```

### Populate data

Create the Kinesis stream or enumerate existing ones.
```shell
aws kinesis create-stream --stream-name=demo
```
```shell
aws kinesis list-streams
```

Publish two data payloads to the Kinesis stream.
```shell
aws kinesis put-record \
  --stream-name=demo --partition-key=default \
  --data '{"sensor_id":1,"ts":"2025-06-01 10:00","temperature":42.42,"humidity":84.84}'

aws kinesis put-record \
  --stream-name=demo --partition-key=default \
  --data '{"sensor_id":2,"ts":"2025-06-01 11:00","temperature":45.21,"humidity":80.82}'
```

### Load data

Use {ref}`index` to load data from Kinesis stream into CrateDB table.

```shell
ctk load \
    "kinesis:?aws_access_key_id=test&aws_secret_access_key=test&region_name=us-east-1&table=demo" \
    "crate://crate:crate@localhost:4200/testdrive/kinesis"
```

### Query data

Inspect database using [crash](https://pypi.org/project/crash/).
```shell
crash -c "SELECT count(*) FROM testdrive.kinesis"
```
```shell
crash -c "SELECT * FROM testdrive.kinesis"
```
```shell
crash -c "SHOW CREATE TABLE testdrive.kinesis"
```

## Documentation

The Kinesis [StreamName] can be provided by using the `&table=` query parameter.
You can also use a full Kinesis [StreamARN] to address the stream in [ARN]
format.
By default, the Kinesis stream is processed from the beginning, but you can
also adjust its [StartingPosition] to start reading from a specific time.
Currently, the pipeline expects the Kinesis records to be encoded in JSON
format.

A fully qualified Kinesis URL template that uses ARNs to address the Kinesis
stream looks like this.
```shell
kinesis:?aws_access_key_id=${AWS_ACCESS_KEY_ID}&aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}&region_name=${AWS_REGION_NAME}&table=arn:aws:kinesis:${AWS_REGION_NAME}:${AWS_ACCOUNT_ID}:stream/${KINESIS_STREAM_NAME}&start_date=${KINESIS_AT_TIMESTAMP}
```

:::{rubric} Kinesis options
:::

- `aws_access_key_id`: AWS access key ID.
- `aws_secret_access_key`: AWS secret access key.
- `region_name`: AWS region name.
- `table`: Kinesis stream name or ARN.
- `start_date`: Kinesis StartingPosition's `AT_TIMESTAMP` value in ISO format.

When using an ARN to address the Kinesis stream in the `table` parameter,
please adjust values for the AWS region, the AWS account ID, and the Kinesis
stream name, according to the layout below.
```text
# ARN prefix    Region    Account ID   Type   Stream name
arn:aws:kinesis:us-east-1:000000000000:stream/demo
```

The `start_date` option supports various datetime formats,
a few examples are listed below.
```text
%Y-%m-%d: 2023-01-31
%Y-%m-%dT%H:%M:%S: 2023-01-31T15:00:00
%Y-%m-%dT%H:%M:%S%z: 2023-01-31T15:00:00+00:00
%Y-%m-%dT%H:%M:%S.%f: 2023-01-31T15:00:00.000123
%Y-%m-%dT%H:%M:%S.%f%z: 2023-01-31T15:00:00.000123+00:00
```

:::{include} ../_cratedb-options.md
:::

## Checkpointing

By default, the Kinesis CDC relay starts reading from the beginning of the
stream (`TRIM_HORIZON`) on every restart. For long-running pipelines, this
means re-ingesting records that were already written to CrateDB.

To enable persistent checkpointing, add the `checkpointer` query parameter
to the Kinesis URL. The relay will then record its progress and resume from
the last checkpoint on restart.

:::{rubric} Checkpointer URL parameters
:::

- `checkpointer`: Backend type. Supported values: `cratedb`, `memory`.
  Without this parameter, no persistent checkpointing is used.
- `checkpointer-name`: Namespace for checkpoint storage. Defaults to the
  stream name. Use distinct names when multiple relays consume the same stream.
- `checkpointer-schema`: CrateDB schema for the checkpoint table.
  Defaults to `ext`.
- `checkpointer-interval`: Seconds between checkpoint flushes. Defaults to `5`.
  Lower values reduce duplicate records after a crash but increase write load.

:::{rubric} Example: CrateDB checkpointer
:::

```shell
ctk load \
    "kinesis+dms://localhost:4566/demo?region=us-east-1&create=true&checkpointer=cratedb&checkpointer-interval=10" \
    "crate://localhost:4200/testdrive" \
    --transformation examples/cdc/aws/dms-load-schema-universal.yaml
```

The relay creates a `kinesis_checkpoints` table in the configured schema
(default `ext`) to store shard progress. On restart, the consumer resumes
from the last checkpointed sequence number.

:::{rubric} Delivery guarantee
:::

Checkpointed delivery is **at-least-once**. Records between the last flushed
checkpoint and a crash will be replayed on restart. Relay SQL writes should
use upsert or idempotent semantics (`INSERT ... ON CONFLICT`) to handle
duplicate delivery gracefully.

:::{rubric} Maintenance
:::

The checkpoint table stores one row per shard per namespace. For typical
workloads (a handful of streams with 1-10 shards each), the table stays
small indefinitely.

Rows may accumulate over time from decommissioned pipelines or Kinesis shard
splits. Inspect the checkpoint state with:

```sql
SELECT "namespace", "shard_id", "sequence", "active", "updated_at"
FROM "ext"."kinesis_checkpoints"
ORDER BY "updated_at" DESC;
```

Prune stale checkpoints (inactive shards older than 30 days):

```sql
DELETE FROM "ext"."kinesis_checkpoints"
WHERE "active" = FALSE
AND "updated_at" < NOW() - INTERVAL '30 days';
```

To remove all checkpoints for a decommissioned pipeline:

```sql
DELETE FROM "ext"."kinesis_checkpoints"
WHERE "namespace" = 'old-pipeline-name';
```

## See also

:::{include} /_snippet/ingest-see-also.md
:::


[Amazon Kinesis]: https://docs.aws.amazon.com/streams/latest/dev/introduction.html
[ARN]: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html
[awscli]: https://pypi.org/project/awscli/
[cratedb-toolkit]: https://pypi.org/project/cratedb-toolkit/
[LocalStack]: https://www.localstack.cloud/
[StartingPosition]: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StartingPosition.html
[streams]: https://aws.amazon.com/what-is/streaming-data/
[StreamARN]: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescription.html#Streams-Type-StreamDescription-StreamARN
[StreamName]: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescription.html#Streams-Type-StreamDescription-StreamName
