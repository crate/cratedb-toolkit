(dynamodb-cdc)=
# DynamoDB CDC Relay

## About
Relay data changes from DynamoDB into CrateDB using a one-stop command
`ctk load table kinesis+dynamodb+cdc://...`, in order to facilitate
convenient data transfers to be used within data pipelines or ad hoc
operations.

It taps into [Change data capture for DynamoDB Streams], in this case
using [Kinesis Data Streams]. It is the sister to the corresponding
full-load implementation, [](#dynamodb-loader).

## Install
```shell
pip install --upgrade 'cratedb-toolkit[kinesis]'
```

## Usage
Consume data from Kinesis Data Stream of DynamoDB CDC events into
CrateDB schema/table.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk load table kinesis+dynamodb+cdc://AWS_ACCESS_KEY:AWS_SECRET_ACCESS_KEY@aws/cdc-stream?region=eu-central-1
```

Query data in CrateDB.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk shell --command "SELECT * FROM testdrive.demo;"
ctk show table "testdrive.demo"
```

## Options

### Batch Size
The source URL option `batch-size` configures how many items to consume from
the Kinesis Stream at once. The default value is `100`.
For many datasets, a much larger batch size is applicable for most efficient
data transfers.
```shell
ctk load table .../cdc-stream?batch-size=5000
```

### Create
The source URL option `create` configures whether the designated Kinesis Stream
should be created upfront. The default value is `false`.
```shell
ctk load table .../cdc-stream?create=true
```

### Create Shards
The source URL option `create-shards` configures whether the designated number
of shards when a Kinesis Stream is created before consuming.
The default value is `1`.
```shell
ctk load table .../cdc-stream?create=true&create-shards=4
```

### Region
The source URL accepts the `region` option to configure the AWS region
label. The default value is `us-east-1`.
```shell
ctk load table .../cdc-stream?region=eu-central-1
```

### Start
The source URL accepts the `start` option to configure the DynamoDB [ShardIteratorType].
It accepts the following values, mapping to corresponding original options. The default
value is `earliest`.

```shell
ctk load table .../cdc-stream?start=latest
```

- `start=earliest`

  Start reading at the last (untrimmed) stream record, which is the oldest record in the
  shard. In DynamoDB Streams, there is a 24 hour limit on data retention. Stream records
  whose age exceeds this limit are subject to removal (trimming) from the stream.
  This option equals `ShardIteratorType=TRIM_HORIZON`.

- `start=latest`

  Start reading just after the most recent stream record in the shard, so that you always
  read the most recent data in the shard. This option equals `ShardIteratorType=LATEST`.

- `start=seqno-at&seqno=...`

  Start reading exactly from the position denoted by a specific sequence number.
  This option equals `ShardIteratorType=AT_SEQUENCE_NUMBER` and `SequenceNumber=...`.

- `start=seqno-after&seqno=...`

  Start reading right after the position denoted by a specific sequence number.
  This option equals `ShardIteratorType=AFTER_SEQUENCE_NUMBER` and `SequenceNumber=...`.


### SeqNo
The source URL accepts the `seqno` option to configure the DynamoDB [SequenceNumber]
parameter. It accepts the sequence number of a stream record in the shard from which
to start reading.
```shell
ctk load table .../cdc-stream?start=seqno-after&seqno=49590338271490256608559692538361571095921575989136588898
```

### Idle Sleep
The `idle-sleep` option configures the waiting time to hibernate the event loop after
running out of items to consume. The default value is `0.5`.

### Buffer Time
The `buffer-time` option configures the time to wait before flushing produced items
to the wire. The default value is `0.5`.


## Variants

### CrateDB Cloud
When aiming to transfer data to CrateDB Cloud, the shape of the target URL
looks like that.
```shell
export CRATEDB_SQLALCHEMY_URL='crate://admin:dZ...6LqB@testdrive.eks1.eu-west-1.aws.cratedb.net:4200/?ssl=true'
```

### LocalStack
In order to exercise data transfers exclusively on your workstation, you can
use LocalStack to run DynamoDB and Kinesis service surrogates locally. See
also the [Get started with Kinesis on LocalStack] tutorial.

For addressing a Kinesis Data Stream on LocalStack, use a command of that shape.
See [Credentials for accessing LocalStack AWS API] for further information.
```shell
ctk load table kinesis+dynamodb+cdc://LSIAQAAAAAAVNCBMPNSG:dummy@localhost:4566/cdc-stream?region=eu-central-1
```

:::{tip}
LocalStack is a cloud service emulator that runs in a single container on your
laptop or in your CI environment. With LocalStack, you can run your AWS
applications or Lambdas entirely on your local machine without connecting to
a remote cloud provider.

In order to invoke LocalStack on your workstation, you can use this Docker
command.
```shell
docker run \
  --rm -it \
  -p 127.0.0.1:4566:4566 \
  -p 127.0.0.1:4510-4559:4510-4559 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  localstack/localstack:latest
```
:::


[Change data capture for DynamoDB Streams]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
[Credentials for accessing LocalStack AWS API]: https://docs.localstack.cloud/references/credentials/
[Get started with Kinesis on LocalStack]: https://docs.localstack.cloud/user-guide/aws/kinesis/
[Kinesis Data Streams]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/kds.html
[SequenceNumber]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html#DDB-streams_GetShardIterator-request-SequenceNumber
[ShardIteratorType]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html#DDB-streams_GetShardIterator-request-ShardIteratorType
