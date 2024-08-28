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
