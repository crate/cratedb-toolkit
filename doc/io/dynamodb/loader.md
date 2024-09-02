(dynamodb-loader)=
# DynamoDB Table Loader

## About
Load data from DynamoDB into CrateDB using a one-stop command
`ctk load table dynamodb://...`, in order to facilitate convenient
data transfers to be used within data pipelines or ad hoc operations.

## Install
```shell
pip install --upgrade 'cratedb-toolkit[dynamodb]'
```

## Usage
Transfer data from DynamoDB table into CrateDB schema/table.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk load table dynamodb://AWS_ACCESS_KEY:AWS_SECRET_ACCESS_KEY@aws/ProductCatalog?region=us-east-1
```

Query data in CrateDB.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk shell --command "SELECT * FROM testdrive.demo;"
ctk show table "testdrive.demo"
```

## Options

### `page-size`
The source URL accepts the `page-size` option to configure DynamoDB
[pagination]. The default value is `1000`.
```shell
ctk load table .../ProductCatalog?region=us-east-1&page-size=5000
```

### `consistent-read`
The source URL accepts the `consistent-read` option to configure DynamoDB
[read consistency]. The default value is `false`.
```shell
ctk load table .../ProductCatalog?region=us-east-1&consistent-read=true
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
use LocalStack to run a DynamoDB service surrogate locally. See also the
[Get started with DynamoDB on LocalStack] tutorial.

For addressing a DynamoDB database on LocalStack, use a command of that shape.
See [Credentials for accessing LocalStack AWS API] for further information.
```shell
ctk load table dynamodb://LSIAQAAAAAAVNCBMPNSG:dummy@localhost:4566/ProductCatalog?region=us-east-1
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


[Credentials for accessing LocalStack AWS API]: https://docs.localstack.cloud/references/credentials/
[Get started with DynamoDB on LocalStack]: https://docs.localstack.cloud/user-guide/aws/dynamodb/
[pagination]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination
[read consistency]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ReadConsistency
