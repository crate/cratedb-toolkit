# DynamoDB CDC Relay with AWS Lambda


## What's Inside
- A convenient [Infrastructure as code (IaC)] procedure to define data pipelines on [AWS].
- Written in Python, using [AWS CloudFormation] stack deployments. To learn
  what's behind, see also [How CloudFormation works].
- Code for running on [AWS Lambda] is packaged into [OCI] images, for efficient
  delta transfers, built-in versioning, and testing purposes.


## Details
- This specific document includes a few general guidelines, and a
  a few specifics coming from `examples/aws/dynamodb_kinesis_lambda_oci_cratedb.py`.
- That program defines a pipeline which looks like this:
  
  DynamoDB CDC -> Kinesis Stream -> Python Lambda via OCI -> CrateDB Cloud

For exercising an AWS pipeline, you need two components: The IaC description,
and a record processor implementation for the AWS Lambda.

The IaC description will deploy a complete software stack for demonstration
purposes, including a DynamoDB Table, connected to a Kinesis Stream.


## Prerequisites

### CrateDB
This walkthrough assumes a running CrateDB cluster, and focuses on CrateDB Cloud. 
It does not provide relevant guidelines to set up a cluster, yet.

### OCI image
In order to package code for AWS Lambda functions packages into OCI images,
and use them, you will need to publish them to the AWS ECR container image
registry.

You will need to authenticate your local Docker environment, and create a
container image repository once for each project using a different runtime
image.

Define your AWS ID, region label, and repository name, to be able to use
the templated commands 1:1.
```shell
aws_id=831394476016
aws_region=eu-central-1
repository_name=kinesis-cratedb-processor-lambda
```
```shell
aws ecr get-login-password --region=${aws_region} | \
    docker login --username AWS --password-stdin ${aws_id}.dkr.ecr.${aws_region}.amazonaws.com
```

(ecr-repository)=
### ECR Repository
Just once, before proceeding, create an image repository hosting the runtime
code for your Lambda function.
```shell
aws ecr create-repository --region=${aws_region} \
    --repository-name=${repository_name} --image-tag-mutability=MUTABLE
```
In order to allow others to pull that image, you will need to define a
[repository policy] using the [set-repository-policy] subcommend of the AWS CLI.
In order to invoke that command, put the [](project:#ecr-repository-policy)
JSON definition into a file called `policy.json`.
```shell
aws ecr set-repository-policy --repository-name=${repository_name} --policy-text file://policy.json
```


## Install
In order to exercise the example outlined below, you need to install
CrateDB Toolkit with the "kinesis" extension, because CDC data will be
relayed using AWS Kinesis.
```shell
pip install 'cratedb-toolkit[kinesis]'
```


## Usage

:::{rubric} Configure
:::
```shell
export CRATEDB_HTTP_URL='https://admin:dZ...6LqB@testdrive.eks1.eu-west-1.aws.cratedb.net:4200/'
export CRATEDB_SQLALCHEMY_URL='crate://admin:dZ...6LqB@testdrive.eks1.eu-west-1.aws.cratedb.net:4200/?ssl=true'
```

:::{rubric} CrateDB Table
:::
The destination table name in CrateDB, where the CDC record
processor will re-materialize CDC events into.
```shell
pip install crash
crash --hosts "${CRATEDB_HTTP_URL}" -c 'CREATE TABLE "demo-sink" (data OBJECT(DYNAMIC));'
```

:::{rubric} Invoke pipeline
:::
Package the Lambda function, upload it, and deploy demo software stack.
```shell
python dynamodb_kinesis_lambda_oci_cratedb.py
```
For example, choose those two variants:

- IaC driver: [dynamodb_kinesis_lambda_oci_cratedb.py]
- Record processor: [kinesis_lambda.py]

Putting them next to each other into a directory, and adjusting
`LambdaPythonImage(entrypoint_file=...)` to point to the second,
should be enough to get you started.


:::{rubric} Trigger CDC events
:::
Inserting a document into the DynamoDB table, and updating it, will trigger two CDC events.
```shell
READING_SQL="{'timestamp': '2024-07-12T01:17:42', 'device': 'foo', 'temperature': 42.42, 'humidity': 84.84}"
READING_WHERE="\"device\"='foo' AND \"timestamp\"='2024-07-12T01:17:42'"

aws dynamodb execute-statement --statement \
  "INSERT INTO \"demo-source\" VALUE ${READING_SQL};"

aws dynamodb execute-statement --statement \
  "UPDATE \"demo-source\" SET temperature=43.59 WHERE ${READING_WHERE};"
```

:::{rubric} Query data in CrateDB
:::
When the stream delivered the CDC data to the processor, and everything worked well,
data should have materialized in the target table in CrateDB.
```shell
crash --hosts "${CRATEDB_HTTP_URL}" --command \
  'SELECT * FROM "demo-sink";'
```

:::{rubric} Shut down AWS stack
:::
In order to complete the experiment, you may want to shut down the AWS stack again.
```shell
aws cloudformation delete-stack --stack-name testdrive-dynamodb-dev
```


## Appendix

### Processor
Check status of Lambda function.
```shell
aws lambda get-function \
  --function-name arn:aws:lambda:eu-central-1:831394476016:function:testdrive-dynamodb-dev-lambda-processor
```
Check status of stream mapping(s).
```shell
aws lambda list-event-source-mappings
```
Check logs.
```shell
aws logs describe-log-groups
aws logs start-live-tail --log-group-identifiers arn:aws:logs:eu-central-1:831394476016:log-group:/aws/lambda/DynamoDBCrateDBProcessor
```

### Database

There are a few utility commands that help you operate the stack, that have not
been absorbed yet. See also [Monitoring and troubleshooting Lambda functions].

Query records in CrateDB table.
```shell
crash --hosts "${CRATEDB_HTTP_URL}" --command \
  'SELECT * FROM "demo-sink";'
```

Truncate CrateDB table.
```shell
crash --hosts "${CRATEDB_HTTP_URL}" --command \
  'DELETE FROM "demo-sink";'
```

Query documents in DynamoDB table.
```shell
aws dynamodb execute-statement --statement \
  "SELECT * FROM \"demo-source\";"
```


(ecr-repository-policy)=
### ECR Repository Policy
```json
{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "allow public pull",
      "Effect": "Allow",
      "Principal": "*",
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer"
      ]
    }
  ]
}
```

## Troubleshooting

### ECR Repository
If you receive such an error message, your session has expired, and you need
to re-run the authentication step.
```text
denied: Your authorization token has expired. Reauthenticate and try again.
```

This error message indicates your ECR repository does not exist. The solution
is to create it, using the command shared above.
```text
name unknown: The repository with name 'kinesis-cratedb-processor-lambda' does
not exist in the registry with id '831394476016'
```

### AWS CloudFormation
If you receive such an error, ...
```text
botocore.exceptions.ClientError: An error occurred (ValidationError) when calling
the CreateChangeSet operation: Stack:arn:aws:cloudformation:eu-central-1:931394475905:stack/testdrive-dynamodb-dev/ea8c32e0-492c-11ef-b9b3-06b708ecd03f
is in UPDATE_ROLLBACK_FAILED state and can not be updated.
```
because some detail when deploying or updating the CloudFormation recipe fails,
the CloudFormation stack is stuck, and you will need to [continue rolling back
an update] manually.
```shell
aws cloudformation continue-update-rollback --stack-name testdrive-dynamodb-dev
```



[AWS]: https://en.wikipedia.org/wiki/Amazon_Web_Services
[AWS CloudFormation]: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/Welcome.html 
[AWS Lambda]: https://en.wikipedia.org/wiki/AWS_Lambda
[continue rolling back an update]: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-continueupdaterollback.html
[dynamodb_kinesis_lambda_oci_cratedb.py]: https://github.com/crate/cratedb-toolkit/blob/main/examples/aws/dynamodb_kinesis_lambda_oci_cratedb.py
[example program]: https://github.com/crate/cratedb-toolkit/tree/main/examples/aws
[How CloudFormation works]: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cloudformation-overview.html
[Infrastructure as code (IaC)]: https://en.wikipedia.org/wiki/Infrastructure_as_code
[kinesis_lambda.py]: https://github.com/crate/cratedb-toolkit/blob/main/cratedb_toolkit/io/processor/kinesis_lambda.py
[Monitoring and troubleshooting Lambda functions]: https://docs.aws.amazon.com/lambda/latest/dg/lambda-monitoring.html 
[OCI]: https://en.wikipedia.org/wiki/Open_Container_Initiative
[repository policy]: https://docs.aws.amazon.com/lambda/latest/dg/images-create.html#gettingstarted-images-permissions
[set-repository-policy]: https://docs.aws.amazon.com/cli/latest/reference/ecr/set-repository-policy.html
