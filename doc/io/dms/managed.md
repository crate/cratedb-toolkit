# AWS DMS Managed

## About
Conduct a data migration from any source supported by AWS DMS into a database
table on [CrateDB Cloud], exclusively using managed infrastructure components.

:::{note}
This is a work in progress. Please contact our data engineers to get started.
:::

## Configuration
1. Set up a DMS instance to replicate data to an Amazon Kinesis Data Stream.
2. Take a note about the AWS ARN of that Kinesis Data Stream,
   for example `arn:aws:kinesis:eu-central-1:831394476016:stream/testdrive`.
3. Reach out to CrateDB support, to make CrateDB Cloud connect to your data
   stream, in order to converge it into your CrateDB Cloud instance.


[to an Amazon Kinesis Data Stream]: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Kinesis.html
[CrateDB]: https://cratedb.com/docs/guide/home/
[CrateDB Cloud]: https://cratedb.com/docs/cloud/
