# AWS DMS Standalone

## About

Relay an [AWS DMS] data stream from Amazon Kinesis into [CrateDB] using
a one-stop command `ctk load table kinesis+dms:///...`.

Use it for ad-hoc transfers or in data pipelines, either from the command-line
(CLI) or as a library.

## Install

Install the CrateDB Toolkit package, including the Kinesis extension.
```shell
pip install --upgrade 'cratedb-toolkit[kinesis]'
```

## Usage

1. Set up a DMS instance, replicating data to Amazon Kinesis.
2. Define CrateDB destination address, using CrateDB Cloud.
   ```shell
   export CRATEDB_CLUSTER_URL="crate://admin:dZ...6LqB@example.eks1.eu-west-1.aws.cratedb.net:4200/testdrive?ssl=true"
   ```
   Alternatively, use CrateDB on localhost for evaluation purposes, for example by using
   Docker or Podman like `docker run --rm -it --name=cratedb --publish=4200:4200 --publish=5432:5432 --env=CRATE_HEAP_SIZE=2g crate/crate:nightly -Cdiscovery.type=single-node`.
   ```shell
   export CRATEDB_CLUSTER_URL="crate://crate@localhost:4200/testdrive"
   ```
3. Transfer data from Kinesis data stream into CrateDB.
   ```shell
   ctk load table "kinesis+dms:///arn:aws:kinesis:eu-central-1:831394476016:stream/testdrive"
   ```
   Alternatively, transfer data from a stream-dump file.
   ```shell
   ctk load table "kinesis+dms:///path/to/dms-over-kinesis.jsonl"
   ```

:::{todo}
The usage section is a bit thin on how to authenticate with AWS.
In general, if your `~/.aws/config` and `~/.aws/credentials` file are populated correctly
because you are using awscli or boto3 successfully, you should be ready to go.
:::



[AWS DMS]: https://aws.amazon.com/dms/
[CrateDB]: https://cratedb.com/docs/guide/home/
