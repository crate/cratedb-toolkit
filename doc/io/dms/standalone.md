# AWS DMS Standalone

## About
Relay an [AWS DMS] data stream from Amazon Kinesis into a [CrateDB] table using
a one-stop command `ctk load table kinesis+dms:///...`.

You can use it to facilitate convenient data transfers to be used within data
pipelines or ad hoc operations. It can be used as a CLI interface and as a library.

## Install
Install the CrateDB Toolkit package including the Kinesis extensions.
```shell
pip install --upgrade 'cratedb-toolkit[kinesis]'
```

## Usage
1. Set up a DMS instance, replicating data to Amazon Kinesis.
2. Define CrateDB destination address.
```shell
export CRATEDB_CLUSTER_URL=crate://crate@localhost:4200/testdrive
```
3.a. Transfer data from Kinesis data stream into CrateDB database table.
```shell
ctk load table kinesis+dms:///arn:aws:kinesis:eu-central-1:831394476016:stream/testdrive
```
3.b. Transfer data from stream dump file into CrateDB database table.
```shell
ctk load table kinesis+dms:///path/to/dms-over-kinesis.jsonl
```


[AWS DMS]: https://aws.amazon.com/dms/
[CrateDB]: https://cratedb.com/docs/guide/home/
