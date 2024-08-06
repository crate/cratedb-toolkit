# AWS DMS Standalone

## About
Relay an AWS DMS data stream from Amazon Kinesis into a [CrateDB] table using
a one-stop command `ctk load table kinesis+dms://...`.

You can use it in order to facilitate convenient data transfers to be used
within data pipelines or ad hoc operations. It can be used as a CLI interface,
and as a library.

## Install
Install the CrateDB Toolkit package.
```shell
pip install --upgrade 'cratedb-toolkit[kinesis]'
```

## Usage
1. Set up a DMS instance, replicating data to Amazon Kinesis.
2. Transfer data from Kinesis Data Stream into CrateDB database table.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk load table kinesis+dms://arn:aws:kinesis:eu-central-1:831394476016:stream/testdrive
```


[CrateDB]: https://cratedb.com/docs/guide/home/
