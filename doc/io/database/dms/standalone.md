# AWS DMS Standalone

## About

Relay an [AWS DMS] data stream from Amazon Kinesis into [CrateDB] using
a one-stop command `ctk load table kinesis+dms:///...`.

Use it for ad-hoc transfers or in data pipelines, either from the command-line
(CLI) or as a library.

## Install

Install the CrateDB Toolkit package, including the Kinesis extension.
```shell
uv tool install --upgrade 'cratedb-toolkit[kinesis]'
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

## Configuration

You can configure the processor element by using a recipe file, to be conveyed through
the `--transformation` CLI option, like so:
```shell
ctk load table \
  "kinesis+dms:///arn:aws:kinesis:eu-central-1:831394476016:stream/testdrive" \
  --transformation=dms-load-schema-universal.yaml
```

The recipe file can be used to define settings, primary key information, the column
mapping strategy, and column type mapping rules.

If you want to provide the SQL DDL manually, please toggle `settings.ignore_ddl: true`,
and provide the primary key information in the `pk` section instead.

When the data includes container types, for example originating from JSON(B) columns,
use the `map` section to assign column type information to them.

If your columns include names with leading underscores, you may need to resort
to the `settings.mapping_strategy: universal` setting.
```yaml
# Recipe file for digesting DMS events.
# https://cratedb-toolkit.readthedocs.io/io/dms/standalone.html
---
meta:
  type: dms-recipe
  version: 1
collections:
- address:
    container: public
    name: foobar
  settings:
    mapping_strategy: direct
    ignore_ddl: true
  pk:
    rules:
    - pointer: /id
      type: bigint
  map:
    rules:
    - pointer: /resource
      type: object
```


[AWS DMS]: https://aws.amazon.com/dms/
[CrateDB]: https://cratedb.com/docs/guide/home/
