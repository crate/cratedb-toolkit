(iceberg)=
(iceberg-loader)=
# Apache Iceberg I/O

## About

Import and export data into/from [Apache Iceberg] tables, for humans and machines.

## Synopsis

Load from Iceberg table:
```shell
ctk load table {file,s3,abfs,gs,hdfs}+iceberg://...
```

Export to Iceberg table:
```shell
ctk save table {file,s3,abfs,gs,hdfs}+iceberg://...
```

## Install

```shell
uv tool install --upgrade 'cratedb-toolkit[iceberg]'
```

:::{tip}
For speedy installations, we recommend using the [uv] package manager.
Install it using `brew install uv` on macOS or `pipx install uv` on
other operating systems.
:::

## Usage

Iceberg works with the concept of a [FileIO] which is a pluggable module for
reading, writing, and deleting files. It supports different backends like
S3, HDFS, Azure Data Lake, Google Cloud Storage, Alibaba Cloud Object Storage,
and Hugging Face.

Please look up available configuration parameters in the reference documentation,
otherwise derive your ETL commands from the examples shared below.

### Load

Load from metadata file on filesystem.
```shell
ctk load table \
    "file+iceberg://./var/lib/iceberg/demo/taxi-tiny/metadata/00003-dd9223cb-6d11-474b-8d09-3182d45862f4.metadata.json" \
    --cluster-url="crate://crate:crate@localhost:4200/demo/taxi-tiny"
```

Load from metadata file on AWS S3.
```shell
ctk load table \
    "s3+iceberg://bucket1/demo/taxi-tiny/metadata/00003-dd9223cb-6d11-474b-8d09-3182d45862f4.metadata.json?s3.access-key-id=<your_access_key_id>&s3.secret-access-key=<your_secret_access_key>&s3.endpoint=<endpoint_url>&s3.region=<s3-region>" \
    --cluster-url="crate://crate:crate@localhost:4200/demo/taxi-tiny"
```

Use REST catalog and AWS S3 storage.
```shell
ctk load table \
    "s3+iceberg://bucket1?catalog-uri=http://iceberg-catalog.example.org:5000&catalog-token=foo&catalog=default&namespace=demo&table=taxi-tiny&s3.access-key-id=<your_access_key_id>&s3.secret-access-key=<your_secret_access_key>&s3.endpoint=<endpoint_url>&s3.region=<s3-region>" \
    --cluster-url="crate://crate:crate@localhost:4200/demo/taxi-tiny"
```

Use catalog in Apache Hive.
```shell
ctk load table "s3+iceberg://bucket1?catalog-uri=thrift://localhost:9083/&catalog-credential=t-1234:secret&..."
```

Use catalog in AWS Glue.
```shell
ctk load table "s3+iceberg://bucket1?catalog-type=glue&glue.id=foo&glue.profile-name=bar&glue.region=region&glue.access-key-id=key&glue.secret-access-key=secret&..."
```

Use catalog in Google BigQuery.
```shell
ctk load table "s3+iceberg://bucket1?catalog-type=bigquery&gcp.bigquery.project-id=foo&..."
```

Use catalog in DynamoDB.
```shell
ctk load table "s3+iceberg://bucket1?catalog-type=dynamodb&dynamodb.profile-name=foo&dynamodb.region=bar&dynamodb.access-key-id=key&dynamodb.secret-access-key=secret&..."
```

Load data from Azure Data Lake Storage.
```shell
ctk load table "abfs+iceberg://container/path?adls.account-name=devstoreaccount1&adls.account-key=foo&..."
```

Load data from Google Cloud Storage.
```shell
ctk load table "gs+iceberg://bucket1/demo/taxi-tiny?gcs.project-id=..."
```

Load data from HDFS Storage.
```shell
ctk load table "hdfs+iceberg://path?hdfs.host=10.0.19.25&hdfs.port=9000&hdfs.user=<user>&hdfs.kerberos_ticket=<ticket>"
```

:::{tip}
After loading your data into CrateDB, query it.
```shell
ctk shell --command 'SELECT * FROM "demo"."taxi-tiny";'
ctk show table '"demo"."taxi-tiny"'
```
:::

### Save

Save to filesystem.
```shell
ctk save table \
    --cluster-url="crate://crate:crate@localhost:4200/demo/taxi-tiny" \
    "file+iceberg://./var/lib/iceberg/?catalog=default&namespace=demo&table=taxi-tiny"
```

Save to AWS S3.
```shell
ctk save table \
    --cluster-url="crate://crate:crate@localhost:4200/demo/taxi-tiny" \
    "s3+iceberg://bucket1?catalog=default&namespace=demo&table=taxi-tiny&s3.access-key-id=<your_access_key_id>&s3.secret-access-key=<your_secret_access_key>&s3.endpoint=<endpoint_url>&s3.region=<s3-region>"
```

For other target URLs, see "Load" section.

### Cloud

A canonical invocation for copying data from an Iceberg table on AWS S3 to CrateDB Cloud.
Please note the `ssl=true` query parameter on the CrateDB cluster URL.

```shell
ctk load table \
    "s3+iceberg://bucket1?catalog-uri=https://iceberg-catalog:5000&catalog-token=foo&catalog=default&namespace=demo&table=taxi-tiny&s3.access-key-id=<your_access_key_id>&s3.secret-access-key=<your_secret_access_key>&s3.endpoint=<endpoint_url>&s3.region=<s3-region>" \
    --cluster-url="crate://admin:dZ...6LqB@green-shaak-ti.eks1.eu-west-1.aws.cratedb.net:4200/testdrive/demo?ssl=true"
```

## Options

### Generic parameters

#### `batch-size`

The source URL accepts the `batch-size` option to configure pagination.
You will need to find an optimal value based on the shape of your data.
The default value is `75000`.

:::{rubric} Example usage
:::
```shell
ctk load table "file+iceberg://./var/lib/iceberg/?batch-size=200000"
```
```shell
ctk save table --cluster-url="crate://?batch-size=200000"
```

### CrateDB parameters

#### `if-exists`

The target CrateDB table will be created automatically, if it does not exist. If it
does exist, the `if-exists` URL query parameter can be used to configure this
behavior. The default value is `fail`, the possible values are:

* `fail`: Raise a ValueError. (default)
* `replace`: Drop the table before inserting new values.
* `append`: Insert new values to the existing table.

:::{rubric} Example usage
:::
In order to always replace the target table, i.e. to drop and re-create it
prior to inserting data, use `?if-exists=replace`.
```shell
export CRATEDB_CLUSTER_URL="crate://crate:crate@localhost:4200/testdrive/demo?if-exists=replace"
ctk load table ...
```

### Iceberg parameters

#### `append`

By default, the target Iceberg table will be overwritten. If you set `append`
to a truthy value, save operations will append to an existing table.

:::{rubric} Example usage
:::
```shell
ctk save table "file+iceberg://./var/lib/iceberg/?...&append=true"
```

#### PyIceberg

The PyIceberg I/O adapters accept a plethora of options that can be used 1:1.
For a list of all available options, please consult the [FileIO] documentation.
For I/O adapters not part of the documentation yet, please consult the source
code about [catalog options] and [storage options].


[Apache Iceberg]: https://iceberg.apache.org/
[catalog options]: https://github.com/apache/iceberg-python/tree/main/pyiceberg/catalog
[FileIO]: https://py.iceberg.apache.org/configuration/#fileio
[storage options]: https://github.com/apache/iceberg-python/tree/main/pyiceberg/io
[uv]: https://docs.astral.sh/uv/
