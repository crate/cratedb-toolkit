(iceberg)=
(iceberg-loader)=
# Apache Iceberg I/O

## About

Import and export data into/from Iceberg tables, for humans and machines.

## Synopsis

- Load from Iceberg table: `ctk load table file+iceberg://...`,
  `ctk load table s3+iceberg://...`

- Export to Iceberg table: `ctk save table file+iceberg://...`

## Install

```shell
pip install --upgrade 'cratedb-toolkit[io]'
```

## Usage

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

Load from REST catalog on AWS S3.
```shell
ctk load table \
    "s3+iceberg://bucket1/?catalog-uri=http://iceberg-catalog.example.org:5000&catalog-token=foo&catalog=default&namespace=demo&table=taxi-tiny&s3.access-key-id=<your_access_key_id>&s3.secret-access-key=<your_secret_access_key>&s3.endpoint=<endpoint_url>&s3.region=<s3-region>" \
    --cluster-url="crate://crate:crate@localhost:4200/demo/taxi-tiny"
```

Query data in CrateDB.
```shell
ctk shell --command 'SELECT * FROM demo."taxi-tiny";'
ctk show table 'demo."taxi-tiny"'
```

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
    "s3+iceberg://bucket1/?catalog=default&namespace=demo&table=taxi-tiny&s3.access-key-id=<your_access_key_id>&s3.secret-access-key=<your_secret_access_key>&s3.endpoint=<endpoint_url>&s3.region=<s3-region>"
```

### Cloud

A canonical invocation for copying data from an Iceberg table on AWS S3 to CrateDB Cloud.
Please note the `ssl=true` query parameter on the CrateDB cluster URL.

```shell
ctk load table \
    "s3+iceberg://bucket1/?catalog-uri=https://iceberg-catalog:5000&catalog-token=foo&catalog=default&namespace=demo&table=taxi-tiny&s3.access-key-id=<your_access_key_id>&s3.secret-access-key=<your_secret_access_key>&s3.endpoint=<endpoint_url>&s3.region=<s3-region>" \
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

Both parameters apply to target pipeline elements, controlling overwrite behaviour.

#### `if-exists`

The target table will be created automatically, if it does not exist. If it
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
ctk save table "file+iceberg://./var/lib/iceberg/?catalog=default&namespace=demo&table=taxi-tiny&append=true"
```
