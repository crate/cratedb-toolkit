(deltalake)=
(deltalake-loader)=
# Delta Lake I/O

## About

Import and export data into/from [Delta Lake] tables ([paper]), for humans and machines.

## Synopsis

Load from Delta Lake table:
```shell
ctk load table {file,http,https,s3,abfs,gs,hdfs,lakefs}+deltalake://...
```

Export to Delta Lake table:
```shell
ctk save table {file,http,https,s3,abfs,gs,hdfs,lakefs}+deltalake://...
```

## Install

```shell
uv tool install --upgrade 'cratedb-toolkit[deltalake]'
```

:::{tip}
For speedy installations, we recommend using the [uv] package manager.
Install it using `brew install uv` on macOS or `pipx install uv` on
other operating systems.
:::

## Usage

Delta Lake supports different backends like S3, Azure Data Lake, and Google
Cloud, see [Loading a Delta Table].

Please look up available configuration parameters in the reference documentation
of the uniform API library `object_store` ([s3 options], [azure options], [gcs options]).
Otherwise, derive your ETL commands from the examples shared below.

### Load

Load from filesystem.
```shell
ctk load table \
    "file+deltalake://./var/lib/delta/demo/taxi-tiny" \
    --cluster-url="crate://crate:crate@localhost:4200/demo/taxi-tiny"
```

Load from HTTP.
```shell
ctk load table \
    "https+deltalake://datahub.example.org/delta/demo/taxi-tiny" \
    --cluster-url="crate://crate:crate@localhost:4200/demo/taxi-tiny"
```

Load from AWS S3.
```shell
ctk load table \
    "s3+deltalake://bucket1/demo/taxi-tiny?AWS_ACCESS_KEY_ID=<your_access_key_id>&AWS_SECRET_ACCESS_KEY=<your_secret_access_key>&AWS_ENDPOINT=<endpoint_url>&AWS_REGION=<s3-region>" \
    --cluster-url="crate://crate:crate@localhost:4200/demo/taxi-tiny"
```

Load from Azure Data Lake.
```shell
ctk load table "abfs+deltalake://container/path?AZURE_STORAGE_ACCOUNT_NAME=devstoreaccount1&AZURE_STORAGE_ACCOUNT_KEY=foo&..."
```

Load from Google Cloud.
```shell
ctk load table "gs+deltalake://bucket1/demo/taxi-tiny?GOOGLE_SERVICE_ACCOUNT=...&GOOGLE_SERVICE_ACCOUNT_KEY="
```

Load from HDFS.
```shell
ctk load table "hdfs+deltalake://localhost:9000"
```

Load from LakeFS.
```shell
ctk load table "lakefs+deltalake://bucket1/demo/taxi-tiny?endpoint=https://lakefs.example.org&access_key_id=LAKEFSID&secret_access_key=LAKEFSKEY"
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
    "file+deltalake://./var/lib/delta/demo/taxi-tiny"
```

Save to AWS S3.
```shell
ctk save table \
    --cluster-url="crate://crate:crate@localhost:4200/demo/taxi-tiny" \
    "s3+deltalake://bucket1/demo/taxi-tiny?AWS_ACCESS_KEY_ID=<your_access_key_id>&AWS_SECRET_ACCESS_KEY=<your_secret_access_key>&AWS_ENDPOINT=<endpoint_url>&AWS_REGION=<s3-region>"
```

For other target URLs, see "Load" section.

### Cloud

A canonical invocation for copying data from a Delta Lake table on AWS S3 to CrateDB Cloud.
Please note the `ssl=true` query parameter on the CrateDB cluster URL.

```shell
ctk load table \
    "s3+deltalake://bucket1/demo/taxi-tiny?AWS_ACCESS_KEY_ID=<your_access_key_id>&AWS_SECRET_ACCESS_KEY=<your_secret_access_key>&AWS_ENDPOINT=<endpoint_url>&AWS_REGION=<s3-region>" \
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
ctk load table "file+deltalake://./var/lib/delta/?batch-size=200000"
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

### Delta Lake parameters

#### `mode`

By default, the target Delta Lake table must not exist. If it exists, and you want
to overwrite it, use `mode=overwrite`. On the other hand, `mode=append` will append
to an existing table.

:::{rubric} Example usage
:::
```shell
ctk save table "file+deltalake://./var/lib/delta/?...&mode=append"
```


[azure options]: https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants
[Delta Lake]: https://delta.io/
[gcs options]: https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants
[paper]: https://www.databricks.com/wp-content/uploads/2020/08/p975-armbrust.pdf
[Loading a Delta Table]: https://delta-io.github.io/delta-rs/usage/loading-table/
[s3 options]: https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants
[uv]: https://docs.astral.sh/uv/
