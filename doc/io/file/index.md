(io-file)=

# Files

:::{div} sd-text-muted
Import and export data into/from files on filesystem and cloud storage.
:::

:::{include} ../_install-ingest.md
:::

## Cloud storage

### Amazon S3
```shell
ctk load \
    "s3://?access_key_id=${AWS_ACCESS_KEY_ID}&secret_access_key=${AWS_SECRET_ACCESS_KEY}&table=openaq-fetches/realtime/2023-02-25/1677351953_eea_2aa299a7-b688-4200-864a-8df7bac3af5b.ndjson#jsonl" \
    "crate://crate:na@localhost:4200/testdrive/s3_ndjson"
```
See documentation about [ingestr and Amazon S3] about details of the URI format,
file globbing patterns, compression options, and file type hinting options.
:::{div}
:style: font-size: small

Source URL template: `s3://?access_key_id=<aws-access-key-id>&secret_access_key=<aws-secret-access-key>&table=<bucket-name>/<file-glob>`
:::

### Google Cloud Storage (GCS)
```shell
ctk load \
    "gs://?credentials_path=/path/to/service-account.json&table=<bucket-name>/<file-glob>" \
    "crate://crate:na@localhost:4200/testdrive/gcs"
```


## File formats

### CSV
```shell
ctk load \
    "csv://./examples/cdc/postgresql/diamonds.csv?table=sample" \
    "crate://crate:na@localhost:4200/testdrive/csv_diamonds"
```


[ingestr and Amazon S3]: https://bruin-data.github.io/ingestr/supported-sources/s3.html
