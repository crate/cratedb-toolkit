# Streams

:::{div} sd-text-muted
Import and export data into/from event streams and message brokers.
:::

:::{include} ../_install-ingest.md
:::

## Integrations

Load data from Amazon Kinesis into CrateDB.
```shell
ctk load table \
    "kinesis://?aws_access_key_id=${AWS_ACCESS_KEY_ID}&aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}&region_name=eu-central-1&table=arn:aws:kinesis:eu-central-1:831394476016:stream/testdrive" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/kinesis_demo"
```
:::{tip}
Source URL template: `kinesis://?aws_access_key_id=<aws-access-key-id>&aws_secret_access_key=<aws-secret-access-key>&region_name=<region-name>&table=arn:aws:kinesis:<region-name>:<aws-account-id>:stream/<stream_name>`
:::

Load data from Apache Kafka into CrateDB.
```shell
ctk load table \
    "kafka://?bootstrap_servers=localhost:9092&group_id=test_group&security_protocol=SASL_SSL&sasl_mechanisms=PLAIN&sasl_username=example_username&sasl_password=example_secret&batch_size=1000&batch_timeout=3&table=my-topic" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/kafka_demo"
```
