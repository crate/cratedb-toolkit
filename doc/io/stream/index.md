# Streams

:::{div} sd-text-muted
Import and export data into/from event streams and message brokers.
:::

Streaming data today is emitted at high volume in a continuous,
incremental manner with the goal of low-latency processing.

Organizations have thousands of data sources that typically simultaneously
emit messages, records, or data ranging in size from a few bytes to several
megabytes (MB). Streaming data includes location, event, and sensor data
that companies use for real-time analytics and visibility into many aspects
of their business.

(io-streams)=
## Integrations

CrateDB provides data pipeline integration adapters for popular
streaming services and brokers.

:::::{grid} 3
:gutter: 2
:padding: 0

::::{grid-item-card}
:link: kinesis
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/kinesis.svg
:height: 80px
```
+++
Amazon Kinesis
::::

:::::

## Synopsis

Load data from Amazon Kinesis stream `testdrive` into CrateDB table `testdrive.kinesis`.
```shell
uvx 'cratedb-toolkit[io-ingest]' load table \
    "kinesis+ingest:?aws_access_key_id=${AWS_ACCESS_KEY_ID}&aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}&region_name=eu-central-1&table=arn:aws:kinesis:eu-central-1:831394476016:stream/testdrive" \
    --cluster-url="crate://crate:crate@localhost:4200/testdrive/kinesis"
```

Load data from Apache Kafka into CrateDB.
```shell
ctk load table \
    "kafka://?bootstrap_servers=localhost:9092&group_id=test_group&security_protocol=SASL_SSL&sasl_mechanisms=PLAIN&sasl_username=example_username&sasl_password=example_secret&batch_size=1000&batch_timeout=3&table=my-topic" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/kafka"
```


```{toctree}
:maxdepth: 1
:hidden:

kinesis
```
