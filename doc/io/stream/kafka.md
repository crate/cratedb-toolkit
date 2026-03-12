(kafka)=

# Apache Kafka

:::{div} sd-text-muted
Load data from Apache Kafka into CrateDB.
:::

[Apache Kafka] is an open-source distributed event streaming platform used
by thousands of companies for high-performance data pipelines, streaming
analytics, data integration, and mission-critical applications.
More than 80% of all Fortune 100 companies trust, and use Apache Kafka.

Today, managed Kafka and Kafka-compatible streaming services are available
through products like Amazon MSK, Confluent Cloud, Redpanda Streaming,
and others.

## Prerequisites

Use Docker or Podman to run all components. This approach works consistently
across Linux, macOS, and Windows.

## Install

Install the most recent Python package [cratedb-toolkit],
or evaluate {ref}`alternative installation methods <install>`.
```shell
uv tool install --upgrade 'cratedb-toolkit[io-ingest]'
```

Install [kcat], an Apache Kafka producer and consumer tool.
```shell
{apt,brew} install kcat
```

## Tutorial

5-minute step-by-step instructions about how
to work with Apache Kafka and CrateDB.

### Services

Run Kafka and CrateDB using Docker or Podman.
```shell
docker run --rm --name=kafka \
  --publish=9092:9092 docker.io/apache/kafka:4.1.1
```
```shell
docker run --rm --name=cratedb \
  --publish=4200:4200 --publish=5432:5432 --env=CRATE_HEAP_SIZE=2g \
  docker.io/crate:latest '-Cdiscovery.type=single-node'
```

### Populate data

Publish two events to a Kafka topic using `kcat`.
```sh
echo '{"sensor_id":1,"ts":"2025-06-01 10:00","temperature":42.42,"humidity":84.84}' | \
  kcat -P -b localhost -t demo

echo '{"sensor_id":2,"ts":"2025-06-01 11:00","temperature":45.21,"humidity":80.82}' | \
  kcat -P -b localhost -t demo
```
Verify events are present by subscribing to the Kafka topic.
```sh
kcat -C -e -b localhost -t demo
```

### Load data

Use {ref}`index` to load data from Kafka topic into CrateDB table.
```shell
ctk load table \
    "kafka:?bootstrap_servers=localhost:9092&group_id=test&table=demo" \
    --cluster-url="crate://crate:crate@localhost:4200/testdrive/kafka"
```

### Query data

Inspect database using [crash](https://pypi.org/project/crash/).
```shell
crash -c "SELECT count(*) FROM testdrive.kafka"
```
```shell
crash -c "SELECT * FROM testdrive.kafka"
```
```shell
crash -c "SHOW CREATE TABLE testdrive.kafka"
```

## Documentation

The Kafka topic name can be provided by using the `&table=` query parameter.

The Kafka adapter provides a few more connectivity options outlined below. 
A fully qualified Kafka URL template looks like this.
```shell
kafka://?bootstrap_servers=localhost:9092&group_id=test&security_protocol=SASL_SSL&sasl_mechanisms=PLAIN&sasl_username=example_username&sasl_password=example_secret&batch_size=1000&batch_timeout=3
```

:::{rubric} Kafka URL parameters
:::

- `bootstrap_servers`: Kafka broker(s).
- `group_id`: Kafka consumer group ID.
  It identifies the consumer group that reads messages from a Kafka
  topic. Because it is used to manage consumer offsets and assign partitions
  to consumers, it is the key to reading messages from the correct partition
  and position in the topic.
- `table`: Kafka topic name.
- `security_protocol`: Communication protocol, e.g. `SASL_SSL` for SSL.
- `sasl_mechanisms`: SASL mechanism for authentication, e.g. `PLAIN`.
- `sasl_username`: Username for SASL authentication.
- `sasl_password`: Password for SASL authentication.
- `batch_size`: Number of messages to fetch in a single batch (default: 3000).
- `batch_timeout`: Maximum time to wait for messages (default: 3 seconds).

:::{include} ../_cratedb-options.md
:::

## Big data example

Use one of our example datasets that includes 100_000 records. If you populate it
to the stream five times, you have a reasonable baseline for transferring larger
datasets.
```shell
NDJSON=https://cdn.crate.io/downloads/datasets/cratedb-datasets/cloud-tutorials/devices_readings.json.gz
curl --silent ${NDJSON} | gunzip | kcat -P -b localhost -t devices_readings
```
```shell
ctk load table \
    "kafka:?bootstrap_servers=localhost:9092&group_id=test&table=devices_readings" \
    --cluster-url="crate://crate:crate@localhost:4200/testdrive/devices_readings"
```
```shell
crash -c "SELECT count(*) FROM testdrive.devices_readings"
```

## See also

Use [kafka-compose.yml] and [kafka-demo.xsh] for an end-to-end
Kafka+CrateDB-in-a-box example rig using {Docker,Podman} Compose.

:::{include} /_snippet/ingest-see-also.md
:::


[Apache Kafka]: https://kafka.apache.org/
[cratedb-toolkit]: https://pypi.org/project/cratedb-toolkit/
[kafka-compose.yml]: https://github.com/crate/cratedb-examples/blob/main/application/ingestr/kafka-compose.yml
[kafka-demo.xsh]: https://github.com/crate/cratedb-examples/blob/main/application/ingestr/kafka-demo.xsh
[kcat]: https://github.com/edenhill/kcat
