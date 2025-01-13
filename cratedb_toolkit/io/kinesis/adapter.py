import asyncio
import typing as t

import boto3
from aiobotocore.session import AioSession
from kinesis import Consumer, JsonProcessor, Producer
from yarl import URL

from cratedb_toolkit.util.data import asbool


class KinesisAdapter:
    # Configuration for Kinesis shard iterators.
    # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html
    # Map `start` option to `ShardIteratorType`.
    start_iterator_type_map = {
        "earliest": "TRIM_HORIZON",
        "latest": "LATEST",
        "seqno-at": "AT_SEQUENCE_NUMBER",
        "seqno-after": "AFTER_SEQUENCE_NUMBER",
    }

    def __init__(self, kinesis_url: URL):
        self.async_session = AioSession()
        self.async_session.set_credentials(access_key=kinesis_url.user, secret_key=kinesis_url.password)

        self.session = boto3.Session(
            aws_access_key_id=kinesis_url.user,
            aws_secret_access_key=kinesis_url.password,
            region_name=kinesis_url.query.get("region"),
        )

        self.endpoint_url = None
        if kinesis_url.host and kinesis_url.host.lower() != "aws":
            self.endpoint_url = f"http://{kinesis_url.host}:{kinesis_url.port}"

        self.kinesis_url = kinesis_url
        self.stream_name = self.kinesis_url.path.lstrip("/")

        self.region_name: str = self.kinesis_url.query.get("region", "us-east-1")
        self.batch_size: int = int(self.kinesis_url.query.get("batch-size", 100))
        self.create: bool = asbool(self.kinesis_url.query.get("create", "false"))
        self.create_shards: int = int(self.kinesis_url.query.get("create-shards", 1))
        self.start: str = self.kinesis_url.query.get("start", "earliest")
        self.seqno: int = int(self.kinesis_url.query.get("seqno", 0))
        self.idle_sleep: float = float(self.kinesis_url.query.get("idle-sleep", 0.5))
        self.buffer_time: float = float(self.kinesis_url.query.get("buffer-time", 0.5))

        self.kinesis_client = self.session.client("kinesis", endpoint_url=self.endpoint_url)
        self.stopping: bool = False

    @property
    def iterator_type(self):
        """
        Map `start` option to Kinesis' `ShardIteratorType`.
        """
        if self.start.startswith("seqno"):
            raise NotImplementedError(
                "Consuming Kinesis Stream from sequence number not implemented yet, please file an issue."
            )
        try:
            return self.start_iterator_type_map[self.start]
        except KeyError as ex:
            raise KeyError(f"Value for 'start' option unknown: {self.start}") from ex

    def consumer_factory(self, **kwargs):
        return Consumer(
            stream_name=self.stream_name,
            session=self.async_session,
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            max_queue_size=self.batch_size,
            sleep_time_no_records=self.idle_sleep,
            iterator_type=self.iterator_type,
            processor=JsonProcessor(),
            create_stream=self.create,
            create_stream_shards=self.create_shards,
            **kwargs,
        )

    def consume_forever(self, handler: t.Callable):
        asyncio.run(self._consume_forever(handler))

    def consume_once(self, handler: t.Callable):
        asyncio.run(self._consume_once(handler))

    def stop(self):
        self.stopping = True

    async def _consume_forever(self, handler: t.Callable):
        """
        Consume items from a Kinesis stream, forever.
        """
        async with self.consumer_factory() as consumer:
            while True:
                async for item in consumer:
                    handler(item)
                if self.stopping:
                    self.stopping = False
                    break

    async def _consume_once(self, handler: t.Callable):
        """
        Consume items from a Kinesis stream, one-shot.
        """
        async with self.consumer_factory() as consumer:
            async for item in consumer:
                handler(item)

    def produce(self, data: t.Dict[str, t.Any]):
        """
        Produce an item to a Kinesis stream.
        """
        asyncio.run(self._produce(data))

    async def _produce(self, data: t.Dict[str, t.Any]):
        """
        Put item onto queue to be flushed via `put_records()`.
        """
        async with Producer(
            stream_name=self.stream_name,
            session=self.async_session,
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            buffer_time=self.buffer_time,
            create_stream=self.create,
            create_stream_shards=self.create_shards,
        ) as producer:
            await producer.put(data)
