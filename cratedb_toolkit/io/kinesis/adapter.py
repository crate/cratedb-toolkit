import asyncio
import typing as t

import boto3
from aiobotocore.session import AioSession
from kinesis import Consumer, JsonProcessor, Producer
from yarl import URL


class KinesisAdapter:
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
        self.region_name = kinesis_url.query.get("region")
        self.stream_name = self.kinesis_url.path.lstrip("/")
        self.kinesis_client = self.session.client("kinesis", endpoint_url=self.endpoint_url)

    def consumer_factory(self, **kwargs):
        return Consumer(
            stream_name=self.stream_name,
            session=self.async_session,
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            processor=JsonProcessor(),
            **kwargs,
        )

    def consume_forever(self, handler: t.Callable):
        asyncio.run(self._consume_forever(handler))

    def consume_once(self, handler: t.Callable):
        asyncio.run(self._consume_once(handler))

    async def _consume_forever(self, handler: t.Callable):
        """
        Consume items from a Kinesis stream.
        """
        async with self.consumer_factory(
            # TODO: Make configurable.
            create_stream=True,
            iterator_type="TRIM_HORIZON",
            sleep_time_no_records=0.2,
        ) as consumer:
            while True:
                async for item in consumer:
                    handler(item)

    async def _consume_once(self, handler: t.Callable):
        async with self.consumer_factory(
            # TODO: Make configurable.
            create_stream=True,
            iterator_type="TRIM_HORIZON",
            sleep_time_no_records=0.2,
        ) as consumer:
            async for item in consumer:
                handler(item)

    def produce(self, data: t.Dict[str, t.Any]):
        asyncio.run(self._produce(data))

    async def _produce(self, data: t.Dict[str, t.Any]):
        # Put item onto queue to be flushed via `put_records()`.
        async with Producer(
            stream_name=self.stream_name,
            session=self.async_session,
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            # TODO: Make configurable.
            create_stream=True,
            buffer_time=0.01,
        ) as producer:
            await producer.put(data)
