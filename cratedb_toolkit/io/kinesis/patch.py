"""
Accept Kinesis StreamARN addressing.

Monkey-patch variant of a patch to async-kinesis.
https://github.com/hampsterx/async-kinesis/pull/39
"""

import kinesis.base
from kinesis import exceptions
from kinesis.base import ClientError, log


class Dummy(kinesis.base.Base):
    @property
    def address(self):
        """
        Return address of stream, either as StreamName or StreamARN, when applicable.
        https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescription.html#Streams-Type-StreamDescription-StreamName
        https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescription.html#Streams-Type-StreamDescription-StreamARN
        """
        if self.stream_name.startswith("arn:"):
            return {"StreamARN": self.stream_name}
        return {"StreamName": self.stream_name}


async def get_stream_description(self):
    try:
        return (await self.client.describe_stream(**self.address))["StreamDescription"]
    except ClientError as err:
        code = err.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            raise exceptions.StreamDoesNotExist("Stream '{}' does not exist".format(self.stream_name)) from None
        raise


async def _create_stream(self, ignore_exists=True):
    log.debug("Creating (or ignoring) stream {} with {} shards".format(self.stream_name, self.create_stream_shards))

    if self.create_stream_shards < 1:
        raise Exception("Min shard count is one")

    try:
        await self.client.create_stream(**self.address, ShardCount=self.create_stream_shards)
    except ClientError as err:
        code = err.response["Error"]["Code"]

        if code == "ResourceInUseException":
            if not ignore_exists:
                raise exceptions.StreamExists("Stream '{}' exists, cannot create it".format(self.stream_name)) from None
        elif code == "LimitExceededException":
            raise exceptions.StreamShardLimit("Stream '{}' exceeded shard limit".format(self.stream_name)) from err
        else:
            raise


async def get_shard_iterator(self, shard_id, last_sequence_number=None):
    log.debug(
        "getting shard iterator for {} @ {}".format(
            shard_id,
            last_sequence_number if last_sequence_number else self.iterator_type,
        )
    )

    params = {
        "ShardId": shard_id,
        "ShardIteratorType": "AFTER_SEQUENCE_NUMBER" if last_sequence_number else self.iterator_type,
    }
    params.update(self.address)

    if last_sequence_number:
        params["StartingSequenceNumber"] = last_sequence_number

    if self.iterator_type == "AT_TIMESTAMP" and self.timestamp:
        params["Timestamp"] = self.timestamp

    response = await self.client.get_shard_iterator(**params)
    return response["ShardIterator"]


def patch_async_kinesis():
    kinesis.base.Base.address = Dummy.address
    kinesis.base.Base.get_stream_description = get_stream_description
    kinesis.base.Base._create_stream = _create_stream
    kinesis.consumer.Consumer.get_shard_iterator = get_shard_iterator
