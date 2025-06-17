# ruff: noqa: E402
import logging
import time
import typing

import pytest

pytest.importorskip("boto3", reason="Skipping Kinesis tests because 'boto3' package is not installed")
pytest.importorskip("kinesis", reason="Only works with async-kinesis installed")

import botocore
from yarl import URL

from cratedb_toolkit.io.kinesis.adapter import KinesisStreamAdapter
from tests.io.kinesis.manager import KinesisTestManager

logger = logging.getLogger(__name__)


# Define streams to be deleted before running each test case.
RESET_STREAMS = [
    "testdrive",
]


class KinesisFixture:
    """
    A little helper wrapping Testcontainer's `LocalStackContainer`.

    TODO: Generalize into `LocalStackFixture`, see also `tests.io.dynamodb.conftest.DynamoDBFixture`.
    """

    def __init__(self):
        self.container = None
        self.url = None
        self.kinesis_adapter: typing.Union[KinesisStreamAdapter, None] = None
        self.setup()

    def setup(self):
        # TODO: Make image name configurable.
        from cratedb_toolkit.testing.testcontainers.localstack import LocalStackContainerWithKeepalive

        self.container = LocalStackContainerWithKeepalive()
        self.container.with_services("kinesis")
        self.container.start()

        self.kinesis_adapter = KinesisStreamAdapter(URL(f"{self.get_connection_url_kinesis()}?region=us-east-1"))

    def finalize(self):
        self.container.stop()

    def reset(self):
        """
        Reset all resources to provide each test case with a fresh canvas.
        """
        self.reset_streams()

    def reset_streams(self):
        """
        Drop all Kinesis streams used for testing.
        """
        kinesis_client = self.kinesis_adapter.kinesis_client
        for stream_name in RESET_STREAMS:
            try:
                kinesis_client.delete_stream(StreamName=stream_name)
            except botocore.exceptions.ClientError as error:
                if error.response["Error"]["Code"] != "ResourceNotFoundException":
                    raise
            waiter = kinesis_client.get_waiter("stream_not_exists")
            waiter.wait(StreamName=stream_name, WaiterConfig={"Delay": 0.3, "MaxAttempts": 15})
            time.sleep(0.25)

    def get_connection_url_kinesis(self):
        url = URL(self.container.get_url())
        return f"kinesis+dms://LSIAQAAAAAAVNCBMPNSG:dummy@{url.host}:{url.port}/testdrive"


@pytest.fixture(scope="session")
def kinesis_service():
    """
    Provide a Kinesis service instance to the test suite.
    """
    stack = KinesisFixture()
    stack.reset()
    yield stack
    stack.finalize()


@pytest.fixture(scope="function")
def kinesis(kinesis_service):
    """
    Provide a fresh canvas to each test case invocation, by resetting database content.
    """
    yield kinesis_service


@pytest.fixture(scope="session")
def kinesis_test_manager(kinesis_service):
    return KinesisTestManager(kinesis_service.get_connection_url_kinesis())
