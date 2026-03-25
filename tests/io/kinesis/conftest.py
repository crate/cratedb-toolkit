# ruff: noqa: E402
import logging
import typing
import uuid

import pytest

pytest.importorskip("boto3", reason="Skipping Kinesis tests because 'boto3' package is not installed")
pytest.importorskip("kinesis", reason="Only works with async-kinesis installed")

from yarl import URL

from cratedb_toolkit.io.kinesis.adapter import KinesisStreamAdapter
from tests.io.kinesis.manager import KinesisTestManager

logger = logging.getLogger(__name__)


class KinesisFixture:
    """
    A little helper wrapping Testcontainer's `LocalStackContainer`.

    TODO: Generalize into `LocalStackFixture`, see also `tests.io.dynamodb.conftest.DynamoDBFixture`.
    """

    def __init__(self):
        from cratedb_toolkit.testing.testcontainers.localstack import LocalStackContainerWithKeepalive

        self.container: LocalStackContainerWithKeepalive
        self.url = None
        self.kinesis_adapter: typing.Union[KinesisStreamAdapter, None] = None
        self._stream_name = "testdrive"
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
        Provide each test case with a fresh canvas by assigning a unique stream name.

        This avoids the delete/recreate race condition with LocalStack's eventual
        consistency, where ``create_stream`` can fail if called too soon after
        ``delete_stream`` completes.
        """
        self._stream_name = f"testdrive-{uuid.uuid4().hex[:8]}"

    def get_connection_url_kinesis(self):
        url = URL(self.container.get_url())
        return f"kinesis+dms://LSIAQAAAAAAVNCBMPNSG:dummy@{url.host}:{url.port}/{self._stream_name}"


@pytest.fixture(scope="session")
def kinesis_service():
    """
    Provide a Kinesis service instance to the test suite.
    """
    stack = KinesisFixture()
    yield stack
    stack.finalize()


@pytest.fixture(scope="function")
def kinesis(kinesis_service):
    """
    Provide a fresh canvas to each test case invocation, by assigning a unique stream name.
    """
    kinesis_service.reset()
    yield kinesis_service


@pytest.fixture(scope="function")
def kinesis_test_manager(kinesis):
    return KinesisTestManager(kinesis.get_connection_url_kinesis())
