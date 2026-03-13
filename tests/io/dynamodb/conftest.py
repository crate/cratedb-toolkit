# ruff: noqa: E402
import logging
import typing
import uuid

import pytest

pytest.importorskip("boto3", reason="Skipping DynamoDB tests because 'boto3' package is not installed")
pytest.importorskip("commons_codec", reason="Skipping DynamoDB tests because 'commons-codec' package is not installed")
pytest.importorskip("kinesis", reason="Skipping DynamoDB tests because 'async-kinesis' package is not installed")

import botocore
from yarl import URL

from cratedb_toolkit.io.dynamodb.adapter import DynamoDBAdapter
from tests.io.dynamodb.manager import DynamoDBTestManager

logger = logging.getLogger(__name__)


# Define tables to be deleted before running each test case.
RESET_TABLES = [
    "ProductCatalog",
]


class DynamoDBFixture:
    """
    A little helper wrapping Testcontainer's ``LocalStackContainer``.

    TODO: Generalize into ``LocalStackFixture``, see also ``tests.io.kinesis.conftest.KinesisFixture``.
    """

    def __init__(self):
        self.container = None
        self.url = None
        self.dynamodb_adapter: typing.Union[DynamoDBAdapter, None] = None
        self._stream_name = f"demo-{uuid.uuid4().hex[:8]}"
        self.setup()

    def setup(self):
        # TODO: Make image name configurable.
        from cratedb_toolkit.testing.testcontainers.localstack import LocalStackContainerWithKeepalive

        self.container = LocalStackContainerWithKeepalive()
        self.container.with_services("dynamodb", "kinesis")
        self.container.start()

        self.dynamodb_adapter = DynamoDBAdapter(URL(f"{self.get_connection_url_dynamodb()}?region=us-east-1"))

    def finalize(self):
        self.container.stop()

    def reset(self):
        """
        Provide each test case with a fresh canvas by assigning a unique stream name.

        This avoids the delete/recreate race condition with LocalStack's eventual
        consistency, where ``create_stream`` can fail if called too soon after
        ``delete_stream`` completes.
        """
        self._stream_name = f"demo-{uuid.uuid4().hex[:8]}"
        self.reset_tables()

    def reset_tables(self):
        """
        Drop all DynamoDB tables used for testing.
        """
        dynamodb_client = self.dynamodb_adapter.dynamodb_client
        for table_name in RESET_TABLES:
            try:
                dynamodb_client.delete_table(TableName=table_name)
            except botocore.exceptions.ClientError as error:
                if error.response["Error"]["Code"] != "ResourceNotFoundException":
                    raise
            waiter = dynamodb_client.get_waiter("table_not_exists")
            waiter.wait(TableName=table_name, WaiterConfig={"Delay": 0.3, "MaxAttempts": 15})

    def get_connection_url_dynamodb(self):
        url = URL(self.container.get_url())
        return f"dynamodb://LSIAQAAAAAAVNCBMPNSG:dummy@{url.host}:{url.port}"

    def get_connection_url_kinesis_dynamodb_cdc(self):
        url = URL(self.container.get_url())
        return f"kinesis+dynamodb+cdc://LSIAQAAAAAAVNCBMPNSG:dummy@{url.host}:{url.port}/{self._stream_name}"


@pytest.fixture(scope="session")
def dynamodb_service():
    """
    Provide a DynamoDB service instance to the test suite.
    """
    db = DynamoDBFixture()
    yield db
    db.finalize()


@pytest.fixture(scope="function")
def dynamodb(dynamodb_service):
    """
    Provide a fresh canvas to each test case invocation, by assigning a unique stream name.
    """
    dynamodb_service.reset()
    yield dynamodb_service


@pytest.fixture(scope="session")
def dynamodb_test_manager(dynamodb_service):
    return DynamoDBTestManager(dynamodb_service.get_connection_url_dynamodb())
