import logging

import pytest
from yarl import URL

from tests.io.dynamodb.manager import DynamoDBTestManager

logger = logging.getLogger(__name__)


# Define databases to be deleted before running each test case.
RESET_TABLES = [
    "ProductCatalog",
]


class DynamoDBFixture:
    """
    A little helper wrapping Testcontainer's `LocalStackContainer`.
    """

    def __init__(self):
        self.container = None
        self.url = None
        self.setup()

    def setup(self):
        # TODO: Make image name configurable.
        from cratedb_toolkit.testing.testcontainers.localstack import LocalStackContainerWithKeepalive

        self.container = LocalStackContainerWithKeepalive()
        self.container.with_services("dynamodb", "kinesis")
        self.container.start()

    def finalize(self):
        self.container.stop()

    def reset(self):
        """
        Drop all databases used for testing.
        """
        # FIXME
        return
        for database_name in RESET_TABLES:
            self.client.drop_database(database_name)

    def get_connection_url_dynamodb(self):
        url = URL(self.container.get_url())
        return f"dynamodb://LSIAQAAAAAAVNCBMPNSG:dummy@{url.host}:{url.port}"

    def get_connection_url_kinesis_dynamodb_cdc(self):
        url = URL(self.container.get_url())
        return f"kinesis+dynamodb+cdc://LSIAQAAAAAAVNCBMPNSG:dummy@{url.host}:{url.port}"


@pytest.fixture(scope="session")
def dynamodb_service():
    """
    Provide a DynamoDB service instance to the test suite.
    """
    db = DynamoDBFixture()
    db.reset()
    yield db
    db.finalize()


@pytest.fixture(scope="function")
def dynamodb(dynamodb_service):
    """
    Provide a fresh canvas to each test case invocation, by resetting database content.
    """
    dynamodb_service.reset()
    yield dynamodb_service


@pytest.fixture(scope="session")
def dynamodb_test_manager(dynamodb_service):
    return DynamoDBTestManager(dynamodb_service.get_connection_url_dynamodb())
