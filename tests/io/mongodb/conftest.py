import logging

import pytest

from tests.conftest import check_sqlalchemy2

logger = logging.getLogger(__name__)


pytest.importorskip("bson", reason="Skipping tests because bson is not installed")
pytest.importorskip("bsonjs", reason="Skipping tests because bsonjs is not installed")
pytest.importorskip("pymongo", reason="Skipping tests because pymongo is not installed")
pytest.importorskip("rich", reason="Skipping tests because rich is not installed")


# Define databases to be deleted before running each test case.
RESET_DATABASES = [
    "testdrive",
]


class MongoDBFixture:
    """
    A little helper wrapping Testcontainer's `MongoDbContainer`.
    """

    def __init__(self):
        from pymongo import MongoClient

        self.container = None
        self.client: MongoClient = None
        self.setup()

    def setup(self):
        # TODO: Make image name configurable.
        from cratedb_toolkit.testing.testcontainers.mongodb import MongoDbContainerWithKeepalive

        self.container = MongoDbContainerWithKeepalive()
        self.container.start()
        self.client = self.container.get_connection_client()

    def finalize(self):
        self.container.stop()

    def reset(self):
        """
        Drop all databases used for testing.
        """
        for database_name in RESET_DATABASES:
            self.client.drop_database(database_name)

    def get_connection_url(self):
        return self.container.get_connection_url()

    def get_connection_client(self):
        return self.container.get_connection_client()


@pytest.fixture(scope="session")
def mongodb_service():
    """
    Provide an MongoDB service instance to the test suite.
    """
    check_sqlalchemy2()
    db = MongoDBFixture()
    db.reset()
    yield db
    db.finalize()


@pytest.fixture(scope="function")
def mongodb(mongodb_service):
    """
    Provide a fresh canvas to each test case invocation, by resetting database content.
    """
    mongodb_service.reset()
    yield mongodb_service
