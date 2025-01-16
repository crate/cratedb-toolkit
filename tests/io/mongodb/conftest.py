import logging
import os

import pytest

from cratedb_toolkit.testing.testcontainers.util import PytestTestcontainerAdapter
from tests.conftest import check_sqlalchemy2

logger = logging.getLogger(__name__)


pytest.importorskip("bson", reason="Skipping tests because bson is not installed")
pytest.importorskip("bsonjs", reason="Skipping tests because bsonjs is not installed")
pytest.importorskip("pymongo", reason="Skipping tests because pymongo is not installed")
pytest.importorskip("rich", reason="Skipping tests because rich is not installed")
pytest.importorskip("undatum", reason="Skipping tests because undatum is not installed")


# Define databases to be deleted before running each test case.
RESET_DATABASES = [
    "testdrive",
]


class MongoDBFixture(PytestTestcontainerAdapter):
    """
    A little helper wrapping Testcontainer's `MongoDbContainer`.
    """

    def __init__(self, container_class):
        from pymongo import MongoClient

        self.container_class = container_class
        self.container = None
        self.client: MongoClient = None
        super().__init__()

    def setup(self):
        # TODO: Make image name configurable.

        mongodb_version = os.environ.get("MONGODB_VERSION", "7")
        mongodb_image = f"mongo:{mongodb_version}"

        self.container = self.container_class(
            image=mongodb_image,
        )
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

    def get_connection_client_replicaset(self):
        return self.container.get_connection_client_replicaset()


class MongoDBFixtureFactory:
    def __init__(self, container):
        self.db = MongoDBFixture(container)

    def __enter__(self):
        self.db.reset()
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.finalize()


@pytest.fixture(scope="session")
def mongodb_service():
    """
    Provide an MongoDB service instance to the test suite.
    """
    check_sqlalchemy2()
    from cratedb_toolkit.testing.testcontainers.mongodb import MongoDbContainerWithKeepalive

    with MongoDBFixtureFactory(container=MongoDbContainerWithKeepalive) as mongo:
        yield mongo


@pytest.fixture(scope="function")
def mongodb(mongodb_service):
    """
    Provide a fresh canvas to each test case invocation, by resetting database content.
    """
    mongodb_service.reset()
    yield mongodb_service


@pytest.fixture(scope="session")
def mongodb_replicaset_service():
    """
    Provide an MongoDB service instance to the test suite.
    """
    check_sqlalchemy2()
    from cratedb_toolkit.testing.testcontainers.mongodb import MongoDbReplicasetContainer

    with MongoDBFixtureFactory(container=MongoDbReplicasetContainer) as mongo:
        yield mongo


@pytest.fixture(scope="function")
def mongodb_replicaset(mongodb_replicaset_service):
    """
    Provide a fresh canvas to each test case invocation, by resetting database content.
    """
    mongodb_replicaset_service.reset()
    yield mongodb_replicaset_service
