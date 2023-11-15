import logging

import pytest

from cratedb_toolkit.testing.testcontainers.util import PytestTestcontainerAdapter

logger = logging.getLogger(__name__)


# Define buckets to be deleted before running each test case.
RESET_BUCKETS = [
    "testdrive",
]


class InfluxDB2Fixture(PytestTestcontainerAdapter):
    """
    A little helper wrapping Testcontainer's `InfluxDB2Container`.
    """

    def __init__(self):
        from influxdb_client import InfluxDBClient

        self.container = None
        self.client: InfluxDBClient = None
        super().__init__()

    def setup(self):
        # TODO: Make image name configurable.
        from cratedb_toolkit.testing.testcontainers.influxdb2 import InfluxDB2Container

        self.container = InfluxDB2Container()
        self.container.start()
        self.client = self.container.get_connection_client()

    def reset(self):
        """
        Delete all buckets used for testing.
        """
        for bucket_name in RESET_BUCKETS:
            bucket = self.client.buckets_api().find_bucket_by_name(bucket_name)
            if bucket is not None:
                self.client.buckets_api().delete_bucket(bucket)

    def get_connection_url(self, *args, **kwargs):
        return self.container.get_connection_url(*args, **kwargs)


@pytest.fixture(scope="session")
def influxdb_service():
    """
    Provide an InfluxDB service instance to the test suite.
    """
    db = InfluxDB2Fixture()
    db.reset()
    yield db
    db.stop()


@pytest.fixture(scope="function")
def influxdb(influxdb_service):
    """
    Provide a fresh canvas to each test case invocation, by resetting database content.
    """
    influxdb_service.reset()
    yield influxdb_service
