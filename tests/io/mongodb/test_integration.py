import logging
import os
import unittest
from unittest import mock

import pymongo

from cratedb_toolkit.io.mongodb.core import gather_collections
from cratedb_toolkit.testing.testcontainers.mongodb import MongoDbContainerWithKeepalive

logger = logging.getLogger(__name__)


class TestMongoDBIntegration(unittest.TestCase):
    """
    A few conditional integration test cases with MongoDB.
    For providing a MongoDB instance, it uses Testcontainers for Python.
    """

    DBNAME = "testdrive"

    SKIP_IF_NOT_RUNNING = False

    @classmethod
    def setUpClass(cls):
        cls.startMongoDB()
        cls.client = cls.mongodb.get_connection_client()
        cls.db = cls.client.get_database(cls.DBNAME)
        try:
            server_info = cls.client.server_info()
            logger.debug(f"MongoDB server info: {server_info}")
        except pymongo.errors.ServerSelectionTimeoutError as ex:
            if cls.SKIP_IF_NOT_RUNNING:
                raise cls.skipTest(cls, reason="MongoDB server not running") from ex
            else:  # noqa: RET506
                raise

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(cls.DBNAME)
        cls.client.close()
        cls.stopMongoDB()

    @classmethod
    def startMongoDB(cls):
        mongodb_version = os.environ.get("MONGODB_VERSION", "7")
        mongodb_image = f"mongo:{mongodb_version}"
        cls.mongodb = MongoDbContainerWithKeepalive(mongodb_image).with_name("testcontainers-mongodb")
        cls.mongodb.start()

    @classmethod
    def stopMongoDB(cls):
        cls.mongodb.stop()

    def test_gather_collections(self):
        """
        Verify if core method `gather_collections` works as expected.
        """
        self.db.create_collection("foobar")
        with mock.patch("builtins.input", return_value="unknown"):
            collections = gather_collections(database=self.db)
            self.assertEqual(collections, ["foobar"])
