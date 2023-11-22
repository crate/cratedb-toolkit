from unittest.mock import patch

import pymongo.collection

from cratedb_toolkit.adapter.pymongo.collection import collection_factory
from cratedb_toolkit.util import DatabaseAdapter


class PyMongoCrateDbAdapter:
    """
    Patch PyMongo to talk to CrateDB.
    """

    def __init__(self, dburi: str):
        self.cratedb = DatabaseAdapter(dburi=dburi)
        self.collection_backup = pymongo.collection.Collection

    def start(self):
        self.__enter__()

    def __enter__(self):
        self.activate_pymongo_adapter()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Restore patched functions.
        """
        pymongo.collection.Collection = pymongo.database.Collection = self.collection_backup  # type: ignore[misc]

    def activate_pymongo_adapter(self):
        """
        Swap in the MongoDB -> CrateDB adapter.
        """
        self.patch_pymongo_noops()
        pymongo.collection.Collection = pymongo.database.Collection = collection_factory(cratedb=self.cratedb)  # type: ignore[misc]

    def patch_pymongo_noops(self):
        """
        Converge a few low-level functions of PyMongo to no-ops.
        """
        patches = [
            patch("pymongo.mongo_client.MongoClient._ensure_session"),
            patch("pymongo.mongo_client._ClientConnectionRetryable._get_server"),
        ]
        for patch_ in patches:
            patch_.start()
