"""
About
=====

Example program demonstrating PyMongo to CrateDB adapter.
Status: Experimental, only basic operations work.

Synopsis
========
::

    pip install cratedb-toolkit[mongodb]

    python examples/pymongo_adapter.py
    crash --command 'SELECT * FROM "test"."my_collection";'

Tests
=====
::

     pytest --no-cov tests/adapter/test_pymongo.py

References
==========
- https://github.com/mongodb/mongo-python-driver
"""
import logging

import pymongo
from pymongo.database import Database

from cratedb_toolkit.adapter.pymongo import PyMongoCrateDbAdapter
from cratedb_toolkit.util.common import setup_logging

logger = logging.getLogger(__name__)


def main():
    client = pymongo.MongoClient(
        "localhost", 27017, timeoutMS=100, connectTimeoutMS=100, socketTimeoutMS=100, serverSelectionTimeoutMS=100
    )

    db: Database = client.test
    logger.info(f"Connecting to: {db.client.topology_description._topology_settings.seeds}")
    # TODO: logger.info(f"Server info: {db.client.server_info()}")
    logger.info(f"Using database: {db.name}")
    logger.info(f"Using collection: {db.my_collection}")

    # Insert records.
    inserted_id = db.my_collection.insert_one({"x": 5}).inserted_id
    logger.info(f"Inserted object: {inserted_id!r}")

    # FIXME: Refresh table.

    # Query records.
    document_count = db.my_collection.count_documents({})
    logger.info(f"Total document count: {document_count}")

    # Find single document.
    document = db.my_collection.find_one()
    logger.info(f"[find_one] Response document: {document}")

    # Assorted basic find operations, with sorting and paging.
    print("results:", end=" ")
    for item in db.my_collection.find():
        print(item["x"], end=", ")
    print()

    print("results:", end=" ")
    for item in db.my_collection.find().sort("x", pymongo.ASCENDING):
        print(item["x"], end=", ")
    print()

    print("results:", end=" ")
    for item in db.my_collection.find().sort("x", pymongo.DESCENDING):
        print(item["x"], end=", ")
    print()

    results = [item["x"] for item in db.my_collection.find().limit(2).skip(1)]
    print("results:", results)
    print("length:", len(results))


if __name__ == "__main__":
    # setup_logging(level=logging.DEBUG, width=42)  # noqa: ERA001
    setup_logging(level=logging.INFO, width=20)

    # Context manager use.
    with PyMongoCrateDbAdapter(dburi="crate://crate@localhost:4200"):
        main()
