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
import datetime as dt
import logging
import time

import pymongo
from pymongo.database import Database

from cratedb_toolkit.adapter.pymongo import PyMongoCrateDbAdapter
from cratedb_toolkit.util.common import setup_logging

logger = logging.getLogger(__name__)


def mongodb_workload():
    client = pymongo.MongoClient(
        "localhost", 27017, timeoutMS=100, connectTimeoutMS=100, socketTimeoutMS=100, serverSelectionTimeoutMS=100
    )

    db: Database = client.test
    logger.info(f"Connecting to: {db.client.topology_description._topology_settings.seeds}")
    # TODO: logger.info(f"Server info: {db.client.server_info()}")
    logger.info(f"Using database: {db.name}")
    logger.info(f"Using collection: {db.my_collection}")

    # TODO: Dropping a collection is not implemented yet.
    # db.my_collection.drop()  # noqa: ERA001

    # Insert document.
    documents = [
        {
            "author": "Mike",
            "text": "My first blog post!",
            "tags": ["mongodb", "python", "pymongo"],
            "date": dt.datetime.now(tz=dt.timezone.utc),
        },
        {
            "author": "Eliot",
            "title": "MongoDB is fun",
            "text": "and pretty easy too!",
            "date": dt.datetime(2009, 11, 10, 10, 45),
        },
    ]
    result = db.my_collection.insert_many(documents)
    logger.info(f"Inserted document identifiers: {result.inserted_ids!r}")

    # FIXME: Refresh table.
    time.sleep(1)

    # Query documents.
    document_count = db.my_collection.count_documents({})
    logger.info(f"Total document count: {document_count}")

    # Find single document.
    document = db.my_collection.find_one({"author": "Mike"})
    logger.info(f"[find_one] Response document: {document}")

    # Run a few basic retrieval operations, with sorting and paging.
    print("Whole collection")
    for item in db.my_collection.find():
        print(item)
    print()

    print("Sort ascending")
    for item in db.my_collection.find().sort("author", pymongo.ASCENDING):
        print(item)
    print()

    print("Sort descending")
    for item in db.my_collection.find().sort("author", pymongo.DESCENDING):
        print(item)
    print()

    print("Paging")
    for item in db.my_collection.find().limit(2).skip(1):
        print(item)
    print()


def main(dburi: str = None):
    dburi = dburi or "crate://crate@localhost:4200"

    # setup_logging(level=logging.DEBUG, width=42)  # noqa: ERA001
    setup_logging(level=logging.INFO, width=20)

    # Context manager use.
    with PyMongoCrateDbAdapter(dburi=dburi):
        mongodb_workload()


if __name__ == "__main__":
    main()
