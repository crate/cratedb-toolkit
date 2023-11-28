# ruff: noqa: E402
import datetime as dt
import typing as t
from unittest import mock

import pymongo
import pytest

from cratedb_toolkit.testing.testcontainers.cratedb import CrateDBTestAdapter
from tests.conftest import check_sqlalchemy1

check_sqlalchemy1(allow_module_level=True)

from cratedb_toolkit.adapter.pymongo import PyMongoCrateDbAdapter
from cratedb_toolkit.adapter.pymongo.util import AmendedObjectId
from cratedb_toolkit.util.date import truncate_milliseconds
from tests.conftest import TESTDRIVE_DATA_SCHEMA

pytestmark = pytest.mark.mongodb


@pytest.fixture
def pymongo_cratedb(cratedb):
    """
    Enable the PyMongo -> CrateDB adapter.
    """
    with PyMongoCrateDbAdapter(dburi=cratedb.database.dburi) as adapter:
        yield adapter


@pytest.fixture
def pymongo_client():
    """
    Provide a PyMongo client to the test cases.
    It will be amalgamated to talk to CrateDB instead.
    """
    return pymongo.MongoClient(
        "localhost",
        27017,
        connectTimeoutMS=100,
        serverSelectionTimeoutMS=100,
        socketTimeoutMS=100,
        timeoutMS=100,
    )


@pytest.fixture
def sync_writes(cratedb):
    def refresh():
        cratedb.database.refresh_table(f'"{TESTDRIVE_DATA_SCHEMA}"."foobar"')

    return refresh


def test_pymongo_metadata(pymongo_cratedb: PyMongoCrateDbAdapter, pymongo_client: pymongo.MongoClient):
    """
    Verify attribute access to the database and collection handles works well.
    """

    db: pymongo.database.Database = pymongo_client.testdrive
    assert db.name == "testdrive"

    hosts: t.Dict = db.client.topology_description._topology_settings.seeds
    assert hosts == {("localhost", 27017)}

    collection: pymongo.collection.Collection = db.foobar
    assert collection.name == "foobar"


def test_pymongo_insert_one_single(
    pymongo_cratedb: PyMongoCrateDbAdapter,
    pymongo_client: pymongo.MongoClient,
    cratedb: CrateDBTestAdapter,
    sync_writes,
):
    """
    Verify a single basic data insert operation `insert_one` works well.
    """

    # Insert document.
    collection: pymongo.collection.Collection = pymongo_client[TESTDRIVE_DATA_SCHEMA].foobar
    inserted_id = collection.insert_one({"x": 42}).inserted_id

    # TODO: Can this be made type-compatible, by swapping in the surrogate implementation?
    assert isinstance(inserted_id, AmendedObjectId)

    # Synchronize write operations.
    sync_writes()

    results = cratedb.database.run_sql(
        f'SELECT * FROM "{TESTDRIVE_DATA_SCHEMA}"."foobar" ORDER BY _id;', records=True  # noqa: S608
    )
    assert results == [{"x": 42}]


def test_pymongo_insert_one_multiple(
    pymongo_cratedb: PyMongoCrateDbAdapter,
    pymongo_client: pymongo.MongoClient,
    cratedb: CrateDBTestAdapter,
    sync_writes,
):
    """
    Verify the basic data insert operation `insert_one` works well, when called multiple times.
    It should dynamically and gradually extend the database schema.
    """

    # Insert documents.
    collection: pymongo.collection.Collection = pymongo_client[TESTDRIVE_DATA_SCHEMA].foobar
    inserted_id_x = collection.insert_one({"x": 42}).inserted_id
    inserted_id_y = collection.insert_one({"y": 84}).inserted_id

    # TODO: Can this be made type-compatible, by swapping in the surrogate implementation?
    assert isinstance(inserted_id_x, AmendedObjectId)
    assert isinstance(inserted_id_y, AmendedObjectId)

    # Synchronize write operations.
    sync_writes()

    results = cratedb.database.run_sql(
        f'SELECT * FROM "{TESTDRIVE_DATA_SCHEMA}"."foobar" ORDER BY _id;', records=True  # noqa: S608
    )
    assert {"x": 42, "y": None} in results
    assert {"x": None, "y": 84} in results


def test_pymongo_insert_many(
    pymongo_cratedb: PyMongoCrateDbAdapter,
    pymongo_client: pymongo.MongoClient,
    cratedb: CrateDBTestAdapter,
    sync_writes,
):
    """
    Verify the basic data insert operation `insert_many` works well.
    """

    # Insert documents.
    collection: pymongo.collection.Collection = pymongo_client[TESTDRIVE_DATA_SCHEMA].foobar
    result = collection.insert_many([{"x": 42}, {"y": 84}])
    assert len(result.inserted_ids) == 2
    inserted_id_x = result.inserted_ids[0]
    inserted_id_y = result.inserted_ids[1]

    # TODO: Can this be made type-compatible, by swapping in the surrogate implementation?
    assert isinstance(inserted_id_x, AmendedObjectId)
    assert isinstance(inserted_id_y, AmendedObjectId)

    # Synchronize write operations.
    sync_writes()

    # Verify documents in database.
    assert collection.find_one({"x": 42}) == {"x": 42, "y": None, "_id": mock.ANY}
    assert collection.find_one({"y": 84}) == {"x": None, "y": 84, "_id": mock.ANY}


def test_pymongo_count_documents(
    pymongo_cratedb: PyMongoCrateDbAdapter,
    pymongo_client: pymongo.MongoClient,
    cratedb: CrateDBTestAdapter,
    sync_writes,
):
    """
    Verify the `count_documents` operation works well.
    """

    # Insert documents.
    collection: pymongo.collection.Collection = pymongo_client[TESTDRIVE_DATA_SCHEMA].foobar
    collection.insert_one({"x": 42})
    collection.insert_one({"y": 42})

    # Synchronize write operations.
    sync_writes()

    # Validate `count_documents` method.
    assert collection.count_documents({}) == 2
    assert collection.count_documents({"x": 42}) == 1
    assert collection.count_documents({"y": 42}) == 1

    assert collection.count_documents({"foo": "bar"}) == 0


def test_pymongo_roundtrip_document(
    pymongo_cratedb: PyMongoCrateDbAdapter,
    pymongo_client: pymongo.MongoClient,
    cratedb: CrateDBTestAdapter,
    sync_writes,
):
    """
    Verify round-tripping a documents works well.

    The function employs workarounds to make the documents match.

    - Python datetime objects with timezone do not work yet.
    - Timestamps are truncated to millisecond resolution.
    - Document identifiers (ObjectId instances) are not round-tripped yet.
    """

    # Define single document to insert.
    document_original = {
        "author": "Mike",
        "text": "My first blog post!",
        "tags": ["mongodb", "python", "pymongo"],
        # TODO: With timezone. "date": dt.datetime.now(tz=dt.timezone.utc),
        "date": dt.datetime.now(),
    }

    # Insert document.
    collection: pymongo.collection.Collection = pymongo_client[TESTDRIVE_DATA_SCHEMA].foobar
    collection.insert_one(document_original)

    # Synchronize write operations.
    sync_writes()

    # Getting a Single Document With find_one().
    # https://pymongo.readthedocs.io/en/stable/tutorial.html#getting-a-single-document-with-find-one
    document_loaded = collection.find_one({"author": "Mike"})

    # Delete document identifier, because the test can't know about it.
    # TODO: Compare document identifiers (ObjectId) in one way or another.
    del document_loaded["_id"]

    # CrateDB stores timestamps with milliseconds resolution. When comparing
    # Python datetime objects loaded from CrateDB, they need to be downgraded
    # from microseconds resolution.
    # TODO: Request for higher resolutions. https://github.com/crate/crate/issues/9430
    document_original["date"] = truncate_milliseconds(document_original["date"])
    document_loaded["date"] = truncate_milliseconds(document_loaded["date"])

    # Compare document representations.
    assert document_loaded == document_original


def test_example_program(cratedb: CrateDBTestAdapter):
    """
    Verify that the program `examples/pymongo_adapter.py` works.
    """
    from examples.pymongo_adapter import main

    main(dburi=cratedb.database.dburi)


def test_pymongo_tutorial(
    pymongo_cratedb: PyMongoCrateDbAdapter,
    pymongo_client: pymongo.MongoClient,
    cratedb: CrateDBTestAdapter,
    sync_writes,
):
    """
    Verify the PyMongo Tutorial works well.

    https://pymongo.readthedocs.io/en/stable/tutorial.html
    """

    # Define multiple documents to insert.
    post_items = [
        {
            "author": "Mike",
            "text": "My first blog post!",
            "tags": ["mongodb", "python", "pymongo"],
            "date": dt.datetime.now(tz=dt.timezone.utc),
        },
        {
            "author": "Mike",
            "text": "Another post!",
            "tags": ["bulk", "insert"],
            "date": dt.datetime(2009, 11, 12, 11, 14),
        },
        {
            "author": "Eliot",
            "title": "MongoDB is fun",
            "text": "and pretty easy too!",
            "date": dt.datetime(2009, 11, 10, 10, 45),
        },
    ]

    # Bulk Inserts.
    # https://pymongo.readthedocs.io/en/stable/tutorial.html#bulk-inserts
    posts: pymongo.collection.Collection = pymongo_client[TESTDRIVE_DATA_SCHEMA].foobar
    result = posts.insert_many(post_items)
    assert len(result.inserted_ids) == 3
    assert isinstance(result.inserted_ids[0], AmendedObjectId)

    # Synchronize write operations.
    sync_writes()

    # Getting a Single Document With find_one().
    # https://pymongo.readthedocs.io/en/stable/tutorial.html#getting-a-single-document-with-find-one
    document_loaded = posts.find_one({"author": "Mike"})
    assert "mongodb" in document_loaded["tags"]

    # Querying By ObjectId.
    # https://pymongo.readthedocs.io/en/stable/tutorial.html#querying-by-objectid
    post_id_1 = result.inserted_ids[0]
    # Note that an ObjectId is not the same as its string representation.
    assert posts.find_one({"_id": str(post_id_1)}) is None
    # It is necessary to convert the ObjectId from a string before passing it to find_one.
    # FIXME: Solve ObjectId value conversion.
    # assert posts.find_one({"_id": post_id_1})["author"] == "Mike"  # noqa: ERA001

    # Querying for More Than One Document.
    # https://pymongo.readthedocs.io/en/stable/tutorial.html#querying-for-more-than-one-document
    assert list(posts.find({"author": "Mike"})) == [
        {
            "author": "Mike",
            "text": "My first blog post!",
            "tags": mock.ANY,
            "date": mock.ANY,
            "title": mock.ANY,
            "_id": mock.ANY,
        },
        {
            "author": "Mike",
            "text": "Another post!",
            "tags": mock.ANY,
            "date": mock.ANY,
            "title": mock.ANY,
            "_id": mock.ANY,
        },
    ]

    # Counting.
    # https://pymongo.readthedocs.io/en/stable/tutorial.html#counting
    assert posts.count_documents({}) == 3
    assert posts.count_documents({"author": "Mike"}) == 2

    # Range Queries.
    # https://pymongo.readthedocs.io/en/stable/tutorial.html#range-queries
    d = dt.datetime(2009, 11, 12, 12)
    iter_documents = posts.find({"date": {"$lt": d}}).sort("author")
    documents = list(iter_documents)
    assert documents[0]["author"] == "Eliot"
    assert documents[1]["author"] == "Mike"

    # Indexing.
    # https://pymongo.readthedocs.io/en/stable/tutorial.html#indexing
    """
    db = pymongo_client[TESTDRIVE_DATA_SCHEMA]
    result = db.profiles.create_index([("user_id", pymongo.ASCENDING)], unique=True)
    assert sorted(list(db.profiles.index_information())) == ['_id_', 'user_id_1']
    """
