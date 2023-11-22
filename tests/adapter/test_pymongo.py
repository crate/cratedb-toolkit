import typing as t

import pymongo
import pytest

from cratedb_toolkit.adapter.pymongo import PyMongoCrateDbAdapter
from cratedb_toolkit.adapter.pymongo.util import AmendedObjectId
from tests.conftest import TESTDRIVE_DATA_SCHEMA, CrateDBFixture

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
    pymongo_cratedb: PyMongoCrateDbAdapter, pymongo_client: pymongo.MongoClient, cratedb: CrateDBFixture
):
    """
    Verify a single basic data insert operation `insert_one` works well.
    """

    # Insert records.
    collection: pymongo.collection.Collection = pymongo_client[TESTDRIVE_DATA_SCHEMA].foobar
    inserted_id = collection.insert_one({"x": 42}).inserted_id

    # TODO: Can this be made type-compatible, by swapping in the surrogate implementation?
    assert isinstance(inserted_id, AmendedObjectId)

    cratedb.database.run_sql(f'REFRESH TABLE "{TESTDRIVE_DATA_SCHEMA}"."foobar";')
    results = cratedb.database.run_sql(
        f'SELECT * FROM "{TESTDRIVE_DATA_SCHEMA}"."foobar" ORDER BY _id;', records=True  # noqa: S608
    )
    assert results == [{"x": 42}]


def test_pymongo_count_documents(
    pymongo_cratedb: PyMongoCrateDbAdapter, pymongo_client: pymongo.MongoClient, cratedb: CrateDBFixture
):
    """
    Verify the `count_documents` operation works well.
    """

    # Insert records.
    collection: pymongo.collection.Collection = pymongo_client[TESTDRIVE_DATA_SCHEMA].foobar
    collection.insert_one({"x": 42})

    cratedb.database.run_sql(f'REFRESH TABLE "{TESTDRIVE_DATA_SCHEMA}"."foobar";')
    assert collection.count_documents({}) == 1
