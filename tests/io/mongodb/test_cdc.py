import datetime as dt
from pathlib import Path

from bson import ObjectId

from cratedb_toolkit.io.mongodb.cdc import MongoDBCDCRelayCrateDB
from cratedb_toolkit.io.mongodb.transform import TransformationManager
from tests.util.processor import BackgroundProcessor

DOCUMENT_INSERT = {
    "_id": ObjectId("669683c2b0750b2c84893f3e"),
    "id": "5F9E",
    "data": {"temperature": 42.42, "humidity": 84.84},
    "tombstone": "foo",
}


DOCUMENT_UPDATE = {
    "_id": ObjectId("669683c2b0750b2c84893f3e"),
    "id": "5F9E",
    "data": {"temperature": 42.5},
    "new": "foo",
    "some_date": dt.datetime(2024, 7, 11, 23, 17, 42),
}


def test_mongodb_cdc_insert_update(caplog, mongodb_replicaset, cratedb):
    """
    Roughly verify that the MongoDB CDC processing works as expected.
    """

    # Define source and target URLs.
    mongodb_url = f"{mongodb_replicaset.get_connection_url()}/testdrive/demo?direct=true"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Optionally configure transformations.
    tm = TransformationManager.from_any(Path("examples/zyp/zyp-transformation.yaml"))

    # Initialize table loader.
    table_loader = MongoDBCDCRelayCrateDB(
        mongodb_url=mongodb_url,
        cratedb_url=cratedb_url,
        tm=tm,
    )

    # Define target table name.
    table_name = table_loader.cratedb_table

    # Create source collection.
    table_loader.mongodb_adapter.create_collection()

    # Create target table.
    table_loader.cratedb_adapter.run_sql(table_loader.cdc.sql_ddl)

    # Start event processor / stream consumer in separate thread, consuming forever.
    with BackgroundProcessor(loader=table_loader, cratedb=cratedb) as processor:
        # Populate source database with data.
        processor.loader.mongodb_adapter.collection.insert_one(DOCUMENT_INSERT)
        next(processor)

        processor.loader.mongodb_adapter.collection.replace_one(
            filter={"_id": DOCUMENT_UPDATE["_id"]}, replacement=DOCUMENT_UPDATE
        )
        next(processor)

    # Verify data in target database, more specifically that both events have been processed well.
    assert cratedb.database.refresh_table(table_name) is True
    assert cratedb.database.count_records(table_name) == 1
    results = cratedb.database.run_sql(f"SELECT * FROM {table_name}", records=True)  # noqa: S608
    record = results[0]["data"]

    # Container content amendment.
    assert record["data"] == {"temperature": 42.5}

    # New attribute added.
    assert record["new"] == "foo"

    # Attribute deleted.
    assert "tombstone" not in record

    # Zyp transformation from dt.datetime.
    assert record["some_date"] == 1720739862000
