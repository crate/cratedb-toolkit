import argparse
import logging

from cratedb_toolkit.io.mongodb.core import export, extract, translate
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util.cr8 import cr8_insert_json
from cratedb_toolkit.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


def mongodb_copy(source_url, target_url, progress: bool = False):
    """
    Synopsis
    --------
    export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
    ctk load table mongodb://localhost:27017/testdrive/demo

    Backlog
    -------
    TODO: Run on multiple collections.
    TODO: Run on the whole database.
    TODO: Accept parameters like `if_exists="append,replace"`.
    TODO: Propagate parameters like `scan="full"`.
    TODO: Handle timestamp precision(s)?
    """
    logger.info("Running MongoDB copy")

    # Decode database URL.
    mongodb_address = DatabaseAddress.from_string(source_url)
    mongodb_uri, mongodb_collection_address = mongodb_address.decode()
    mongodb_database = mongodb_collection_address.schema
    mongodb_collection = mongodb_collection_address.table

    # 1. Extract schema from MongoDB collection.
    logger.info(f"Extracting schema from MongoDB: {mongodb_database}.{mongodb_collection}")
    extract_args = argparse.Namespace(
        url=str(mongodb_uri), database=mongodb_database, collection=mongodb_collection, scan="full"
    )
    mongodb_schema = extract(extract_args)
    count = mongodb_schema[mongodb_collection]["count"]
    if not count > 0:
        logger.error(f"No results when extracting schema from MongoDB: {mongodb_database}.{mongodb_collection}")
        return False

    # 2. Translate schema to SQL DDL.
    cratedb_address = DatabaseAddress.from_string(target_url)
    cratedb_uri, cratedb_table_address = cratedb_address.decode()
    ddl = translate(mongodb_schema, schemaname=cratedb_table_address.schema)

    # 3. Load schema SQL DDL into CrateDB.
    cratedb = DatabaseAdapter(dburi=str(cratedb_uri))
    for collection, query in ddl.items():
        logger.info(f"Creating table for collection '{collection}': {query}")
        cratedb.run_sql(query)

    # 4. Transfer data to CrateDB.
    """
    migr8 export --host localhost --port 27017 --database test_db --collection test | \
        cr8 insert-json --hosts localhost:4200 --table test
    """
    logger.info(
        f"Transferring data from MongoDB to CrateDB: "
        f"source={mongodb_collection_address.fullname}, target={cratedb_table_address.fullname}"
    )
    export_args = argparse.Namespace(url=str(mongodb_uri), database=mongodb_database, collection=mongodb_collection)
    buffer = export(export_args)
    cr8_insert_json(infile=buffer, hosts=cratedb_address.httpuri, table=cratedb_table_address.fullname)

    return True
