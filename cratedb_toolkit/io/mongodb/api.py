import argparse
import logging
import typing as t
from pathlib import Path

from boltons.urlutils import URL
from polars.exceptions import PanicException
from zyp.model.project import TransformationProject

from cratedb_toolkit.io.mongodb.adapter import mongodb_adapter_factory
from cratedb_toolkit.io.mongodb.cdc import MongoDBCDCRelayCrateDB
from cratedb_toolkit.io.mongodb.copy import MongoDBFullLoad
from cratedb_toolkit.io.mongodb.core import export, extract, translate
from cratedb_toolkit.io.mongodb.transform import TransformationManager
from cratedb_toolkit.model import AddressPair, DatabaseAddress
from cratedb_toolkit.util.cr8 import cr8_insert_json
from cratedb_toolkit.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


def mongodb_copy_migr8(source_url, target_url, transformation: Path = None, limit: int = 0, progress: bool = False):
    """
    Transfer MongoDB collection using migr8.

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
        url=str(mongodb_uri) + f"&limit={limit}",
        database=mongodb_database,
        collection=mongodb_collection,
        scan="partial",
        transformation=transformation,
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
    export_args = argparse.Namespace(
        url=str(mongodb_uri) + f"&limit={limit}",
        database=mongodb_database,
        collection=mongodb_collection,
        transformation=transformation,
    )
    buffer = export(export_args)
    cr8_insert_json(infile=buffer, hosts=cratedb_address.httpuri, table=cratedb_table_address.fullname)

    return True


def mongodb_copy(
    source_url: t.Union[str, URL],
    target_url: t.Union[str, URL],
    transformation: t.Union[Path, TransformationManager, TransformationProject, None] = None,
    progress: bool = False,
):
    """
    Transfer MongoDB collection using translator component.

    Synopsis
    --------
    export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
    ctk load table mongodb://localhost:27017/testdrive/demo
    """

    logger.info(f"mongodb_copy. source={source_url}, target={target_url}")

    source_url = URL(source_url)
    target_url = URL(target_url)

    # Optionally configure transformations.
    tm = TransformationManager.from_any(transformation)

    # Check if source address URL includes a table name or not.
    has_table = True
    if "*" in source_url.path:
        has_table = False
    mongodb_address = DatabaseAddress(source_url)
    mongodb_uri, mongodb_collection_address = mongodb_address.decode()
    if mongodb_collection_address.table is None:
        has_table = False

    # Build list of tasks. Either a single one when transferring a single
    # collection into a table, or multiple ones when transferring multiple
    # collections.
    tasks = []

    # `full-load` procedure, single collection.
    if has_table:
        tasks.append(
            MongoDBFullLoad(
                mongodb_url=source_url,
                cratedb_url=target_url,
                tm=tm,
                progress=progress,
            )
        )

    # `full-load` procedure, multiple collections.
    else:
        logger.info(f"Inquiring collections at {source_url}")
        address_pair_root = AddressPair(source_url=source_url, target_url=target_url)

        mongodb_adapter = mongodb_adapter_factory(address_pair_root.source_url)
        collections = mongodb_adapter.get_collection_names()
        logger.info(f"Discovered collections: {len(collections)}")
        logger.debug(f"Processing collections: {collections}")

        for collection_path in collections:
            address_pair = address_pair_root.navigate(
                source_path=Path(collection_path).name,
                target_path=Path(collection_path).stem,
            )
            tasks.append(
                MongoDBFullLoad(
                    mongodb_url=address_pair.source_url,
                    cratedb_url=address_pair.target_url,
                    tm=tm,
                    progress=progress,
                )
            )

    outcome = True
    for task in tasks:
        try:
            outcome_task = task.start()
        except (Exception, PanicException) as ex:
            logger.exception(f"Task failed: {ex}")
            outcome_task = False
        outcome = outcome and outcome_task

    return outcome


def mongodb_relay_cdc(
    source_url,
    target_url,
    transformation: t.Union[Path, TransformationManager, TransformationProject, None] = None,
):
    """
    Synopsis
    --------
    export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo-cdc
    ctk load table mongodb+cdc://localhost:27017/testdrive/demo

    Backlog
    -------
    TODO: Run on multiple collections.
    TODO: Run on the whole database.
    TODO: Accept parameters like `if_exists="append,replace"`.
    TODO: Propagate parameters like `scan="full"`.
    """
    logger.info("Running MongoDB CDC relay")

    # Optionally configure transformations.
    tm = TransformationManager.from_any(transformation)

    # Configure machinery.
    relay = MongoDBCDCRelayCrateDB(
        mongodb_url=source_url,
        cratedb_url=target_url,
        tm=tm,
    )

    # Invoke machinery.
    relay.start()
