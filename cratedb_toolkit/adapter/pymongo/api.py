from unittest.mock import patch

import pymongo.collection

from cratedb_toolkit.adapter.pymongo.collection import collection_factory
from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.util.pandas import patch_pandas_sqltable_with_extended_mapping
from cratedb_toolkit.util.sqlalchemy import patch_types_map


class PyMongoCrateDBAdapter:
    """
    Patch PyMongo to talk to CrateDB.
    """

    def __init__(self, dburi: str):
        self.cratedb = DatabaseAdapter(dburi=dburi)
        self.collection_backup = pymongo.collection.Collection

        collection_patched = collection_factory(cratedb=self.cratedb)  # type: ignore[misc]
        self.patches = [
            # Patch PyMongo's `Collection` implementation.
            patch("pymongo.collection.Collection", collection_patched),
            patch("pymongo.database.Collection", collection_patched),
            # Converge a few low-level functions of PyMongo to no-ops.
            patch("pymongo.mongo_client.MongoClient._ensure_session"),
            patch("pymongo.mongo_client._ClientConnectionRetryable._get_server"),
        ]

    def start(self):
        self.adjust_sqlalchemy()
        self.activate()

    def stop(self):
        self.deactivate()

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def adjust_sqlalchemy(self):
        """
        Configure CrateDB SQLAlchemy dialect.

        Setting the CrateDB column policy to `dynamic` means that new columns
        can be added without needing to explicitly change the table definition
        by running corresponding `ALTER TABLE` statements.

        https://cratedb.com/docs/crate/reference/en/latest/general/ddl/column-policy.html#dynamic
        """
        # 1. Patch data types for CrateDB dialect.
        # TODO: Upstream to `sqlalchemy-cratedb`.
        patch_types_map()

        # 2. Prepare pandas.
        # TODO: Provide unpatching hook.
        # TODO: Use `with table_kwargs(...)`.
        from cratedb_toolkit.util.pandas import patch_pandas_sqltable_with_dialect_parameters

        patch_pandas_sqltable_with_dialect_parameters(table_kwargs={"crate_column_policy": "'dynamic'"})
        patch_pandas_sqltable_with_extended_mapping()

    def activate(self):
        """
        Swap in the MongoDB -> CrateDB adapter, by patching functions in PyMongo.
        """
        for patch_ in self.patches:
            patch_.start()

    def deactivate(self):
        """
        Swap out the MongoDB -> CrateDB adapter, by restoring patched functions.
        """
        for patch_ in self.patches:
            patch_.stop()
