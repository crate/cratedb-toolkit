# Copyright (c) 2023-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging

import sqlalchemy as sa

from cratedb_toolkit.materialized.model import MaterializedViewSettings
from cratedb_toolkit.materialized.store import MaterializedViewStore
from cratedb_toolkit.model import TableAddress

logger = logging.getLogger(__name__)


class MaterializedViewManager:
    """
    The main application, implementing basic synthetic materialized views.
    """

    def __init__(self, settings: MaterializedViewSettings):
        # Runtime context settings.
        self.settings = settings

        # Materialized store API.
        self.store = MaterializedViewStore(settings=self.settings)

    def refresh(self, name: str):
        """
        Resolve a materialized view and refresh it.
        """
        logger.info(f"Refreshing materialized view: {name}")

        table_address = TableAddress.from_string(name)
        mview = self.store.get_by_table(table_address)
        logger.info(f"Loaded materialized view definition: {mview}")

        sql_ddl = f"DROP TABLE IF EXISTS {mview.staging_table_fullname}"
        logger.info(f"Dropping materialized view (staging): {sql_ddl}")
        self.store.execute(sa.text(sql_ddl))

        sql_ddl = f"CREATE TABLE IF NOT EXISTS {mview.staging_table_fullname} AS (\n{mview.sql}\n)"
        logger.info(f"Creating materialized view (staging): {sql_ddl}")
        self.store.execute(sa.text(sql_ddl))
        sql_refresh = f"REFRESH TABLE {mview.staging_table_fullname}"
        self.store.execute(sa.text(sql_refresh))

        sql_ddl = f"CREATE TABLE IF NOT EXISTS {mview.table_fullname} (dummy INT)"
        logger.info(f"Creating materialized view (live): {sql_ddl}")
        self.store.execute(sa.text(sql_ddl))

        # TODO: Use `ALTER TABLE ... RENAME TO ...` after resolving issue.
        #       SQLParseException[Target table name must not include a schema]
        #       https://github.com/crate/crate/issues/14833
        #       CrateDB does not support renaming to a different schema, thus the target
        #       table identifier must not include a schema. This is an artificial limitation.
        #       Technically, it can be done.
        #       https://github.com/crate/crate/blob/5.3.3/server/src/main/java/io/crate/analyze/AlterTableAnalyzer.java#L97-L102
        sql_ddl = f"ALTER CLUSTER SWAP TABLE {mview.staging_table_fullname} TO {mview.table_fullname}"
        logger.info(f"Activating materialized view: {sql_ddl}")
        self.store.execute(sa.text(sql_ddl))
        sql_refresh = f"REFRESH TABLE {mview.table_fullname}"
        self.store.execute(sa.text(sql_refresh))

        sql_ddl = f"DROP TABLE IF EXISTS {mview.staging_table_fullname}"
        logger.info(f"Dropping materialized view (staging): {sql_ddl}")
        self.store.execute(sa.text(sql_ddl))
