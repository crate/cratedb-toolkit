# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
from importlib.resources import read_text

from cratedb_toolkit.materialized.model import MaterializedViewSettings
from cratedb_toolkit.util.database import run_sql

logger = logging.getLogger(__name__)


def setup_schema(settings: MaterializedViewSettings):
    """
    Set up the materialized view management table schema.

    TODO: Refactor to `store` module.
    """

    logger.info(
        f"Installing materialized view management table at "
        f"database '{settings.database.safe}', table {settings.materialized_table}"
    )

    # Read SQL DDL statement.
    sql = read_text("cratedb_toolkit.materialized", "schema.sql")

    tplvars = settings.to_dict()
    sql = sql.format_map(tplvars)

    if settings.dry_run:
        logger.info(f"Pretending to execute SQL statement:\n{sql}")
        return

    # Materialize table schema.
    run_sql(settings.database.dburi, sql)
    run_sql(settings.database.dburi, f"REFRESH TABLE {settings.materialized_table.fullname}")
