# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
from importlib.resources import read_text

from cratedb_retention.model import JobSettings
from cratedb_retention.util.database import run_sql

logger = logging.getLogger(__name__)


def setup_schema(settings: JobSettings):
    """
    Set up the retention policy table schema.
    """

    logger.info(
        f"Installing retention policy bookkeeping table at "
        f"database '{settings.database.safe}', table {settings.policy_table}"
    )

    # Read SQL DDL statement.
    sql = read_text("cratedb_retention.setup", "schema.sql")

    tplvars = settings.to_dict()
    sql = sql.format_map(tplvars)

    # Materialize table schema.
    run_sql(settings.database.dburi, sql)
