# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
from importlib.resources import read_text

from cratedb_rollup.util.database import run_sql


def setup_schema(dburi: str):
    """
    Set up `retention_policies` table schema.
    """

    # Read SQL DDL statement.
    sql = read_text("cratedb_rollup.setup", "schema.sql")

    # Materialize table schema.
    run_sql(dburi, sql)
