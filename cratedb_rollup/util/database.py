# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import sqlalchemy as sa


def run_sql(dburi: str, sql: str):
    """
    Connect to database, invoke SQL statement, and return results.
    Run SQL statement
    """
    sa_engine = sa.create_engine(dburi, echo=False)
    with sa_engine.connect() as conn:
        result = conn.execute(sa.text(sql))
        return result.fetchall()
