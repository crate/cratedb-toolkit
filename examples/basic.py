# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
Example program demonstrating how to set up a data retention policy,
and invoke it.

It initializes the data retention and expiry subsystem, and creates
a data retention policy. After that, it invokes the corresponding
data retention job.

The program obtains a single positional argument from the command line,
the database URI, in SQLAlchemy-compatible string format.
"""
import sys

from sqlalchemy.exc import ProgrammingError

from cratedb_retention.core import RetentionJob
from cratedb_retention.model import DatabaseAddress, JobSettings, RetentionStrategy
from cratedb_retention.setup.schema import setup_schema
from cratedb_retention.util.common import setup_logging
from cratedb_retention.util.database import run_sql


def setup_retention(dburi: str):
    """
    How to initialize the subsystem, and create a data retention policy.
    """
    # Create `JobSettings` instance.
    # It is the single source of truth about configuration and runtime settings.
    settings = JobSettings(
        database=DatabaseAddress.from_string(dburi),
    )
    setup_schema(settings=settings)

    # TODO: All values are currently hardcoded.
    sql = """
    -- A policy using the DELETE strategy.
    INSERT INTO "ext"."retention_policy"
      (strategy, table_schema, table_name, partition_column, retention_period)
    VALUES
      ('delete', 'doc', 'raw_metrics', 'ts_day', 1);
    """
    try:
        run_sql(dburi, sql)
    except ProgrammingError as ex:
        if "DuplicateKeyException" not in str(ex):
            raise


def run_retention(dburi: str, strategy: str, cutoff_day: str):
    """
    How to invoke a data retention job.
    """
    # Create `JobSettings` instance.
    # It is the single source of truth about configuration and runtime settings.
    settings = JobSettings(
        database=DatabaseAddress.from_string(dburi),
        strategy=RetentionStrategy(strategy.upper()),
        cutoff_day=cutoff_day,
    )

    # Invoke the data retention job.
    job = RetentionJob(settings=settings)
    job.start()


def main(dburi: str):
    """
    Set up a data retention policy, and invoke the corresponding job.
    """
    setup_retention(dburi)
    run_retention(
        dburi=dburi,
        strategy="delete",
        cutoff_day="2023-06-27",
    )


if __name__ == "__main__":
    """
    The program obtains a single positional argument from the command line,
    the database URI, in SQLAlchemy-compatible string format.
    """
    setup_logging()
    try:
        dburi = sys.argv[1]
    except IndexError:
        dburi = "crate://localhost/"
    main(dburi)
