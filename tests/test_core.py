# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import datetime

import pytest

from cratedb_retention.core import RetentionJob
from cratedb_retention.model import RetentionPolicy, RetentionStrategy
from tests.conftest import TESTDRIVE_DATA_SCHEMA


def test_no_strategy_given(store, settings):
    """
    Verify the engine reports correctly when invoked without retention strategy.
    """
    job = RetentionJob(settings=settings)
    with pytest.raises(ValueError) as ex:
        job.start()
    assert ex.match("Unable to load retention policies without strategy")


def test_data_table_not_found(caplog, store, settings):
    """
    When a retention policy is invoked, but the corresponding data table
    does not exist, the program should report about it.
    """

    assert store.retrieve() == []

    # Add a retention policy.
    policy = RetentionPolicy(
        strategy=RetentionStrategy.DELETE,
        table_schema=TESTDRIVE_DATA_SCHEMA,
        table_name="foobar",
        partition_column="ts_day",
        retention_period=1,
    )
    store.create(policy, ignore="DuplicateKeyException")

    # Invoke the data retention job.
    settings.strategy = RetentionStrategy.DELETE
    job = RetentionJob(settings=settings)
    job.start()

    assert f'Data table not found: "{TESTDRIVE_DATA_SCHEMA}"."foobar"' in caplog.messages


def test_no_cutoff_date_given(caplog, store, settings):
    """
    Verify the engine uses default=today() when invoked without cutoff date.
    """
    # Add a retention policy.
    policy = RetentionPolicy(
        strategy=RetentionStrategy.DELETE,
        table_schema=TESTDRIVE_DATA_SCHEMA,
        table_name="foobar",
        partition_column="ts_day",
        retention_period=1,
    )
    store.create(policy, ignore="DuplicateKeyException")

    # Create the data table. It's ok if it is empty.
    store.database.run_sql(f'CREATE TABLE "{TESTDRIVE_DATA_SCHEMA}"."foobar" (foo INTEGER, ts_day TIMESTAMPTZ);')

    # Invoke the data retention job.
    settings.strategy = RetentionStrategy.DELETE
    job = RetentionJob(settings=settings)
    job.start()

    today = datetime.date.today().isoformat()
    assert f"No cutoff date selected, will use today(): {today}" in caplog.messages


def test_no_data_to_be_retired(caplog, store, settings):
    """
    When a retention policy is invoked, but not data is to be retired from
    the corresponding data table, the program should report about it.
    """

    assert store.retrieve() == []

    # Add a retention policy.
    policy = RetentionPolicy(
        strategy=RetentionStrategy.DELETE,
        table_schema=TESTDRIVE_DATA_SCHEMA,
        table_name="foobar",
        partition_column="ts_day",
        retention_period=1,
    )
    store.create(policy, ignore="DuplicateKeyException")

    # Create the data table. It's ok if it is empty.
    store.database.run_sql(f'CREATE TABLE "{TESTDRIVE_DATA_SCHEMA}"."foobar" (foo INTEGER, ts_day TIMESTAMPTZ);')

    # Invoke the data retention job.
    settings.strategy = RetentionStrategy.DELETE
    settings.cutoff_day = "1990-01-01"
    job = RetentionJob(settings=settings)
    job.start()

    assert f'No data to be retired from data table: "{TESTDRIVE_DATA_SCHEMA}"."foobar"' in caplog.messages
