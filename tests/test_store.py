# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
from cratedb_retention.model import RetentionPolicy, RetentionStrategy


def test_create_retrieve_delete(store):
    """
    Verify creating and deleting a retention policy record.
    """

    assert store.retrieve() == []

    # Add a retention policy.
    policy = RetentionPolicy(
        strategy=RetentionStrategy.DELETE,
        tags={"foo", "bar"},
        table_schema="doc",
        table_name="raw_metrics",
        partition_column="ts_day",
        retention_period=1,
    )
    identifier = store.create(policy, ignore="DuplicateKeyException")

    # Verify it has been stored within the database.
    record = store.retrieve()[0]
    del record["id"]
    assert record == {
        "strategy": "delete",
        "tags": ["bar", "foo"],
        "table_schema": "doc",
        "table_name": "raw_metrics",
        "partition_column": "ts_day",
        "retention_period": 1,
        "reallocation_attribute_name": None,
        "reallocation_attribute_value": None,
        "target_repository_name": None,
    }

    # Invoke delete operation, and verify the relevant items have been purged.
    rowcount = store.delete(identifier=identifier)
    assert rowcount == 1
    assert store.retrieve() == []


def test_delete_by_tag(store):
    """
    Verify deleting a retention policy by single tag.
    """

    assert store.retrieve() == []

    # Add retention policies, using different tags.
    policies = [
        RetentionPolicy(
            strategy=RetentionStrategy.DELETE,
            tags={"foo", "bar"},
            table_schema="doc",
            table_name="raw_metrics",
            partition_column="ts_day",
            retention_period=1,
        ),
        RetentionPolicy(
            strategy=RetentionStrategy.DELETE,
            tags={"bar"},
            table_schema="doc",
            table_name="raw_metrics",
            partition_column="ts_day",
            retention_period=1,
        ),
    ]
    for policy in policies:
        store.create(policy, ignore="DuplicateKeyException")

    # Verify items have been stored within the database.
    assert len(store.retrieve()) == 2

    # Invoke delete operation, and verify the relevant items have been purged.
    rowcount = store.delete_by_tag("foo")
    assert rowcount == 1
    assert len(store.retrieve()) == 1


def test_delete_by_all_tags(store):
    """
    Verify deleting a retention policy by multiple tags.

    Tags are combined using SQL's `AND` operator.
    """

    assert store.retrieve() == []

    # Add retention policies, using different tags.
    policies = [
        RetentionPolicy(
            strategy=RetentionStrategy.DELETE,
            tags={"foo", "bar"},
            table_schema="doc",
            table_name="raw_metrics",
            partition_column="ts_day",
            retention_period=1,
        ),
        RetentionPolicy(
            strategy=RetentionStrategy.DELETE,
            tags={"bar"},
            table_schema="doc",
            table_name="raw_metrics",
            partition_column="ts_day",
            retention_period=1,
        ),
    ]
    for policy in policies:
        store.create(policy, ignore="DuplicateKeyException")

    # Verify items have been stored within the database.
    assert len(store.retrieve()) == 2

    # Invoke delete operation, and verify the relevant items have been purged.
    rowcount = store.delete_by_all_tags(["foo", "bar"])
    assert rowcount == 1
    assert len(store.retrieve()) == 1


def test_delete_unknown(store):
    """
    Verify behavior when deleting an unknown item by identifier.
    """
    rowcount = store.delete(identifier=None)
    assert rowcount == 0

    rowcount = store.delete(identifier="unknown")
    assert rowcount == 0


def test_delete_by_tag_unknown(store):
    """
    Verify behavior when deleting items by unknown tag
    """
    rowcount = store.delete_by_tag(None)
    assert rowcount == 0

    rowcount = store.delete_by_tag("unknown")
    assert rowcount == 0


def test_delete_by_all_tags_unknown(store):
    """
    Verify behavior when deleting items by unknown tag
    """
    rowcount = store.delete_by_all_tags(None)
    assert rowcount == 0

    rowcount = store.delete_by_all_tags([])
    assert rowcount == 0

    rowcount = store.delete_by_all_tags([None])
    assert rowcount == 0

    rowcount = store.delete_by_all_tags(["unknown"])
    assert rowcount == 0
