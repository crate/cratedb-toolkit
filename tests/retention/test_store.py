# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import pytest

from cratedb_toolkit.retention.model import RetentionPolicy, RetentionStrategy


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


def test_create_exists(store):
    """
    Verify that only one retention policy can be created for a specific table.
    When attempting to create multiple policies per table, the program fails.
    """
    # Add a retention policy.
    policy = RetentionPolicy(
        strategy=RetentionStrategy.DELETE,
        tags={"foo", "bar"},
        table_schema="doc",
        table_name="raw_metrics",
        partition_column="ts_day",
        retention_period=1,
    )
    store.create(policy)
    with pytest.raises(ValueError) as ex:
        store.create(policy)
    assert ex.match("Retention policy for table 'doc.raw_metrics' already exists")


def test_list_tags(store):
    """
    Verify `list-tags` subcommand.
    """

    # Add a retention policy.
    policy = RetentionPolicy(
        strategy=RetentionStrategy.DELETE,
        tags={"foo", "bar"},
        table_schema="doc",
        table_name="raw_metrics",
        partition_column="ts_day",
        retention_period=1,
    )
    store.create(policy, ignore="DuplicateKeyException")

    tags = store.retrieve_tags()
    assert tags == ["bar", "foo"]


def test_delete_by_tag(store, needs_sqlalchemy2):
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
            table_name="foo",
            partition_column="ts_day",
            retention_period=1,
        ),
        RetentionPolicy(
            strategy=RetentionStrategy.DELETE,
            tags={"bar"},
            table_schema="doc",
            table_name="bar",
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


def test_delete_by_all_tags(store, needs_sqlalchemy2):
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
            table_name="foo",
            partition_column="ts_day",
            retention_period=1,
        ),
        RetentionPolicy(
            strategy=RetentionStrategy.DELETE,
            tags={"bar"},
            table_schema="doc",
            table_name="bar",
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


def test_delete_unknown(caplog, store):
    """
    Verify behavior when deleting an unknown item by identifier.
    """

    caplog.clear()
    rowcount = store.delete(identifier=None)
    assert rowcount == 0
    assert "Retention policy not found with id: None" in caplog.messages

    caplog.clear()
    rowcount = store.delete(identifier="unknown")
    assert rowcount == 0
    assert "Retention policy not found with id: unknown" in caplog.messages


def test_delete_by_tag_unknown(caplog, store, needs_sqlalchemy2):
    """
    Verify behavior when deleting items by unknown tag
    """

    caplog.clear()
    rowcount = store.delete_by_tag(None)
    assert rowcount == 0
    assert "No retention policies found with tags: [None]" in caplog.messages

    caplog.clear()
    rowcount = store.delete_by_tag("unknown")
    assert rowcount == 0
    assert "No retention policies found with tags: ['unknown']" in caplog.messages


def test_delete_by_all_tags_unknown(caplog, store, needs_sqlalchemy2):
    """
    Verify behavior when deleting items by unknown tag
    """

    caplog.clear()
    rowcount = store.delete_by_all_tags(None)
    assert rowcount == 0
    assert "No tags obtained, skipping deletion" in caplog.messages

    caplog.clear()
    rowcount = store.delete_by_all_tags([])
    assert rowcount == 0
    assert "No tags obtained, skipping deletion" in caplog.messages

    caplog.clear()
    rowcount = store.delete_by_all_tags([None])
    assert rowcount == 0
    assert "No retention policies found with tags: [None]" in caplog.messages

    caplog.clear()
    rowcount = store.delete_by_all_tags(["unknown"])
    assert rowcount == 0
    assert "No retention policies found with tags: ['unknown']" in caplog.messages
