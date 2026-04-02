# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
Tests for Kinesis checkpoint maintenance operations.

Placed outside ``tests/io/kinesis/`` to avoid the ``importorskip("kinesis")``
gate in that package's conftest. These tests only need CrateDB (via
testcontainer), not LocalStack or async-kinesis.
"""

import typing as t
from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa

from cratedb_toolkit.exception import CheckpointTableNotFound
from cratedb_toolkit.io.kinesis.maintenance import (
    TABLE_NAME,
    list_checkpoints,
    parse_duration,
    prune_checkpoints,
)
from tests.conftest import TESTDRIVE_EXT_SCHEMA

pytestmark = pytest.mark.kinesis


def _create_checkpoint_table(engine: sa.Engine, schema: str) -> None:
    """Create the checkpoint table matching the CrateDBCheckPointer layout."""
    table = f'"{schema}"."{TABLE_NAME}"'
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            "namespace" TEXT NOT NULL,
            "shard_id"  TEXT NOT NULL,
            "sequence"  TEXT,
            "active"    BOOLEAN DEFAULT TRUE,
            "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY ("namespace", "shard_id")
        )
    """
    with engine.connect() as conn:
        conn.execute(sa.text(ddl))
        conn.commit()


def _insert_checkpoint(
    engine: sa.Engine,
    schema: str,
    namespace: str,
    shard_id: str,
    sequence: str,
    active: bool,
    updated_at: t.Optional[datetime] = None,
) -> None:
    """Insert a checkpoint row with an explicit updated_at timestamp."""
    table = f'"{schema}"."{TABLE_NAME}"'
    sql = sa.text(
        f'INSERT INTO {table} ("namespace", "shard_id", "sequence", "active", "updated_at") '  # noqa: S608
        "VALUES (:ns, :shard, :seq, :active, :ts)"
    )
    ts = updated_at or datetime.now(tz=timezone.utc)
    with engine.connect() as conn:
        conn.execute(sql, {"ns": namespace, "shard": shard_id, "seq": sequence, "active": active, "ts": ts})
        conn.commit()




# -- parse_duration tests (pure, no DB) --


def test_parse_duration_days():
    assert parse_duration("7d") == timedelta(days=7)


def test_parse_duration_hours():
    assert parse_duration("24h") == timedelta(hours=24)


def test_parse_duration_minutes():
    assert parse_duration("30m") == timedelta(minutes=30)


def test_parse_duration_seconds():
    assert parse_duration("90s") == timedelta(seconds=90)


def test_parse_duration_weeks():
    assert parse_duration("2w") == timedelta(weeks=2)


def test_parse_duration_combined():
    assert parse_duration("1d12h") == timedelta(days=1, hours=12)


def test_parse_duration_full_combination():
    assert parse_duration("1w2d3h15m30s") == timedelta(weeks=1, days=2, hours=3, minutes=15, seconds=30)


def test_parse_duration_whitespace_stripped():
    assert parse_duration("  7d  ") == timedelta(days=7)


def test_parse_duration_invalid_format():
    with pytest.raises(ValueError, match="Invalid duration"):
        parse_duration("seven days")


def test_parse_duration_empty_string():
    with pytest.raises(ValueError, match="Invalid duration"):
        parse_duration("")


def test_parse_duration_zero():
    with pytest.raises(ValueError, match="must be positive"):
        parse_duration("0d")


# -- list_checkpoints tests --


def test_list_checkpoints_table_not_found(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    with pytest.raises(CheckpointTableNotFound, match="does not exist"):
        list_checkpoints(engine=engine, schema="nonexistent_schema")


def test_list_checkpoints_empty_table(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    _create_checkpoint_table(engine, TESTDRIVE_EXT_SCHEMA)
    rows = list_checkpoints(engine=engine, schema=TESTDRIVE_EXT_SCHEMA)
    assert rows == []


def test_list_checkpoints_all(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    _create_checkpoint_table(engine, TESTDRIVE_EXT_SCHEMA)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "stream_a", "shard-0", "100", True)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "stream_b", "shard-0", "200", False)

    rows = list_checkpoints(engine=engine, schema=TESTDRIVE_EXT_SCHEMA)
    assert len(rows) == 2
    namespaces = {r["namespace"] for r in rows}
    assert namespaces == {"stream_a", "stream_b"}


def test_list_checkpoints_filter_by_namespace(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    _create_checkpoint_table(engine, TESTDRIVE_EXT_SCHEMA)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "stream_a", "shard-0", "100", True)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "stream_b", "shard-0", "200", False)

    rows = list_checkpoints(engine=engine, schema=TESTDRIVE_EXT_SCHEMA, namespace="stream_a")
    assert len(rows) == 1
    assert rows[0]["namespace"] == "stream_a"


def test_list_checkpoints_result_keys(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    _create_checkpoint_table(engine, TESTDRIVE_EXT_SCHEMA)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "ns", "shard-0", "100", True)

    rows = list_checkpoints(engine=engine, schema=TESTDRIVE_EXT_SCHEMA)
    assert len(rows) == 1
    assert set(rows[0].keys()) == {"namespace", "shard_id", "sequence", "active", "updated_at"}


# -- prune_checkpoints tests --


def test_prune_checkpoints_table_not_found(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    with pytest.raises(CheckpointTableNotFound, match="does not exist"):
        prune_checkpoints(engine=engine, older_than="7d", schema="nonexistent_schema")


def test_prune_checkpoints_invalid_duration(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    with pytest.raises(ValueError, match="Invalid duration"):
        prune_checkpoints(engine=engine, older_than="abc", schema=TESTDRIVE_EXT_SCHEMA)


def test_prune_checkpoints_requires_filter(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    with pytest.raises(ValueError, match="At least one"):
        prune_checkpoints(engine=engine, schema=TESTDRIVE_EXT_SCHEMA)


def test_prune_checkpoints_inactive_old_rows(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    _create_checkpoint_table(engine, TESTDRIVE_EXT_SCHEMA)

    old_ts = datetime.now(tz=timezone.utc) - timedelta(days=10)
    recent_ts = datetime.now(tz=timezone.utc) - timedelta(hours=1)

    # Old inactive (should be pruned).
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "old_stream", "shard-0", "50", False, old_ts)
    # Old active (should NOT be pruned).
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "active_stream", "shard-0", "100", True, old_ts)
    # Recent inactive (should NOT be pruned).
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "recent_stream", "shard-0", "200", False, recent_ts)

    result = prune_checkpoints(engine=engine, older_than="7d", schema=TESTDRIVE_EXT_SCHEMA)
    assert result["deleted"] == 1

    remaining = list_checkpoints(engine=engine, schema=TESTDRIVE_EXT_SCHEMA)
    remaining_ns = {r["namespace"] for r in remaining}
    assert remaining_ns == {"active_stream", "recent_stream"}


def test_prune_checkpoints_include_active(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    _create_checkpoint_table(engine, TESTDRIVE_EXT_SCHEMA)

    old_ts = datetime.now(tz=timezone.utc) - timedelta(days=10)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "dead_ns", "shard-0", "50", True, old_ts)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "dead_ns", "shard-1", "60", False, old_ts)

    result = prune_checkpoints(
        engine=engine,
        older_than="7d",
        namespace="dead_ns",
        schema=TESTDRIVE_EXT_SCHEMA,
        include_active=True,
    )
    assert result["deleted"] == 2

    remaining = list_checkpoints(engine=engine, schema=TESTDRIVE_EXT_SCHEMA)
    assert len(remaining) == 0


def test_prune_checkpoints_by_namespace_only(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    _create_checkpoint_table(engine, TESTDRIVE_EXT_SCHEMA)

    ts = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "keep_ns", "shard-0", "50", False, ts)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "remove_ns", "shard-0", "60", False, ts)

    result = prune_checkpoints(engine=engine, namespace="remove_ns", schema=TESTDRIVE_EXT_SCHEMA)
    assert result["deleted"] == 1

    remaining = list_checkpoints(engine=engine, schema=TESTDRIVE_EXT_SCHEMA)
    assert len(remaining) == 1
    assert remaining[0]["namespace"] == "keep_ns"


def test_prune_checkpoints_dry_run(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    _create_checkpoint_table(engine, TESTDRIVE_EXT_SCHEMA)

    old_ts = datetime.now(tz=timezone.utc) - timedelta(days=10)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "stale", "shard-0", "50", False, old_ts)

    result = prune_checkpoints(engine=engine, older_than="7d", schema=TESTDRIVE_EXT_SCHEMA, dry_run=True)
    assert result["would_delete"] == 1

    # Row should still exist.
    remaining = list_checkpoints(engine=engine, schema=TESTDRIVE_EXT_SCHEMA)
    assert len(remaining) == 1


def test_prune_checkpoints_with_namespace_filter(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    _create_checkpoint_table(engine, TESTDRIVE_EXT_SCHEMA)

    old_ts = datetime.now(tz=timezone.utc) - timedelta(days=10)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "keep_ns", "shard-0", "50", False, old_ts)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "prune_ns", "shard-0", "60", False, old_ts)

    result = prune_checkpoints(engine=engine, older_than="7d", schema=TESTDRIVE_EXT_SCHEMA, namespace="prune_ns")
    assert result["deleted"] == 1

    remaining = list_checkpoints(engine=engine, schema=TESTDRIVE_EXT_SCHEMA)
    assert len(remaining) == 1
    assert remaining[0]["namespace"] == "keep_ns"


def test_prune_checkpoints_nothing_to_delete(cratedb_synchronized):
    engine = cratedb_synchronized.database.engine
    _create_checkpoint_table(engine, TESTDRIVE_EXT_SCHEMA)

    recent_ts = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    _insert_checkpoint(engine, TESTDRIVE_EXT_SCHEMA, "recent", "shard-0", "100", False, recent_ts)

    result = prune_checkpoints(engine=engine, older_than="7d", schema=TESTDRIVE_EXT_SCHEMA)
    assert result["deleted"] == 0
