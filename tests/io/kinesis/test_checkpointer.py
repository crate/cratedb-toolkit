import asyncio
from pathlib import Path

import pytest
import sqlalchemy as sa

pytestmark = pytest.mark.kinesis

pytest.importorskip("kinesis", reason="Only works with async-kinesis installed")
pytest.importorskip("commons_codec", reason="Only works with commons-codec installed")

from cratedb_toolkit.io.kinesis.checkpointer import CrateDBCheckPointer, _validate_identifier  # noqa: E402
from cratedb_toolkit.io.kinesis.model import RecipeDefinition  # noqa: E402
from cratedb_toolkit.io.kinesis.relay import KinesisRelay  # noqa: E402
from tests.conftest import TESTDRIVE_EXT_SCHEMA  # noqa: E402
from tests.io.kinesis.data import DMS_CDC_CREATE_TABLE, DMS_CDC_INSERT_BASIC, DMS_CDC_INSERT_SECOND  # noqa: E402
from tests.io.test_awslambda import wrap_kinesis  # noqa: E402

TRANSFORMATION_FILE = Path("./examples/cdc/aws/dms-load-schema-universal.yaml")


# -- Unit tests: CrateDB container only, no LocalStack --


def test_cratedb_checkpointer_table_creation(cratedb):
    engine = sa.create_engine(cratedb.get_connection_url())
    try:
        CrateDBCheckPointer(engine=engine, name="test_table_creation", schema=TESTDRIVE_EXT_SCHEMA)

        # Table should exist.
        with engine.connect() as conn:
            conn.execute(sa.text(f'REFRESH TABLE "{TESTDRIVE_EXT_SCHEMA}"."kinesis_checkpoints"'))
            result = conn.execute(
                sa.text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = :schema AND table_name = :table"
                ),
                {"schema": TESTDRIVE_EXT_SCHEMA, "table": "kinesis_checkpoints"},
            ).scalar()
        assert result == 1
    finally:
        engine.dispose()


def test_cratedb_checkpointer_first_run(cratedb):
    engine = sa.create_engine(cratedb.get_connection_url())
    try:
        cp = CrateDBCheckPointer(engine=engine, name="test_first_run", schema=TESTDRIVE_EXT_SCHEMA)

        success, sequence = asyncio.run(cp.allocate("shardId-000000000000"))
        assert success is True
        assert sequence is None
    finally:
        engine.dispose()


def test_cratedb_checkpointer_lifecycle(cratedb):
    engine = sa.create_engine(cratedb.get_connection_url())
    try:
        cp = CrateDBCheckPointer(engine=engine, name="test_lifecycle", schema=TESTDRIVE_EXT_SCHEMA)

        # Allocate shard (fresh).
        success, sequence = asyncio.run(cp.allocate("shardId-000000000000"))
        assert success is True
        assert sequence is None

        # Checkpoint at sequence 100.
        asyncio.run(cp.checkpoint("shardId-000000000000", "100"))

        # Deallocate.
        asyncio.run(cp.deallocate("shardId-000000000000"))

        # Re-allocate: should resume from sequence 100.
        cp2 = CrateDBCheckPointer(engine=engine, name="test_lifecycle", schema=TESTDRIVE_EXT_SCHEMA)
        success, sequence = asyncio.run(cp2.allocate("shardId-000000000000"))
        assert success is True
        assert sequence == "100"
    finally:
        engine.dispose()


def test_cratedb_checkpointer_crash_recovery(cratedb):
    engine = sa.create_engine(cratedb.get_connection_url())
    try:
        cp = CrateDBCheckPointer(engine=engine, name="test_crash", schema=TESTDRIVE_EXT_SCHEMA)

        # Simulate a crash: allocate + checkpoint, but never deallocate.
        asyncio.run(cp.allocate("shardId-000000000000"))
        asyncio.run(cp.checkpoint("shardId-000000000000", "42"))

        # New consumer should still be able to allocate (single-consumer model).
        cp2 = CrateDBCheckPointer(engine=engine, name="test_crash", schema=TESTDRIVE_EXT_SCHEMA)
        success, sequence = asyncio.run(cp2.allocate("shardId-000000000000"))
        assert success is True
        assert sequence == "42"
    finally:
        engine.dispose()


def test_cratedb_checkpointer_namespace_isolation(cratedb):
    engine = sa.create_engine(cratedb.get_connection_url())
    try:
        cp_a = CrateDBCheckPointer(engine=engine, name="stream_a", schema=TESTDRIVE_EXT_SCHEMA)
        cp_b = CrateDBCheckPointer(engine=engine, name="stream_b", schema=TESTDRIVE_EXT_SCHEMA)

        shard_id = "shardId-000000000000"

        asyncio.run(cp_a.allocate(shard_id))
        asyncio.run(cp_a.checkpoint(shard_id, "100"))

        asyncio.run(cp_b.allocate(shard_id))
        asyncio.run(cp_b.checkpoint(shard_id, "999"))

        # Each namespace should have its own checkpoint.
        assert cp_a.get_all_checkpoints() == {shard_id: "100"}
        assert cp_b.get_all_checkpoints() == {shard_id: "999"}
    finally:
        engine.dispose()


def test_cratedb_checkpointer_monotonic_guard(cratedb):
    engine = sa.create_engine(cratedb.get_connection_url())
    try:
        cp = CrateDBCheckPointer(engine=engine, name="test_monotonic", schema=TESTDRIVE_EXT_SCHEMA)

        asyncio.run(cp.allocate("shardId-000000000000"))
        asyncio.run(cp.checkpoint("shardId-000000000000", "100"))

        # Attempt to checkpoint a lower sequence.
        asyncio.run(cp.checkpoint("shardId-000000000000", "50"))

        # Persisted sequence should still be 100.
        checkpoints = cp.get_all_checkpoints()
        assert checkpoints["shardId-000000000000"] == "100"
    finally:
        engine.dispose()


def test_cratedb_checkpointer_schema_validation():
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        _validate_identifier("DROP TABLE; --")

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        _validate_identifier("has space")

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        _validate_identifier("")

    # Valid identifiers should not raise.
    _validate_identifier("ext")
    _validate_identifier("my_schema")
    _validate_identifier("_private")
    _validate_identifier("Schema123")
    _validate_identifier("testdrive-ext")


def test_cratedb_checkpointer_close(cratedb):
    engine = sa.create_engine(cratedb.get_connection_url())
    try:
        cp = CrateDBCheckPointer(engine=engine, name="test_close", schema=TESTDRIVE_EXT_SCHEMA)

        asyncio.run(cp.allocate("shardId-000000000000"))
        asyncio.run(cp.allocate("shardId-000000000001"))
        asyncio.run(cp.checkpoint("shardId-000000000000", "10"))
        asyncio.run(cp.checkpoint("shardId-000000000001", "20"))

        asyncio.run(cp.close())

        # In-memory allocation should be cleared.
        assert not cp.is_allocated("shardId-000000000000")
        assert not cp.is_allocated("shardId-000000000001")

        # Both shards should now be inactive in the database.
        with engine.connect() as conn:
            conn.execute(sa.text(f'REFRESH TABLE "{TESTDRIVE_EXT_SCHEMA}"."kinesis_checkpoints"'))
            rows = conn.execute(
                sa.text(
                    f'SELECT "shard_id", "active" FROM "{TESTDRIVE_EXT_SCHEMA}"."kinesis_checkpoints" '  # noqa: S608
                    'WHERE "namespace" = :ns ORDER BY "shard_id"'
                ),
                {"ns": "test_close"},
            ).fetchall()
        assert len(rows) == 2
        assert all(row[1] is False for row in rows)
    finally:
        engine.dispose()


def test_cratedb_checkpointer_get_all_checkpoints(cratedb):
    engine = sa.create_engine(cratedb.get_connection_url())
    try:
        cp = CrateDBCheckPointer(engine=engine, name="test_get_all", schema=TESTDRIVE_EXT_SCHEMA)

        asyncio.run(cp.allocate("shardId-000000000000"))
        asyncio.run(cp.allocate("shardId-000000000001"))
        asyncio.run(cp.checkpoint("shardId-000000000000", "100"))
        asyncio.run(cp.checkpoint("shardId-000000000001", "200"))

        checkpoints = cp.get_all_checkpoints()
        assert checkpoints == {
            "shardId-000000000000": "100",
            "shardId-000000000001": "200",
        }
    finally:
        engine.dispose()


# -- Integration tests: CrateDB + LocalStack --


def test_checkpointer_url_param_wiring(cratedb, kinesis):
    kinesis_url = (
        f"{kinesis.get_connection_url_kinesis()}"
        f"?region=us-east-1&create=true&buffer-time=0.01&idle-sleep=0.01"
        f"&checkpointer=cratedb&checkpointer-name=my_stream&checkpointer-interval=10"
    )
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    recipe = RecipeDefinition.from_yaml(TRANSFORMATION_FILE.read_text())
    relay = KinesisRelay(kinesis_url=kinesis_url, cratedb_url=cratedb_url, recipe=recipe)

    # Verify the parsed params on the adapter.
    adapter = relay.kinesis_adapter
    assert adapter.checkpointer_type == "cratedb"
    assert adapter.checkpointer_name == "my_stream"
    assert adapter.checkpoint_interval == 10.0


def test_no_checkpointer_regression_guard(caplog, cratedb, kinesis):
    kinesis_url = (
        f"{kinesis.get_connection_url_kinesis()}?region=us-east-1&create=true&buffer-time=0.01&idle-sleep=0.01"
    )
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    events = [
        wrap_kinesis(DMS_CDC_CREATE_TABLE),
        wrap_kinesis(DMS_CDC_INSERT_BASIC),
    ]

    recipe = RecipeDefinition.from_yaml(TRANSFORMATION_FILE.read_text())
    relay = KinesisRelay(kinesis_url=kinesis_url, cratedb_url=cratedb_url, recipe=recipe)

    for event in events:
        relay.kinesis_adapter.produce(event)

    relay.start(once=True)

    # Data should still be processed normally.
    table_name = '"testdrive"."demo"'
    cratedb.database.refresh_table(table_name)
    assert cratedb.database.count_records(table_name) == 1

    # Warning about missing checkpointer should be logged.
    assert "No persistent checkpointer configured" in caplog.text


def test_memory_checkpointer_integration(cratedb, kinesis):
    kinesis_url = (
        f"{kinesis.get_connection_url_kinesis()}"
        f"?region=us-east-1&create=true&buffer-time=0.01&idle-sleep=0.01"
        f"&checkpointer=memory"
    )
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    events = [
        wrap_kinesis(DMS_CDC_CREATE_TABLE),
        wrap_kinesis(DMS_CDC_INSERT_BASIC),
    ]

    recipe = RecipeDefinition.from_yaml(TRANSFORMATION_FILE.read_text())
    relay = KinesisRelay(kinesis_url=kinesis_url, cratedb_url=cratedb_url, recipe=recipe)

    for event in events:
        relay.kinesis_adapter.produce(event)

    relay.start(once=True)

    table_name = '"testdrive"."demo"'
    cratedb.database.refresh_table(table_name)
    assert cratedb.database.count_records(table_name) == 1


def test_cratedb_checkpointer_integration(cratedb, kinesis):
    kinesis_url = (
        f"{kinesis.get_connection_url_kinesis()}"
        f"?region=us-east-1&create=true&buffer-time=0.01&idle-sleep=0.01"
        f"&checkpointer=cratedb&checkpointer-interval=0.1&checkpointer-schema={TESTDRIVE_EXT_SCHEMA}"
    )
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive"

    events = [
        wrap_kinesis(DMS_CDC_CREATE_TABLE),
        wrap_kinesis(DMS_CDC_INSERT_BASIC),
        wrap_kinesis(DMS_CDC_INSERT_SECOND),
    ]

    recipe = RecipeDefinition.from_yaml(TRANSFORMATION_FILE.read_text())
    relay = KinesisRelay(kinesis_url=kinesis_url, cratedb_url=cratedb_url, recipe=recipe)

    for event in events:
        relay.kinesis_adapter.produce(event)

    relay.start(once=True)

    # Both records should be in CrateDB.
    table_name = '"testdrive"."demo"'
    cratedb.database.refresh_table(table_name)
    assert cratedb.database.count_records(table_name) == 2

    # Checkpoint state should exist in CrateDB.
    engine = sa.create_engine(cratedb.get_connection_url())
    try:
        with engine.connect() as conn:
            conn.execute(sa.text(f'REFRESH TABLE "{TESTDRIVE_EXT_SCHEMA}"."kinesis_checkpoints"'))
            rows = conn.execute(
                sa.text(
                    f'SELECT "namespace", "shard_id", "sequence" '  # noqa: S608
                    f'FROM "{TESTDRIVE_EXT_SCHEMA}"."kinesis_checkpoints"'
                )
            ).fetchall()

        # At least one checkpoint row should exist.
        assert len(rows) >= 1
    finally:
        engine.dispose()
