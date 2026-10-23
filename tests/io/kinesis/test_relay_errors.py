import asyncio
import sqlite3
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from commons_codec.model import SkipOperation, SQLOperation
from kinesis import Consumer
from kinesis.checkpointers import MemoryCheckPointer
from sqlalchemy.exc import OperationalError, ProgrammingError

from cratedb_toolkit.io.kinesis.relay import KinesisRelay

pytestmark = pytest.mark.kinesis


@pytest.fixture
def relay_factory(mocker):
    mocker.patch("cratedb_toolkit.io.kinesis.relay.KinesisAdapterBase.factory")
    mocker.patch("cratedb_toolkit.io.kinesis.relay.DatabaseAdapter")

    def factory(**kwargs):
        relay = KinesisRelay("kinesis+dms://localhost/demo", "crate://localhost/testdrive/demo", **kwargs)
        relay.connection = MagicMock()
        relay.progress_bar = MagicMock()
        relay.translator = MagicMock()
        relay.translator.to_sql.return_value = SQLOperation(statement="SELECT :id", parameters={"id": 1})
        return relay

    return factory


@pytest.mark.parametrize("error_type", [ProgrammingError, OperationalError])
@pytest.mark.parametrize("policy", [None, "ignore", "raise"])
@pytest.mark.parametrize("stage", ["execute", "refresh", "commit"])
def test_kinesis_relay_write_error_policy(relay_factory, caplog, policy, stage, error_type):
    relay = relay_factory(**({"on_error": policy} if policy is not None else {}))
    error = error_type("query", {}, Exception("write failed"))
    if stage == "commit":
        relay.connection.commit.side_effect = [error, None]
    elif stage == "refresh":
        relay.cratedb_table = '"testdrive"."demo"'
        relay.connection.execute.side_effect = [None, error, None, None]
    else:
        relay.connection.execute.side_effect = [error, None]

    if policy != "ignore":
        with pytest.raises(error_type) as raised:
            relay.process_event({"id": "bad"})
        assert raised.value is error
        relay.connection.rollback.assert_not_called()
        relay.progress_bar.update.assert_not_called()
    else:
        relay.process_event({"id": "bad"})
        relay.progress_bar.update.assert_not_called()
        relay.connection.rollback.assert_called_once_with()
        relay.process_event({"id": "good"})
        relay.progress_bar.update.assert_called_once_with()
        assert relay.translator.to_sql.call_args.args[0]["id"] == "good"
        assert relay.connection.commit.call_count == (2 if stage == "commit" else 1)
    assert "Executing query failed" in caplog.text


def test_kinesis_relay_invalid_policy(mocker):
    mocker.patch("cratedb_toolkit.io.kinesis.relay.KinesisAdapterBase.factory")
    mocker.patch("cratedb_toolkit.io.kinesis.relay.DatabaseAdapter")
    with pytest.raises(ValueError, match="Unknown 'on_error' value: typo"):
        KinesisRelay("kinesis+dms://localhost/demo", "crate://localhost/testdrive/demo", on_error="typo")  # ty: ignore[invalid-argument-type]


def test_kinesis_relay_unrelated_write_error(relay_factory):
    relay = relay_factory(on_error="ignore")
    relay.connection.execute.side_effect = RuntimeError("unrelated error")
    with pytest.raises(RuntimeError, match="unrelated error"):
        relay.process_event({"id": "bad"})
    relay.connection.rollback.assert_not_called()


def test_kinesis_relay_setup_error_propagates(relay_factory):
    relay = relay_factory(on_error="ignore")
    relay.cratedb_table = '"testdrive"."demo"'
    relay.translator.sql_ddl = "CREATE TABLE demo (id INTEGER)"
    relay.cratedb_adapter.table_exists.return_value = False
    relay.cratedb_adapter.engine.connect.return_value = relay.connection
    relay.connection.execute.side_effect = OperationalError("CREATE TABLE", {}, Exception("setup failed"))
    with pytest.raises(OperationalError, match="setup failed"):
        relay.start(once=True)
    relay.kinesis_adapter.consume_once.assert_not_called()
    assert not hasattr(relay, "connection")


@pytest.mark.parametrize("stage", ["decode", "translation", "skip"])
def test_kinesis_relay_skips_invalid_event(relay_factory, stage):
    relay = relay_factory(on_error="raise")
    event = {"id": "bad"}
    if stage == "decode":
        event = {"kinesis": {"data": "invalid"}}
    else:
        relay.translator.to_sql.side_effect = SkipOperation() if stage == "skip" else ValueError("invalid")
    relay.process_event(event)
    relay.connection.execute.assert_not_called()
    relay.progress_bar.update.assert_not_called()


def test_kinesis_relay_recovers_after_commit_error(relay_factory, monkeypatch):
    relay = relay_factory(on_error="ignore")
    engine = sa.create_engine("sqlite://")
    with engine.connect() as connection:
        relay.connection = connection
        original_commit = engine.dialect.do_commit

        def fail_commit_once(dbapi_connection):
            monkeypatch.setattr(engine.dialect, "do_commit", original_commit)
            raise sqlite3.OperationalError("commit failed")

        monkeypatch.setattr(engine.dialect, "do_commit", fail_commit_once)
        relay.process_event({"id": "bad"})
        assert not connection.in_transaction()
        relay.process_event({"id": "good"})
        relay.progress_bar.update.assert_called_once_with()
    engine.dispose()


@pytest.mark.parametrize("policy", ["ignore", "raise"])
def test_kinesis_relay_checkpoint_after_write_error(relay_factory, policy):
    relay = relay_factory(on_error=policy)
    relay.connection.execute.side_effect = [OperationalError("query", {}, Exception("write failed")), None]

    async def consume():
        checkpointer = MemoryCheckPointer(name="on-error")
        consumer = Consumer(stream_name="demo", checkpointer=checkpointer, idle_timeout=0.01)  # ty: ignore[invalid-argument-type]
        consumer.shards = [{"ShardId": "shard-0"}]
        await checkpointer.allocate("shard-0")
        # Exercise the real consumer queue and checkpoint path without a network fetcher.
        consumer.fetch_task = asyncio.create_task(asyncio.sleep(60))
        await consumer.queue.put({"id": "bad"})
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "1"}})
        await consumer.queue.put({"id": "good"})
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "2"}})
        try:
            if policy == "raise":
                with pytest.raises(OperationalError, match="write failed"):
                    async for event in consumer:
                        relay.process_event(event)
            else:
                async for event in consumer:
                    relay.process_event(event)
        finally:
            fetch_task = consumer.fetch_task
            await consumer.close()
            if fetch_task is not None:
                try:
                    await fetch_task
                except asyncio.CancelledError:
                    pass
        return checkpointer.get_all_checkpoints()

    checkpoints = asyncio.run(consume())
    assert checkpoints["shard-0"]["sequence"] == ("2" if policy == "ignore" else None)
