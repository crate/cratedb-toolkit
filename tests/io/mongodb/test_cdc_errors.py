from unittest.mock import MagicMock

import pytest
from commons_codec.model import SQLOperation
from sqlalchemy.exc import OperationalError, ProgrammingError

from cratedb_toolkit.io.mongodb.cdc import MongoDBCDCRelayCrateDB

pytestmark = pytest.mark.mongodb


@pytest.fixture
def cdc_relay(mocker):
    mocker.patch("cratedb_toolkit.io.mongodb.cdc.mongodb_adapter_factory")
    adapter = mocker.patch("cratedb_toolkit.io.mongodb.cdc.DatabaseAdapter").return_value
    adapter.quote_relation_name.return_value = '"testdrive"."demo"'
    relay = MongoDBCDCRelayCrateDB("mongodb://localhost/testdrive/demo", "crate://localhost/testdrive/demo", tm=None)
    relay.cdc = MagicMock(sql_ddl="CREATE TABLE demo (id TEXT)")
    relay.cdc.to_sql.side_effect = [
        SQLOperation(statement="INSERT INTO demo (id) VALUES (:id)", parameters={"id": "bad"}),
        SQLOperation(statement="INSERT INTO demo (id) VALUES (:id)", parameters={"id": "good"}),
    ]
    mocker.patch.object(relay, "consume", return_value=iter([{"id": "bad"}, {"id": "good"}]))
    return relay


@pytest.mark.parametrize("error_type", [ProgrammingError, OperationalError])
@pytest.mark.parametrize("policy", [None, "ignore", "raise"])
def test_mongodb_cdc_write_error_policy(cdc_relay, caplog, policy, error_type):
    if policy is not None:
        cdc_relay.on_error = policy
    connection = cdc_relay.cratedb_adapter.engine.connect.return_value.__enter__.return_value
    error = error_type("INSERT", {}, Exception("write failed"))
    connection.execute.side_effect = [None, error, None]

    if policy == "raise":
        with pytest.raises(error_type) as raised:
            cdc_relay.start()
        assert raised.value is error
        assert connection.execute.call_count == 2
        connection.rollback.assert_not_called()
    else:
        cdc_relay.start()
        assert connection.execute.call_count == 3
        assert connection.execute.call_args.args[1] == {"id": "good"}
        connection.rollback.assert_called_once_with()
    assert "Executing query failed" in caplog.text


@pytest.mark.parametrize("stage", ["ddl", "translation", "write"])
def test_mongodb_cdc_error_boundaries(cdc_relay, stage):
    connection = cdc_relay.cratedb_adapter.engine.connect.return_value.__enter__.return_value
    error = RuntimeError("unrelated error")
    if stage == "ddl":
        error = OperationalError("CREATE TABLE", {}, Exception("setup failed"))
        connection.execute.side_effect = error
    elif stage == "translation":
        cdc_relay.cdc.to_sql.side_effect = error
    else:
        connection.execute.side_effect = [None, error]
    with pytest.raises(type(error)) as raised:
        cdc_relay.start()
    assert raised.value is error
    connection.rollback.assert_not_called()


def test_mongodb_cdc_empty_operation(cdc_relay):
    cdc_relay.cdc.to_sql.side_effect = [None, None]
    cdc_relay.start()
    connection = cdc_relay.cratedb_adapter.engine.connect.return_value.__enter__.return_value
    assert connection.execute.call_count == 1


@pytest.mark.parametrize("policy", ["ignore", "raise"])
def test_mongodb_cdc_constructor_policy(mocker, policy):
    mocker.patch("cratedb_toolkit.io.mongodb.cdc.mongodb_adapter_factory")
    mocker.patch("cratedb_toolkit.io.mongodb.cdc.DatabaseAdapter")
    relay = MongoDBCDCRelayCrateDB(
        "mongodb://localhost/testdrive/demo", "crate://localhost/testdrive/demo", tm=None, on_error=policy
    )
    assert relay.on_error == policy


def test_mongodb_cdc_invalid_policy(mocker):
    mocker.patch("cratedb_toolkit.io.mongodb.cdc.mongodb_adapter_factory")
    mocker.patch("cratedb_toolkit.io.mongodb.cdc.DatabaseAdapter")
    with pytest.raises(ValueError, match="Unknown 'on_error' value: typo"):
        MongoDBCDCRelayCrateDB("mongodb://localhost/testdrive/demo", "crate://localhost/testdrive/demo", None, "typo")  # ty: ignore[invalid-argument-type]


def test_mongodb_cdc_continues_after_rejected_write(cdc_relay, cratedb):
    cdc_relay.cratedb_adapter = cratedb.database
    table = '"testdrive"."cdc_errors"'
    insert = 'INSERT INTO "testdrive"."cdc_errors" (id) VALUES (:id)'
    cdc_relay.cdc.sql_ddl = f"CREATE TABLE {table} (id INTEGER)"
    cdc_relay.cdc.to_sql.side_effect = [
        SQLOperation(statement=insert, parameters={"id": "invalid"}),
        SQLOperation(statement=insert, parameters={"id": 42}),
    ]
    cdc_relay.start()
    cratedb.database.refresh_table(table)
    assert cratedb.database.run_sql(f"SELECT id FROM {table}") == [(42,)]  # noqa: S608
