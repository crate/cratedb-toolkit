import json
import os
from typing import Any
from unittest import mock

import pytest
from click.testing import CliRunner

from cratedb_toolkit.query.cli import cli

TESTDRIVE_DATA_SCHEMA = "testdrive"

pytest.importorskip("llama_index.core", reason="Skipping NLSQL tests because 'llama-index' is not installed")

from llama_index.core.llms import CompletionResponse, CustomLLM, LLMMetadata  # noqa: E402
from llama_index.core.llms.callbacks import llm_completion_callback  # noqa: E402

pytestmark = pytest.mark.nlsql


class ScriptedLLM(CustomLLM):
    """
    A deterministic, offline stand-in for a real LLM.

    """

    sql: str = "SELECT 1"
    answer: str = ""

    @property
    def metadata(self) -> LLMMetadata:
        return LLMMetadata(context_window=8192, num_output=512, is_chat_model=False, model_name="scripted")

    def _route(self, prompt: str) -> str:
        low = prompt.lower()
        if "synthesize a response" in low or "sql response:" in low:
            return self.answer
        return f"SQLQuery: {self.sql}"

    @llm_completion_callback()
    def complete(self, prompt: str, formatted: bool = False, **kwargs: Any) -> CompletionResponse:
        return CompletionResponse(text=self._route(prompt))

    @llm_completion_callback()
    def stream_complete(self, prompt: str, formatted: bool = False, **kwargs: Any):
        raise NotImplementedError("Streaming is not used by these tests")


@pytest.fixture(scope="session", autouse=True)
def reset_environment():
    """
    Reset environment variables.
    """
    envvars = ["NLSQL_PERMIT_ALL_STATEMENTS"]
    for envvar in envvars:
        os.environ.pop(envvar, None)


@pytest.fixture
def provision_db(cratedb):
    sql_ddl = f"""
CREATE TABLE "{TESTDRIVE_DATA_SCHEMA}".time_series_data (
    timestamp TIMESTAMP,
    value DOUBLE,
    location STRING,
    sensor_id INT
);
"""
    sql_dml = f"""
INSERT INTO "{TESTDRIVE_DATA_SCHEMA}".time_series_data (timestamp, value, location, sensor_id)
VALUES
    ('2023-09-14T00:00:00', 10.5, 'Sensor A', 1),
    ('2023-09-14T01:00:00', 15.2, 'Sensor A', 1),
    ('2023-09-14T02:00:00', 18.9, 'Sensor A', 1),
    ('2023-09-14T03:00:00', 12.7, 'Sensor B', 2),
    ('2023-09-14T04:00:00', 17.3, 'Sensor B', 2),
    ('2023-09-14T05:00:00', 20.1, 'Sensor B', 2),
    ('2023-09-14T06:00:00', 22.5, 'Sensor A', 1),
    ('2023-09-14T07:00:00', 18.3, 'Sensor A', 1),
    ('2023-09-14T08:00:00', 16.8, 'Sensor A', 1),
    ('2023-09-14T09:00:00', 14.6, 'Sensor B', 2),
    ('2023-09-14T10:00:00', 13.2, 'Sensor B', 2),
    ('2023-09-14T11:00:00', 11.7, 'Sensor B', 2);
"""  # noqa: S608
    sql_refresh = f"""
REFRESH TABLE "{TESTDRIVE_DATA_SCHEMA}".time_series_data;
"""
    cratedb.database.run_sql(sql_ddl)
    cratedb.database.run_sql(sql_dml)
    cratedb.database.run_sql(sql_refresh)


def _run_nlsql(cratedb, question: str, scripted: ScriptedLLM, permit_all: bool = False):
    """
    Invoke `ctk query nlsql -` end-to-end with the real CLI and a scripted LLM.

    """
    env = {
        "CRATEDB_CLUSTER_URL": cratedb.get_connection_url(),
        "CRATEDB_SCHEMA": TESTDRIVE_DATA_SCHEMA,
        "LLM_PROVIDER": "openai",
        "LLM_NAME": "mock-model",
        "OPENAI_API_KEY": "sk-test-not-used",
    }
    if permit_all:
        env["NLSQL_PERMIT_ALL_STATEMENTS"] = "true"

    runner = CliRunner(env=env)
    with mock.patch("cratedb_toolkit.query.nlsql.util.configure_llm", return_value=scripted):
        result = runner.invoke(cli, args="nlsql -", input=question, catch_exceptions=False)
    return result


def test_nlsql_query_success(cratedb, provision_db):
    """
    A question is translated to SQL, executed against CrateDB, and an answer
    is synthesized.
    """
    scripted = ScriptedLLM(
        sql="SELECT AVG(value) AS average_value FROM time_series_data WHERE sensor_id = 1",
        answer="The average value for sensor 1 is approximately 17.03.",
    )
    result = _run_nlsql(cratedb, "What is the average value for sensor 1?", scripted)

    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    assert output["answer"] == "The average value for sensor 1 is approximately 17.03."
    assert output["sql_query"] == "SELECT AVG(value) AS average_value FROM time_series_data WHERE sensor_id = 1"
    # The SQL actually ran against CrateDB: avg over sensor-1 rows is ~17.03.
    assert output["result"][0][0] == pytest.approx(17.03, abs=0.01)


def test_nlsql_query_rejects_drop(cratedb, provision_db):
    """
    The read-only SQL gateway must reject a generated `DROP` and leave the table intact.
    """
    scripted = ScriptedLLM(
        sql="DROP TABLE time_series_data;",
        answer="Your request to drop the table has been rejected; only read-only queries are allowed.",
    )
    result = _run_nlsql(cratedb, "Please drop table 'time_series_data'.", scripted)

    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    assert "has been rejected" in output["answer"]
    # The gateway blocked execution: no SQL result, and the table survives.
    assert "sql_query" not in output
    assert cratedb.database.table_exists("testdrive.time_series_data"), "Table must not be dropped"


def test_nlsql_query_rejects_delete(cratedb, provision_db):
    """A generated `DELETE` (data-wipe attempt) must likewise be rejected."""
    scripted = ScriptedLLM(
        sql="DELETE FROM time_series_data;",
        answer="That operation is not allowed; only read-only queries are permitted.",
    )
    result = _run_nlsql(cratedb, "Please wipe the whole database.", scripted)

    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    # The gateway blocked execution: no SQL result, and the rows survive.
    # (Checking `table_exists` would be meaningless here -- `DELETE` never drops the table.)
    assert "sql_query" not in output
    assert cratedb.database.count_records("testdrive.time_series_data") == 12, "Rows must not be deleted"


def test_nlsql_query_permit_all_statements(cratedb, provision_db):
    """
    With `NLSQL_PERMIT_ALL_STATEMENTS=true` the gateway is bypassed, so a generated `DROP`
    executes and the table is removed.
    """
    scripted = ScriptedLLM(
        sql="DROP TABLE time_series_data;",
        answer="The table has been dropped successfully.",
    )
    result = _run_nlsql(cratedb, "Please drop table 'time_series_data'.", scripted, permit_all=True)

    assert result.exit_code == 0, result.output
    assert not cratedb.database.table_exists("testdrive.time_series_data"), "Table should have been dropped"
