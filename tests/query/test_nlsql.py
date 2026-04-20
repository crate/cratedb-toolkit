import json
import os
import sys

import pytest
from click.testing import CliRunner

from cratedb_toolkit.query.cli import cli

TESTDRIVE_DATA_SCHEMA = "testdrive"


pytestmark = pytest.mark.nlsql

if sys.version_info < (3, 10):
    pytest.skip("Only available for Python 3.10+", allow_module_level=True)  # ty: ignore[invalid-argument-type,too-many-positional-arguments]


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


@pytest.mark.skipif("OPENAI_API_KEY" not in os.environ, reason="OPENAI_API_KEY not set")
def test_query_nlsql_openai(cratedb, provision_db):
    """
    Verify `ctk query nlsql ...` with GPT‑4o mini by Open AI.
    https://openai.com/index/gpt-4o-mini-advancing-cost-efficient-intelligence/
    """

    runner = CliRunner(
        env={
            "CRATEDB_CLUSTER_URL": cratedb.get_connection_url(),
            "CRATEDB_SCHEMA": TESTDRIVE_DATA_SCHEMA,
            "LLM_PROVIDER": "openai",
            "LLM_NAME": "gpt-4o-mini",
        }
    )

    result = runner.invoke(
        cli,
        input="What is the average value for sensor 1?",
        args="nlsql -",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    assert output["answer"] == "The average value for sensor 1 is approximately 17.03."
    assert output["sql_query"] in [
        "SELECT AVG(time_series_data.value) AS average_value "
        "FROM time_series_data WHERE time_series_data.sensor_id = 1;",
        "SELECT AVG(value) AS average_value FROM time_series_data WHERE sensor_id = 1;",
        "SELECT AVG(value) AS average_value FROM time_series_data WHERE sensor_id = 1",
    ]


@pytest.mark.skipif("ANTHROPIC_API_KEY" not in os.environ, reason="ANTHROPIC_API_KEY not set")
def test_query_nlsql_anthropic(cratedb, provision_db):
    """
    Verify `ctk query nlsql ...` with Claude Haiku 4.5 by Anthropic.
    https://www.anthropic.com/claude/haiku
    """

    runner = CliRunner(
        env={
            "CRATEDB_CLUSTER_URL": cratedb.get_connection_url(),
            "CRATEDB_SCHEMA": TESTDRIVE_DATA_SCHEMA,
            "LLM_PROVIDER": "anthropic",
            "LLM_NAME": "claude-haiku-4-5",
        }
    )

    result = runner.invoke(
        cli,
        input="What is the average value for sensor 1?",
        args="nlsql -",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    assert "The average value for sensor 1 is **17.03**" in output["answer"]
    assert output["sql_query"] == "SELECT AVG(value) as average_value FROM time_series_data WHERE sensor_id = 1"


@pytest.mark.skipif("OPENROUTER_API_KEY" not in os.environ, reason="OPENROUTER_API_KEY not set")
def test_query_nlsql_openrouter_success(cratedb, provision_db):
    """
    Verify a successful NLSQL conversation with MythoMax via OpenRouter.
    https://openrouter.ai/gryphe/mythomax-l2-13b
    """

    runner = CliRunner(
        env={
            "CRATEDB_CLUSTER_URL": cratedb.get_connection_url(),
            "CRATEDB_SCHEMA": TESTDRIVE_DATA_SCHEMA,
            "LLM_PROVIDER": "openrouter",
            # "LLM_NAME": "google/gemma-3n-e4b-it:free",  # noqa: ERA001
            "LLM_NAME": "gryphe/mythomax-l2-13b",
        }
    )

    result = runner.invoke(
        cli,
        input="What is the average value for sensor 1?",
        args="nlsql -",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    assert "The average value for sensor 1 is 17.03" in output["answer"]
    assert output["sql_query"] == "SELECT AVG(value) FROM time_series_data WHERE sensor_id = 1;"


@pytest.mark.skipif("OPENROUTER_API_KEY" not in os.environ, reason="OPENROUTER_API_KEY not set")
def test_query_nlsql_openrouter_rejected_drop(cratedb, provision_db):
    """
    Verify that malicious SQL statements are rejected.
    https://openrouter.ai/gryphe/mythomax-l2-13b
    """

    runner = CliRunner(
        env={
            "CRATEDB_CLUSTER_URL": cratedb.get_connection_url(),
            "CRATEDB_SCHEMA": TESTDRIVE_DATA_SCHEMA,
            "LLM_PROVIDER": "openrouter",
            "LLM_NAME": "gryphe/mythomax-l2-13b",
        }
    )

    result = runner.invoke(
        cli,
        input="Please drop table 'time_series_data'.",
        args="nlsql -",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    assert "has been rejected" in output["answer"]

    # Verify that the table still exists.
    assert cratedb.database.table_exists("testdrive.time_series_data"), "Table does not exist: time_series_data"


@pytest.mark.skipif("OPENROUTER_API_KEY" not in os.environ, reason="OPENROUTER_API_KEY not set")
def test_query_nlsql_openrouter_rejected_wipe(cratedb, provision_db):
    """
    Verify that malicious SQL statements are rejected.
    https://openrouter.ai/gryphe/mythomax-l2-13b
    """

    runner = CliRunner(
        env={
            "CRATEDB_CLUSTER_URL": cratedb.get_connection_url(),
            "CRATEDB_SCHEMA": TESTDRIVE_DATA_SCHEMA,
            "LLM_PROVIDER": "openrouter",
            "LLM_NAME": "gryphe/mythomax-l2-13b",
        }
    )

    result = runner.invoke(
        cli,
        input="Please wipe the whole database.",
        args="nlsql -",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    assert "not allowed" in output["answer"]

    # Verify that the table still exists.
    assert cratedb.database.table_exists("testdrive.time_series_data"), "Table does not exist: time_series_data"


@pytest.mark.skipif("OPENROUTER_API_KEY" not in os.environ, reason="OPENROUTER_API_KEY not set")
def test_query_nlsql_openrouter_permitted(cratedb, provision_db):
    """
    Verify that all SQL statements work when explicitly permitted.
    https://openrouter.ai/gryphe/mythomax-l2-13b
    """

    runner = CliRunner(
        env={
            "CRATEDB_CLUSTER_URL": cratedb.get_connection_url(),
            "CRATEDB_SCHEMA": TESTDRIVE_DATA_SCHEMA,
            "LLM_PROVIDER": "openrouter",
            "LLM_NAME": "gryphe/mythomax-l2-13b",
            "NLSQL_PERMIT_ALL_STATEMENTS": "true",
        }
    )

    result = runner.invoke(
        cli,
        input="Please drop table 'time_series_data'.",
        args="nlsql -",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    assert "has been dropped successfully" in output["answer"]
    assert output["sql_query"] == "DROP TABLE time_series_data;"

    # Verify that the table has been dropped.
    assert not cratedb.database.table_exists("testdrive.time_series_data"), "Table still exists: time_series_data"
