import json
import os
import sys

import pytest
from click.testing import CliRunner

from cratedb_toolkit.query.cli import cli

if sys.version_info < (3, 10):
    pytest.skip("Only available for Python 3.10+", allow_module_level=True)


@pytest.fixture
def provision_db(cratedb):
    sql_ddl = """
CREATE TABLE IF NOT EXISTS time_series_data (
    timestamp TIMESTAMP,
    value DOUBLE,
    location STRING,
    sensor_id INT
);
"""
    sql_dml = """
INSERT INTO time_series_data (timestamp, value, location, sensor_id)
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
"""
    cratedb.database.run_sql(sql_ddl)
    cratedb.database.run_sql(sql_dml)


@pytest.mark.skipif("OPENAI_API_KEY" not in os.environ, reason="OPENAI_API_KEY not set")
def test_query_llm(cratedb, provision_db):
    """
    Verify `ctk query nlsql ...`.
    """

    runner = CliRunner(
        env={
            "CRATEDB_CLUSTER_URL": cratedb.get_connection_url(),
            "LLM_PROVIDER": "openai",
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
    assert output["sql_query"] == "SELECT AVG(value) FROM time_series_data WHERE sensor_id = 1"
