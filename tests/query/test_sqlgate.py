"""
Unit tests for the NLSQL SQL gateway (`cratedb_toolkit.query.nlsql.sqlgate`).

These verify the security-critical, read-only enforcement logic directly, without
any LLM or database involved -- so the guarantee "only `SELECT` statements are
permitted" is covered deterministically, independent of any model's behaviour.
"""

import pytest

pytest.importorskip("sqlparse")

from cratedb_toolkit.query.nlsql.sqlgate import SqlStatementClassifier, sql_is_permitted  # noqa: E402


@pytest.mark.parametrize(
    "expression",
    [
        "SELECT * FROM time_series_data",
        "SELECT AVG(value) AS average_value FROM time_series_data WHERE sensor_id = 1",
        "  select 1  ",
    ],
)
def test_permitted_select_statements(expression):
    assert sql_is_permitted(expression) is True


@pytest.mark.parametrize(
    "expression",
    [
        "DROP TABLE time_series_data",
        "DELETE FROM time_series_data",
        "UPDATE time_series_data SET value = 0",
        "INSERT INTO time_series_data (value) VALUES (1)",
        "CREATE TABLE foo (id INT)",
        "TRUNCATE TABLE time_series_data",
        "ALTER TABLE time_series_data ADD COLUMN foo INT",
    ],
)
def test_denied_mutating_statements(expression):
    assert sql_is_permitted(expression) is False


def test_denied_empty_statement():
    assert sql_is_permitted("") is False


def test_denied_select_into():
    # `SELECT ... INTO ...` writes data despite looking like a read.
    assert sql_is_permitted("SELECT * INTO backup FROM time_series_data") is False


def test_denied_evasive_multiple_statements():
    # Stacked statements are a classic injection vector; reject them wholesale.
    assert sql_is_permitted("SELECT * FROM time_series_data; DROP TABLE time_series_data") is False


class TestSqlStatementClassifier:
    def test_operation_detection(self):
        assert SqlStatementClassifier(expression="SELECT 1").operation == "SELECT"
        assert SqlStatementClassifier(expression="DROP TABLE t").operation == "DROP"

    def test_is_dql(self):
        assert SqlStatementClassifier(expression="SELECT 1").is_dql is True
        assert SqlStatementClassifier(expression="DELETE FROM t").is_dql is False

    def test_none_expression_is_denied(self):
        assert SqlStatementClassifier(expression=None).is_dql is False
