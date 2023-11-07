import io

import pytest
import sqlalchemy as sa

from cratedb_toolkit.io.sql import run_sql


@pytest.fixture
def sqlcmd(cratedb):
    def workhorse(sql: str, records: bool = True):
        return run_sql(dburi=cratedb.database.dburi, sql=sql, records=records)

    return workhorse


def test_run_sql_direct(cratedb):
    sql_string = "SELECT 1;"
    outcome = run_sql(dburi=cratedb.database.dburi, sql=sql_string, records=True)
    assert outcome == [{"1": 1}]


def test_run_sql_from_string(sqlcmd):
    sql_string = "SELECT 1;"
    outcome = sqlcmd(sql_string)
    assert outcome == [{"1": 1}]


def test_run_sql_from_file(sqlcmd, tmp_path):
    sql_file = tmp_path / "temp.sql"
    sql_file.write_text("SELECT 1;")
    outcome = sqlcmd(sql_file)
    assert outcome == [{"1": 1}]


def test_run_sql_from_buffer(sqlcmd):
    sql_buffer = io.StringIO("SELECT 1;")
    outcome = sqlcmd(sql_buffer)
    assert outcome == [{"1": 1}]


def test_run_sql_no_records(sqlcmd):
    sql_string = "SELECT 1;"
    outcome = sqlcmd(sql_string, records=False)
    assert outcome == [(1,)]


def test_run_sql_multiple_statements(sqlcmd):
    sql_string = "SELECT 1; SELECT 42;"
    outcome = sqlcmd(sql_string)
    assert outcome == [[{"1": 1}], [{"42": 42}]]


def test_run_sql_invalid_host(capsys):
    with pytest.raises(sa.exc.OperationalError) as ex:
        run_sql(dburi="crate://localhost:12345", sql="SELECT 1;")
    assert ex.match(
        ".*ConnectionError.*No more Servers available.*HTTPConnectionPool.*"
        "Failed to establish a new connection.*Connection refused.*"
    )


def test_run_sql_invalid_sql_type(capsys, sqlcmd):
    with pytest.raises(TypeError) as ex:
        sqlcmd(None)
    assert ex.match("SQL statement type must be either string, Path, or IO handle")
