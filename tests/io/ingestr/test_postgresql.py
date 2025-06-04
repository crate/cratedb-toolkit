import pytest
from click.testing import CliRunner

from cratedb_toolkit.cli import cli
from cratedb_toolkit.util.database import DatabaseAdapter

pytestmark = pytest.mark.postgresql

pytest.importorskip("dlt_cratedb", reason="Skipping PostgreSQL tests because 'dlt-cratedb' package is not installed")


def test_postgresql_load_table(caplog, cratedb):
    """
    CLI test: Invoke `ctk load table` for PostgresSQL to CrateDB.

    TODO: Use Testcontainers.
    """
    # postgresql_url = f"{postgresql.get_connection_url(driver=None)}?sslmode=disable&table=information_schema.tables"  # noqa: ERA001, E501
    # cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"  # noqa: ERA001
    ingestr_postgresql_url = (
        "postgresql://postgres:postgres@localhost:5433?sslmode=disable&table=information_schema.tables"
    )
    ingestr_cratedb_url = "cratedb://crate:crate@localhost:5432/testdrive/demo"
    cratedb_sqlalchemy_url = "crate://crate:crate@localhost:4200/"

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_CLUSTER_URL": ingestr_cratedb_url})
    result = runner.invoke(
        cli,
        args=f"load table {ingestr_postgresql_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    db = DatabaseAdapter(dburi=cratedb_sqlalchemy_url)
    assert db.table_exists("testdrive.demo") is True, "Table `testdrive.demo` does not exist"
    assert db.refresh_table("testdrive.demo") is True, "Refreshing table `testdrive.demo` failed"
    assert db.count_records("testdrive.demo") >= 200, (
        "Table `testdrive.demo` does not include expected amount of records"
    )
