# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.

from click.testing import CliRunner

from cratedb_rollup.cli import cli
from cratedb_rollup.util.database import run_sql


def test_version():
    """
    CLI test: Invoke `cratedb-rollup --version`.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args="--version",
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_setup(cratedb):
    """
    CLI test: Invoke `cratedb-rollup setup`.
    """
    database_url = cratedb.get_connection_url()
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args=f'setup "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_run_delete(cratedb, provision_database):
    """
    CLI test: Invoke `cratedb-rollup run --strategy=delete`.
    """

    database_url = cratedb.get_connection_url()
    runner = CliRunner()

    # Invoke data retention through CLI interface.
    result = runner.invoke(
        cli,
        args=f'run --day=2024-12-31 --strategy=delete "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify that records have been deleted.
    sql = "SELECT COUNT(*) AS count FROM doc.raw_metrics;"
    results = run_sql(dburi=database_url, sql=sql)
    assert results[0] == (0,)
