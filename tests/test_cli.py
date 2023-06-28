# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import pytest
from click.testing import CliRunner
from sqlalchemy.exc import ProgrammingError

from cratedb_rollup.cli import cli
from cratedb_rollup.util.database import run_sql
from tests.conftest import TESTDRIVE_DATA_SCHEMA, TESTDRIVE_EXT_SCHEMA


@pytest.fixture(scope="module", autouse=True)
def configure_database_schema(module_mocker):
    """
    Configure the machinery to use another schema for storing the retention
    policy table, so that it does not accidentally touch a production system.

    If not configured otherwise, the test suite currently uses `testdrive-ext`.
    """
    module_mocker.patch("os.environ", {"CRATEDB_EXT_SCHEMA": TESTDRIVE_EXT_SCHEMA})


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
        args=f'run --cutoff-day=2024-12-31 --strategy=delete "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify that records have been deleted.
    sql = f'SELECT COUNT(*) AS count FROM "{TESTDRIVE_DATA_SCHEMA}"."raw_metrics";'  # noqa: S608
    results = run_sql(dburi=database_url, sql=sql)
    assert results[0] == (0,)


def test_run_reallocate(cratedb, provision_database):
    """
    CLI test: Invoke `cratedb-rollup run --strategy=reallocate`.
    """

    database_url = cratedb.get_connection_url()
    runner = CliRunner()

    # Invoke data retention through CLI interface.
    result = runner.invoke(
        cli,
        args=f'run --cutoff-day=2024-12-31 --strategy=reallocate "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify that records have been deleted.
    sql = f'SELECT COUNT(*) AS count FROM "{TESTDRIVE_DATA_SCHEMA}"."raw_metrics";'  # noqa: S608
    results = run_sql(dburi=database_url, sql=sql)

    # FIXME: Currently, the test for this strategy apparently does not remove any records.
    #        The reason is probably, because the scenario can't easily be simulated on
    #        a single-node cluster.
    assert results[0] == (2,)


def test_run_snapshot(cratedb, provision_database):
    """
    CLI test: Invoke `cratedb-rollup run --strategy=snapshot`.
    """

    database_url = cratedb.get_connection_url()
    runner = CliRunner()

    # Invoke data retention through CLI interface.
    # FIXME: This currently can not be tested, because it needs a snapshot repository.
    # TODO: Provide an embedded MinIO S3 instance.
    with pytest.raises(ProgrammingError):
        runner.invoke(
            cli,
            args=f'run --cutoff-day=2024-12-31 --strategy=snapshot "{database_url}"',
            catch_exceptions=False,
        )
