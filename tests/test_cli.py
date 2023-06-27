# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.

from click.testing import CliRunner

from cratedb_retentions.cli import cli


def test_version():
    """
    CLI test: Invoke `cratedb-retentions --version`.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args="--version",
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_run_delete():
    """
    CLI test: Invoke `cratedb-retentions run --strategy=delete`.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args='run --strategy=delete "crate://localhost/testdrive/data"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0
