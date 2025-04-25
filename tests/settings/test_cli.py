from click.testing import CliRunner

from cratedb_toolkit.settings.cli import cli


def test_settings_list(cratedb, caplog):
    """
    Verify `ctk settings list`.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi}, mix_stderr=False)
    result = runner.invoke(
        cli,
        args="list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert "whether or not to collect statistical information" in result.output


def test_settings_compare(cratedb, caplog):
    """
    Verify `ctk settings compare`.
    """

    # Invoke command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi}, mix_stderr=False)
    result = runner.invoke(
        cli,
        args="compare --no-color",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert "Comparing settings" in result.output
    assert "Extracting CrateDB settings" in caplog.text
    assert "Default settings loaded" in result.output
    assert "Heap Size: 536_870_912 bytes" in result.output
