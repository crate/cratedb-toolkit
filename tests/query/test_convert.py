from click.testing import CliRunner

from cratedb_toolkit.query.cli import cli


def test_query_convert_ddb_relocate_pks():
    """
    Verify `ctk query convert --type=ddb-relocate-pks`.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        input="SELECT * FROM foobar WHERE data['PK']",
        args="convert --type=ddb-relocate-pks --primary-keys=PK,SK -",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.output == "SELECT * FROM foobar WHERE pk['PK']"
