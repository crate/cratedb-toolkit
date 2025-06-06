from click.testing import CliRunner

from cratedb_toolkit.util.app import make_cli


def test_api_no_address():
    """
    Verify `ctk <anything>` fails when invoked without a database address.
    """

    # Create CLI with a placeholder subcommand entrypoint.
    import click

    @click.command()
    def foo():
        pass

    cli = make_cli()
    cli.add_command(foo, "foo")

    # Invoke fake subcommand.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="foo",
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "Error: Missing database address" in result.output


def test_api_duplicate_address_managed():
    """
    Verify `ctk <anything>` fails when invoked with an ambiguous/duplicate database address.
    """

    # Create CLI with a placeholder subcommand entrypoint.
    import click

    @click.command()
    def foo():
        pass

    cli = make_cli()
    cli.add_command(foo, "foo")

    # Invoke fake subcommand.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="--cluster-id=foo --cluster-name=bar foo",
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "Error: Duplicate database address, please specify only one" in result.output


def test_api_duplicate_address_standalone():
    """
    Verify `ctk <anything>` fails when invoked with an ambiguous/duplicate database address.
    """

    # Create CLI with a placeholder subcommand entrypoint.
    import click

    @click.command()
    def foo():
        pass

    cli = make_cli()
    cli.add_command(foo, "foo")

    # Invoke fake subcommand.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="--cluster-id=foo --cluster-url=http://localhost:1234 foo",
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "Error: Duplicate database address, please specify only one" in result.output
