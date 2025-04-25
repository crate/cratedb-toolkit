import logging

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.util.cli import boot_click, make_command

logger = logging.getLogger(__name__)


@click.group(cls=ClickAliasedGroup)
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.pass_context
def cli(ctx: click.Context, verbose: bool, debug: bool):
    """
    Provide access to CrateDB's documentation.
    """
    return boot_click(ctx, verbose, debug)


def help_functions():
    """
    Extract CrateDB SQL function definitions by scraping relevant documentation pages.

    Examples
    ========

    # Extract functions to JSON (default)
    ctk docs functions

    # Extract functions to Markdown
    ctk docs functions --format markdown

    # Specify custom output file
    ctk docs functions --format markdown --output cratedb-functions.md
    """  # noqa: E501


def help_settings():
    """
    Scrape CrateDB configuration settings from documentation.

    Examples
    ========

    # Extract settings to JSON (default)
    ctk docs settings

    # Extract settings to Markdown
    ctk docs settings --format markdown

    # Extract SQL statements for runtime configurable settings
    ctk docs settings --format sql

    # Specify custom output file
    ctk docs settings --format markdown --output cratedb-settings.md
    """  # noqa: E501


@make_command(cli, "functions", help_functions)
@click.option(
    "--format",
    "-f",
    "format_",
    type=click.Choice(["json", "yaml", "markdown", "sql"]),
    default="json",
    help="Output format (json, yaml, markdown or sql)",
)
@click.option("--output", "-o", default=None, help="Output file name")
def functions(format_: str, output: str):
    """
    Extract CrateDB functions from documentation.

    Output in JSON, Markdown, or SQL format.
    """
    from .functions import FunctionsExtractor

    try:
        extractor = FunctionsExtractor()
        extractor.acquire().render(format_).write(output)
    except Exception as e:
        msg = f"Failed to extract functions: {e}"
        logger.exception(msg)
        raise click.ClickException(msg) from e


@make_command(cli, "settings", help_settings)
@click.option(
    "--format",
    "-f",
    "format_",
    type=click.Choice(["json", "yaml", "markdown", "sql"]),
    default="json",
    help="Output format (json, yaml, markdown or sql)",
)
@click.option("--output", "-o", default=None, help="Output file name")
def settings(format_: str, output: str):
    """
    Extract CrateDB settings from documentation.

    Output in JSON, Markdown, or SQL format.
    """
    from cratedb_toolkit.docs.settings import SettingsExtractor

    try:
        extractor = SettingsExtractor()
        extractor.acquire().render(format_).write(output)
    except Exception as e:
        msg = f"Failed to extract settings: {e}"
        logger.exception(msg)
        raise click.ClickException(msg) from e
