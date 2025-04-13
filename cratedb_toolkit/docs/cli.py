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
    Initialize the CrateDB CLI.
    
    Sets up the command-line interface for accessing CrateDB documentation and configures logging
    based on the verbose and debug options.
    
    Args:
        verbose: Enables verbose output if True.
        debug: Enables debug-level logging if True.
    """
    return boot_click(ctx, verbose, debug)


def help_functions():
    """
    Extract CrateDB SQL function definitions from documentation pages.
    
    This function scrapes documentation pages to gather SQL function details for CrateDB and outputs the results in the requested format (default is JSON).
    
    Examples:
        # Extract functions in JSON format (default)
        ctk docs functions
    
        # Extract functions in Markdown format
        ctk docs functions --format markdown
    
        # Specify a custom output file for functions
        ctk docs functions --format markdown --output cratedb-functions.md
    """  # noqa: E501


def help_settings():
    """
    Extract CrateDB configuration settings by scraping relevant documentation pages.

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
    Extract CrateDB SQL function definitions from documentation.
    
    Uses FunctionsExtractor to acquire function definitions, render them in the
    specified format, and write the output to a file.
    
    Args:
        format_ (str): Output format; must be one of "json", "yaml", "markdown", or "sql".
        output (str): File path where the rendered functions are saved.
    """
    from .functions import FunctionsExtractor

    extractor = FunctionsExtractor()
    extractor.acquire().render(format_).write(output)


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
    from .settings import SettingsExtractor

    extractor = SettingsExtractor()
    extractor.acquire().render(format_).write(output)
