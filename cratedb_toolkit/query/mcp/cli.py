import asyncio
import logging

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.query.mcp.inquiry import McpServerInquiry
from cratedb_toolkit.query.mcp.registry import McpServerRegistry
from cratedb_toolkit.query.mcp.util import format_output
from cratedb_toolkit.util.cli import boot_click

logger = logging.getLogger(__name__)


def get_format_option(default="markdown"):
    return click.option(
        "--format",
        "format_",
        type=click.Choice(["markdown", "json", "yaml"]),
        required=True,
        default=default,
        help="Select output format",
    )


format_option_markdown = get_format_option(default="markdown")
format_option_json = get_format_option(default="json")


@click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
@click.option("--server-name", type=str, required=False, help="Select MCP server name")
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, server_name: str, verbose: bool, debug: bool):
    """
    MCP utilities.
    """
    ctx.meta["registry"] = McpServerRegistry()
    ctx.meta["server_name"] = server_name
    return boot_click(ctx, verbose, debug)


@cli.command(name="list")
@format_option_json
@click.pass_context
def list_servers(
    ctx: click.Context,
    format_: str,
):
    """
    Enumerate registered MCP servers.
    """
    registry = ctx.meta["registry"]
    server_name = ctx.meta["server_name"]
    inquiry = McpServerInquiry(registry.select(server_name))
    names = [server.name for server in inquiry.servers]
    print(format_output(names, format_))  # noqa: T201


@cli.command()
@format_option_markdown
@click.pass_context
def inquire(
    ctx: click.Context,
    format_: str,
):
    """
    Inquire MCP servers, report about their capabilities.
    """
    registry = ctx.meta["registry"]
    server_name = ctx.meta["server_name"]
    inquiry = McpServerInquiry(registry.select(server_name))
    result = asyncio.run(inquiry.format(format_))
    print(result)  # noqa: T201


@cli.command()
@click.pass_context
def launch(
    ctx: click.Context,
):
    """
    Launch MCP server.
    """
    registry = ctx.meta["registry"]
    server_name = ctx.meta["server_name"]
    if not server_name:
        raise click.BadParameter("MCP server name is required")
    servers = registry.select(server_name)
    servers[0].launch()
