from ..util.app import make_cli
from .convert.cli import convert_query
from .llm.cli import llm_cli
from .mcp.cli import cli as mcp_cli

cli = make_cli()
cli.add_command(convert_query, name="convert")
cli.add_command(llm_cli, name="llm")
cli.add_command(mcp_cli, name="mcp")
