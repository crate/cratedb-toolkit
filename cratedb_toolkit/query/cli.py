from ..util.app import make_cli
from .convert.cli import convert_query
from .mcp.cli import cli as mcp_cli
from .nlsql.cli import llm_cli

cli = make_cli()
cli.add_command(convert_query, name="convert")
cli.add_command(llm_cli, name="nlsql")
cli.add_command(mcp_cli, name="mcp")
