# Inquire Model Context Protocol (MCP) servers.
#
# Usage:
#
#   uvx 'cratedb-toolkit[mcp]' query mcp inquire --format=markdown | sponge mcp-cratedb-landscape.md
#
# ruff: noqa: T201
import dataclasses
import io
import logging
import typing as t
from contextlib import redirect_stdout

from cratedb_toolkit.query.mcp.model import McpServer

from .util import McpServerCapabilities, to_json, to_yaml

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class McpServerInquiry:
    """
    Inquire the capabilities of MCP server, and format as JSON, Markdown, or YAML.
    """

    servers: t.List[McpServer]

    title = "MCP server landscape for PostgreSQL and CrateDB"
    text = (
        "An overview about a fragment of the MCP server landscape,\n"
        "focusing on the most popular ones that can connect to both PostgreSQL and CrateDB databases.\n"
    )

    notes = (
        ":::{note}\n"
        "This page was generated automatically, please do not edit manually. To rebuild, use this command:\n"
        "```shell\n"
        "uvx 'cratedb-toolkit[mcp]' query mcp inquire --format=markdown | sponge doc/query/mcp/landscape.md\n"
        "```\n"
        ":::\n"
    )

    seealso = (
        ":::{seealso}\n"
        "Ready-to-run example programs about all the adapters are available per\n"
        "example collection about [exploring MCP with CrateDB].\n"
        "[exploring MCP with CrateDB]: https://github.com/crate/cratedb-examples/tree/main/framework/mcp.\n"
        ":::\n"
    )

    def __post_init__(self):
        if not self.servers:
            msg = "No servers selected"
            logger.error(msg)
            raise UserWarning(msg)

    @staticmethod
    async def get_capabilities(server: McpServer):
        """
        Launch MCP server with stdio transport, and inquire API for capabilities.

        Derived from:
        https://github.com/modelcontextprotocol/python-sdk?tab=readme-ov-file#writing-mcp-clients
        """
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        if server.program is None:
            raise ValueError("Program name for MCP server not defined")

        # Create server parameters for stdio connection.
        server_params = StdioServerParameters(
            command=server.program,
            args=server.args,
            env=server.env,
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                # Initialize the connection.
                await session.initialize()

                # Inquire API.
                response = McpServerCapabilities(session)
                await response.inquire()
                return response

    async def format(self, variant: str):
        if variant == "json":
            return await self.to_json()
        elif variant == "markdown":
            return await self.to_markdown()
        elif variant == "yaml":
            return await self.to_yaml()
        else:
            raise NotImplementedError(f"Output variant not implemented: {variant}")

    async def to_dict(self):
        payload: t.Dict[str, t.Any] = {
            "meta": {"title": self.title, "text": self.text, "notes": self.notes, "seealso": self.seealso},
            "data": {},
        }
        for server in self.servers:
            capabilities = await self.get_capabilities(server)
            payload["data"][server.name] = {
                "meta": server.to_dict(),
                "capabilities": capabilities.to_dict(),
            }
        return payload

    async def to_markdown(self):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            print(f"# {self.title}")
            print()
            print(self.text)
            for server in self.servers:
                print(server.to_markdown())
                try:
                    capabilities = await self.get_capabilities(server)
                    print(capabilities.to_markdown())
                except Exception as ex:
                    logger.error(f"MCP server capability inquiry failed: {ex}")
            print(self.notes)
            print(self.seealso)
        return buffer.getvalue()

    async def to_json(self):
        return to_json(await self.to_dict())

    async def to_yaml(self):
        return to_yaml(await self.to_dict())
