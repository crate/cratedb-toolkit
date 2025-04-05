import dataclasses
import typing as t

from cratedb_toolkit.query.mcp.model import McpServer


@dataclasses.dataclass
class McpServerRegistry:
    """
    An enumeration of MCP servers that can talk to CrateDB in one way or another.
    """

    servers: t.List[McpServer] = dataclasses.field(default_factory=list)

    cratedb_mcp = McpServer(
        name="cratedb-mcp",
        command="cratedb-mcp",
        env={
            "CRATEDB_MCP_HTTP_URL": "http://localhost:4200",
            "CRATEDB_MCP_TRANSPORT": "stdio",
        },
        requirements=["cratedb-mcp @ git+https://github.com/crate/cratedb-mcp@packaging-adjustments"],
        homepage="https://github.com/crate/cratedb-mcp",
        description="""
The CrateDB MCP server specialises on advanced CrateDB SQL operations by blending in
knowledge base resources from CrateDB's documentation about query optimizations.
It is written in Python, optionally to be invoked with `uv` or `uvx`.
            """,
    )

    dbhub = McpServer(
        name="dbhub",
        command="npx -y @bytebase/dbhub@0.2.3 --transport=stdio --dsn=postgres://crate@localhost:5432/testdrive",
        homepage="https://github.com/bytebase/dbhub",
        description="""
DBHub is a universal database gateway implementing the Model Context Protocol (MCP) server interface. This
gateway allows MCP-compatible clients to connect to and explore different databases.
It is written in TypeScript, to be invoked with `npx`.
        """,
    )

    mcp_alchemy = McpServer(
        name="mcp-alchemy",
        command="mcp-alchemy",
        env={"DB_URL": "crate://crate@localhost:4200/?schema=testdrive"},
        requirements=[
            "mcp-alchemy @ git+https://github.com/runekaagaard/mcp-alchemy.git@b85aae6",
            "sqlalchemy-cratedb>=0.42.0.dev1",
        ],
        homepage="https://github.com/runekaagaard/mcp-alchemy",
        description="""
The MCP Alchemy MCP server package uses SQLAlchemy to talk to databases and provides quite a range of tools.
It is written in Python, optionally to be invoked with `uv` or `uvx`.
            """,
    )

    pg_mcp = McpServer(
        name="pg-mcp",
        command="python -m cratedb_toolkit.query.mcp.pg_mcp",
        preinstall="""
set -e
TARGET="/tmp/pg-mcp-server"
rm -rf ${TARGET}
git clone --depth 1 --no-checkout --filter=blob:none \
  https://github.com/crate-workbench/pg-mcp.git ${TARGET}
cd ${TARGET}
git checkout 16d7f61d5b3197777293ebae33b519f14a9d6e55 -- pyproject.toml uv.lock server test.py
cat pyproject.toml | grep -v requires-python | sponge pyproject.toml
uv pip install .
            """,
        homepage="https://github.com/crate-workbench/pg-mcp-server",
        description="""
The PG-MCP server is specialised to talk to PostgreSQL servers. With a few adjustments,
the adapter can also talk to CrateDB. The project offers rich MCP server capabilities,
and includes advanced client programs for Claude and Gemini that work out of the box.
It is written in Python, optionally to be invoked with `uv` or `uvx`.
            """,
    )

    postgres_basic = McpServer(
        name="postgres-basic",
        command="npx -y @modelcontextprotocol/server-postgres@0.6 postgresql://crate@localhost:5432/testdrive",
        homepage="https://www.npmjs.com/package/@modelcontextprotocol/server-postgres",
        description="""
A basic Model Context Protocol server that provides read-only access to
PostgreSQL databases per `query` tool.
It is written in TypeScript, to be invoked with `npx`.
            """,
    )

    quarkus = McpServer(
        name="quarkus",
        command="jbang run --java=21 jdbc@quarkiverse/quarkus-mcp-servers jdbc:postgresql://localhost:5432/testdrive -u crate",  # noqa: E501
        homepage="https://github.com/quarkiverse/quarkus-mcp-servers",
        description="""
The Quarkus MCP server communicates with databases using JDBC, providing quite a range of tools.
It is written in Java, to be invoked with `jbang`.
        """,
    )

    # Define the list of built-in servers, which are those enumerated above.
    builtins = [
        cratedb_mcp,
        dbhub,
        mcp_alchemy,
        pg_mcp,
        postgres_basic,
        quarkus,
    ]

    def __post_init__(self):
        """
        Register all built-in servers by default.
        """
        for server in self.builtins:
            self.register(server)

    def register(self, server: McpServer):
        """
        Register an MCP server.
        """
        self.servers.append(server)
        return self

    def select(self, name: t.Optional[str] = None) -> t.List[McpServer]:
        """
        Select MCP server by name. When no specific server is selected, all built-in servers are returned.
        """
        if not name:
            return self.servers
        servers = []
        for server in self.servers:
            if server.name == name:
                server.install()
                servers.append(server)
        return servers
