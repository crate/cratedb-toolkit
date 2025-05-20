import dataclasses
import typing as t

from cratedb_toolkit.query.mcp.model import McpServer


@dataclasses.dataclass
class McpServerRegistry:
    """
    An enumeration of MCP servers that can connect to CrateDB in one way or another.
    """

    servers: t.List[McpServer] = dataclasses.field(default_factory=list)

    cratedb_mcp = McpServer(
        name="cratedb-mcp",
        command="cratedb-mcp",
        env={
            "CRATEDB_MCP_HTTP_URL": "http://localhost:4200",
            "CRATEDB_MCP_TRANSPORT": "stdio",
        },
        requirements=["cratedb-mcp"],
        homepage="https://cratedb.com/docs/guide/integrate/mcp/cratedb-mcp.html",
        description="""
The CrateDB MCP server specialises on advanced CrateDB SQL operations by blending in
knowledge base resources from CrateDB's documentation about query optimizations.
It is written in Python, optionally to be invoked with `uv` or `uvx`.
        """,
        cratedb_validated=True,
        example=r"""
export CRATEDB_MCP_HTTP_URL=http://localhost:4200
mcpt call query_sql --params '{"query":"SELECT * FROM sys.summits LIMIT 3"}' \
  uvx cratedb-mcp serve \
  | jq
        """,
    )

    dbhub = McpServer(
        name="dbhub",
        command="npx -y @bytebase/dbhub --transport=stdio --dsn=postgres://crate@localhost:5432/testdrive",
        homepage="https://github.com/bytebase/dbhub",
        description="""
DBHub is a universal database gateway implementing the Model Context Protocol (MCP) server interface. This
gateway allows MCP-compatible clients to connect to and explore different databases.
It is written in TypeScript, to be invoked with `npx`.
        """,
        cratedb_validated=True,
        example=r"""
mcpt call run_query --params '{"query":"SELECT * FROM sys.summits LIMIT 3"}' \
  npx -y @bytebase/dbhub --transport=stdio --dsn=postgres://crate@localhost:5432/testdrive \
  | jq
        """,
    )

    mcp_alchemy = McpServer(
        name="mcp-alchemy",
        command="mcp-alchemy",
        env={"DB_URL": "crate://crate@localhost:4200/?schema=testdrive"},
        requirements=[
            "mcp-alchemy>=2025.4.8",
            "sqlalchemy-cratedb>=0.42.0.dev2",
        ],
        homepage="https://github.com/runekaagaard/mcp-alchemy",
        description="""
The MCP Alchemy MCP server package uses SQLAlchemy to connect to databases and provides quite a range of tools.
It is written in Python, optionally to be invoked with `uv` or `uvx`.
        """,
        cratedb_validated=True,
        example=r"""
export DB_URL=crate://crate@localhost:4200/?schema=testdrive
mcpt call execute_query --params '{"query":"SELECT * FROM sys.summits LIMIT 3"}' \
  uvx --with='sqlalchemy-cratedb>=0.42.0.dev2' mcp-alchemy
        """,
    )

    mcp_db = McpServer(
        name="mcp-db",
        command="npx --yes github:dwarvesf/mcp-db --transport stdio",
        homepage="https://github.com/dwarvesf/mcp-db",
        requirements=["mcp-db"],
        description="""
A Model Context Protocol (MCP) server built with mcp-framework that provides tools and resources for
interacting with databases (PostgreSQL via DuckDB) and Google Cloud Storage (GCS).
It is written in TypeScript, to be invoked with `npx`.
        """,
        cratedb_validated=False,
        issues=["https://github.com/dwarvesf/mcp-db/issues/19"],
    )

    mcp_dbutils = McpServer(
        name="mcp-dbutils",
        command="mcp-dbutils --config=config.yaml",
        homepage="https://github.com/donghao1393/mcp-dbutils",
        requirements=["mcp-dbutils"],
        description="""
DButils is an all-in-one MCP service that enables your AI to do data analysis by harnessing versatile
types of database (sqlite, mysql, postgres, and more) within a unified configuration of connections
in a secured way (like SSL).
It is written in Python, optionally to be invoked with `uv` or `uvx`.
        """,
        cratedb_validated=False,
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
git checkout 8031f7c23472d19ea57fec90ebf25f63ab273f3c -- pyproject.toml uv.lock server test.py
cat pyproject.toml | grep -v requires-python | sponge pyproject.toml
uv pip install .
            """,
        homepage="https://github.com/crate-workbench/pg-mcp-server",
        description="""
The PG-MCP server is specialised to connect to PostgreSQL servers. With a few adjustments,
the adapter can also connect to CrateDB. The project offers rich MCP server capabilities,
and includes advanced client programs for Claude and Gemini that work out of the box.
It is written in Python, optionally to be invoked with `uv` or `uvx`.
        """,
        cratedb_validated=True,
        issues=["https://github.com/stuzero/pg-mcp-server/issues/10"],
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
        cratedb_validated=True,
    )

    postgres_mcp = McpServer(
        name="postgres-mcp (Postgres Pro)",
        command="postgres-mcp postgresql://crate@localhost:5432/testdrive --access-mode=unrestricted",
        homepage="https://github.com/crystaldba/postgres-mcp",
        requirements=["postgres-mcp"],
        description="""
Postgres Pro is an open-source Model Context Protocol (MCP) server
with index tuning, explain plans, health checks, and safe SQL execution.
It is written in Python, optionally to be invoked with `uv` or `uvx`.
        """,
        cratedb_validated=False,
        example=r"""
mcpt call execute_sql --params '{"sql":"SELECT * FROM sys.summits LIMIT 3"}' \
  uvx postgres-mcp postgresql://crate@localhost:5432/testdrive --access-mode=unrestricted
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
        cratedb_validated=True,
        example=r"""
mcpt call read_query --params '{"query":"SELECT * FROM sys.summits LIMIT 3"}' \
  jbang run --java=21 jdbc@quarkiverse/quarkus-mcp-servers jdbc:postgresql://localhost:5432/testdrive -u crate \
  | jq
        """,
    )

    # Define the list of built-in servers, which are those enumerated above.
    builtins = [
        cratedb_mcp,
        dbhub,
        mcp_alchemy,
        # mcp_db,
        mcp_dbutils,
        pg_mcp,
        postgres_basic,
        postgres_mcp,
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
