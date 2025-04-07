# Model Context Protocol (MCP)

## About

[MCP], the Model Context Protocol, is an open protocol that enables seamless
integration between LLM applications and external data sources and tools.

The main entities of MCP are [Prompts], [Resources], and [Tools].

MCP clients call servers by either invoking them as a subprocess and
communicate via stdio, or by using SSE, which implements TCP socket
communication, see [Transports].

## What's Inside

This subsystem of CTK includes a little wrapper around the [Model Context
Protocol Python SDK], providing [inquiry] and launcher utilities, i.e. to
start MCP servers and hold conversations with them, with a focus on MCP
servers that wrap database access.

In this case, the specific focus is on [CrateDB], by using relevant
PostgreSQL adapter implementations provided by the MCP ecosystem,
because CrateDB is compatible with PostgreSQL, and a specialised
MCP server for advanced conversations with CrateDB.

In order to get an idea about a fragment of the MCP server landscape,
focusing on those that are talking to PostgreSQL and CrateDB databases,
and their detailed capabilities, this subsystem also provides a little
[MCP database server registry] which can render its ingredients into JSON,
YAML, and Markdown formats.

## Usage

Enumerate registered MCP servers.
```shell
ctk query mcp list
```
```json
[
  "cratedb-mcp",
  "dbhub",
  "mcp-alchemy",
  "pg-mcp",
  "postgres-basic",
  "quarkus"
]
```

Inquire all registered MCP servers, and report about their capabilities.
```shell
ctk query mcp inquire
```
Inquire specific registered MCP server.
```shell
ctk query mcp --server-name=postgres-basic inquire
```

Launch registered MCP server.
```shell
ctk query mcp --server-name=postgres-basic launch
```

## References

Popular MCP servers that can talk to CrateDB, alphabetically sorted.
Detailed MCP API capabilities about all of them, regarding available prompts,
resources, and tools, can be explored on the {doc}`landscape` page.
Ready-to-run example programs are available per example collection
about [exploring MCP with CrateDB].

- [CrateDB MCP]
- [DBHub]
- [MCP Alchemy]
- [PG-MCP]
- [PostgreSQL basic]
- [Quarkus JDBC]

:::{note}
The {doc}`landscape` page can be generated using this command:
```shell
uvx 'cratedb-toolkit[mcp]' query mcp inquire --format=markdown | sponge mcp-cratedb-landscape.md
```
:::


```{toctree}
:maxdepth: 2
:hidden:

landscape
notes
backlog
```


[CrateDB]: https://cratedb.com/database
[CrateDB MCP]: https://github.com/crate/cratedb-mcp
[CrateDB SQLAlchemy dialect]: https://cratedb.com/docs/sqlalchemy-cratedb/
[DBHub]: https://github.com/bytebase/dbhub
[exploring MCP with CrateDB]: https://github.com/crate/cratedb-examples/tree/main/framework/mcp
[inquiry]: https://github.com/crate/cratedb-toolkit/blob/main/cratedb_toolkit/query/mcp/inquiry.py
[Introduction to MCP]: https://modelcontextprotocol.io/introduction
[JBang]: https://www.jbang.dev/
[MCP]: https://modelcontextprotocol.io/
[MCP Alchemy]: https://github.com/runekaagaard/mcp-alchemy
[MCP database server registry]: https://github.com/crate/cratedb-toolkit/blob/main/cratedb_toolkit/query/mcp/registry.py
[MCP Python SDK]: https://github.com/modelcontextprotocol/python-sdk
[MCP SSE]: https://github.com/sidharthrajaram/mcp-sse
[Model Context Protocol (MCP) @ CrateDB]: https://github.com/crate/crate-clients-tools/discussions/234
[Model Context Protocol Python SDK]: https://pypi.org/project/mcp/
[PostgreSQL basic]: https://www.npmjs.com/package/@modelcontextprotocol/server-postgres
[PG-MCP]: https://github.com/stuzero/pg-mcp-server
[Prompts]: https://modelcontextprotocol.io/docs/concepts/prompts
[Quarkus JDBC]: https://github.com/quarkiverse/quarkus-mcp-servers/tree/main/jdbc#readme
[quarkus-mcp-servers]: https://github.com/quarkiverse/quarkus-mcp-servers
[Resources]: https://modelcontextprotocol.io/docs/concepts/resources
[SQLAlchemy]: https://sqlalchemy.org/
[Tools]: https://modelcontextprotocol.io/docs/concepts/tools
[Transports]: https://modelcontextprotocol.io/docs/concepts/transports
[uv]: https://docs.astral.sh/uv/
