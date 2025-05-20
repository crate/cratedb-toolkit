# Model Context Protocol (MCP)

## Introduction

[MCP], the Model Context Protocol, is an open protocol that enables seamless
integration between LLM applications and external data sources and tools.
The main entities of MCP are [Prompts], [Resources], and [Tools].

MCP clients call MCP servers, either by invoking them as a subprocess and
communicating via Standard Input/Output (stdio), or by using Server-Sent
Events (SSE), see [Transports].

## What's Inside

The focus is on MCP servers for [CrateDB] that predominantly use PostgreSQL
adapter implementations from the MCP ecosystem because CrateDB is compatible
with PostgreSQL.


::::{grid} 1 2 2 2
:margin: 4 4 0 0

:::{grid-item-card} {material-outlined}`apps;2em` CrateDB MCP servers
:link: server
:link-type: doc
:link-alt: CrateDB MCP servers

Popular MCP servers that can connect to CrateDB.
+++
**What's inside:**
An overview about MCP servers that are compatible with CrateDB.
:::

:::{grid-item-card} {material-outlined}`not_started;2em` CrateDB MCP examples
:link: https://github.com/crate/cratedb-examples/tree/main/framework/mcp
:link-type: url
:link-alt: CrateDB MCP exploration examples

Examples for all MCP servers compatible with CrateDB.
+++
**What's inside:**
Ready-to-run MCP client programs written in Python.
:::

::::


## Development

Walkthrough tutorial and API documentation to provide a few insights and
starting points.

::::{grid} 1 2 2 2
:margin: 4 4 0 0

:::{grid-item-card} {material-outlined}`school;2em` MCP sandbox tutorial
:link: sandbox
:link-type: doc
:link-alt: CrateDB MCP sandbox walkthrough

Learn how to get started invoking and inquiring MCP servers.
+++
**What's inside:**
A complete walkthrough and guidelines for MCP and CrateDB.
:::

:::{grid-item-card} {material-outlined}`api;2em` API overview
:link: landscape
:link-type: doc
:link-alt: MCP server API details landscape for PostgreSQL and CrateDB

An overview of a fragment of the MCP server landscape for PostgreSQL
and CrateDB databases.
+++
**What's inside:**
Detailed insights into exposed prompts, resources, and tools.
:::

::::


```{toctree}
:maxdepth: 2
:hidden:

Servers <server>
Landscape <landscape>
Sandbox <sandbox>
Backlog <backlog>
```


[CrateDB]: https://cratedb.com/database
[CrateDB SQLAlchemy dialect]: https://cratedb.com/docs/sqlalchemy-cratedb/
[Introduction to MCP]: https://modelcontextprotocol.io/introduction
[JBang]: https://www.jbang.dev/
[MCP]: https://modelcontextprotocol.io/
[MCP Python SDK]: https://github.com/modelcontextprotocol/python-sdk
[MCP SSE]: https://github.com/sidharthrajaram/mcp-sse
[Model Context Protocol (MCP) @ CrateDB]: https://github.com/crate/crate-clients-tools/discussions/234
[Prompts]: https://modelcontextprotocol.io/docs/concepts/prompts
[quarkus-mcp-servers]: https://github.com/quarkiverse/quarkus-mcp-servers
[Resources]: https://modelcontextprotocol.io/docs/concepts/resources
[SQLAlchemy]: https://sqlalchemy.org/
[Tools]: https://modelcontextprotocol.io/docs/concepts/tools
[Transports]: https://modelcontextprotocol.io/docs/concepts/transports
[uv]: https://docs.astral.sh/uv/
