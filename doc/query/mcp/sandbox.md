# CrateDB MCP sandbox

A little walkthrough to get you started using CrateDB and MCP.

## Prerequisites

You will need [CrateDB], [CrateDB Toolkit], [mcptools], a Swiss Army Knife for
MCP Servers, and the [uv] package manager. CrateDB will be invoked using [Docker],
but you can also use [Podman] or [CrateDB Cloud].

## Install

Run a single instance of CrateDB for evaluation purposes.
```shell
docker run --rm --name=cratedb \
  --publish=4200:4200 --publish=5432:5432 \
  --env=CRATE_HEAP_SIZE=2g crate/crate:latest \
  -Cdiscovery.type=single-node
```

Install software packages.
```shell
brew tap f/mcptools
brew install mcp uv
brew install --cask docker-desktop
```

Install the _CrateDB Toolkit_ package with MCP support.
```shell
uv tool install --upgrade 'cratedb-toolkit[mcp]'
```

## Usage

### CLI

Two examples that make the MCP server submit SQL statements to the database,
by invoking the corresponding [MCP tool] of the MCP server.

Using [cratedb-mcp].
```shell
export CRATEDB_MCP_HTTP_URL=http://localhost:4200
mcpt call query_sql --params '{"query":"SELECT * FROM sys.summits LIMIT 3"}' \
  uvx cratedb-mcp serve \
  | jq
```
Using [mcp-alchemy].
```shell
export DB_URL=crate://crate@localhost:4200/?schema=testdrive
mcpt call execute_query --params '{"query":"SELECT * FROM sys.summits LIMIT 3"}' \
  uvx --with='sqlalchemy-cratedb>=0.42.0.dev2' mcp-alchemy
```

## Landscape API documentation

The {doc}`landscape` page provides an overview of a portion of the MCP server landscape,
focusing on the most popular ones that can connect to both PostgreSQL and CrateDB databases.

:::{note}
The page can be generated using this command:
```shell
uvx 'cratedb-toolkit[mcp]' query mcp inquire --format=markdown | sponge mcp-cratedb-landscape.md
```
:::

### Looking glass

CTK includes a subsystem that provides a wrapper around the [Model Context
Protocol Python SDK], including [inquiry] and launcher utilities, i.e. to
start MCP servers and hold conversations with them, with a focus on MCP
servers that wrap database access.

In order to get an idea about a fragment of the MCP server landscape,
focusing on those that are talking to PostgreSQL and CrateDB databases,
and their detailed capabilities, this subsystem also provides a little
[MCP database server registry] which can render its ingredients into JSON,
YAML, and Markdown formats.

Enumerate registered MCP servers. All of them have been validated to work well with CrateDB.

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

:::{todo}
Launch registered MCP server.
```shell
ctk query mcp --server-name=postgres-basic launch
```
:::



[CrateDB]: https://cratedb.com/database
[cratedb-mcp]: https://cratedb.com/docs/guide/integrate/mcp/cratedb-mcp.html
[CrateDB Cloud]: https://cratedb.com/docs/cloud/
[CrateDB Toolkit]: https://cratedb-toolkit.readthedocs.io/
[Docker]: https://www.docker.com/
[inquiry]: https://github.com/crate/cratedb-toolkit/blob/main/cratedb_toolkit/query/mcp/inquiry.py
[mcp-alchemy]: https://github.com/runekaagaard/mcp-alchemy
[MCP database server registry]: https://github.com/crate/cratedb-toolkit/blob/main/cratedb_toolkit/query/mcp/registry.py
[MCP tool]: https://modelcontextprotocol.io/docs/concepts/tools
[mcptools]: https://github.com/f/mcptools
[Model Context Protocol Python SDK]: https://pypi.org/project/mcp/
[Podman]: https://podman.io/
[uv]: https://docs.astral.sh/uv/
