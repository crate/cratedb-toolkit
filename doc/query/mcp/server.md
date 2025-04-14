# Database MCP servers for PostgreSQL and CrateDB

## Overview

Popular MCP servers that can connect to both PostgreSQL and CrateDB, alphabetically sorted.

- [CrateDB MCP]
- [DBHub]
- [MCP Alchemy]
- [PG-MCP]
- [PostgreSQL basic]
- [Quarkus JDBC]

:::{seealso}
Detailed MCP API capabilities about all of them, regarding available prompts,
resources, and tools, can be explored on the {doc}`landscape` page.
Ready-to-run example programs are available per example collection
about [exploring MCP with CrateDB].
:::

## Install

The [uv package manager] is the tool of choice to install MCP servers written
in Python across the board. Please ensure it is installed on the machine
where you are invoking the MCP server.

### Claude Desktop

MCP servers are configured within the `mcpServers` section of your
`claude_desktop_config.json` configuration file. A CrateDB configuration
for [MCP Alchemy] that will automatically install the required packages
looks like [this][mcp-alchemy-cratedb]:
```json
{
  "mcpServers": {
    "my_cratedb": {
      "command": "uvx",
      "args": ["--with", "sqlalchemy-cratedb>=0.42.0.dev2", "mcp-alchemy"],
      "env": {
        "DB_URL": "crate://crate@localhost:4200/?schema=testdrive"
      }
    }
  }
}
```
If you provided required packages upfront, the incantation snippet is slightly
different.
```shell
uv pip install 'mcp-alchemy' 'sqlalchemy-cratedb>=0.42.0.dev2'
```
```json
{
  "mcpServers": {
    "my_cratedb": {
      "command": "mcp-alchemy",
      "args": [],
      "env": {
        "DB_URL": "crate://crate@localhost:4200/?schema=testdrive"
      }
    }
  }
}
```
For connecting to [CrateDB Cloud], use a URL like
`crate://user:password@example.aks1.westeurope.azure.cratedb.net:4200?ssl=true`.



[CrateDB Cloud]: https://cratedb.com/docs/cloud/
[CrateDB MCP]: https://github.com/crate/cratedb-mcp
[DBHub]: https://github.com/bytebase/dbhub
[exploring MCP with CrateDB]: https://github.com/crate/cratedb-examples/tree/main/framework/mcp
[MCP Alchemy]: https://github.com/runekaagaard/mcp-alchemy
[mcp-alchemy-cratedb]: https://github.com/runekaagaard/mcp-alchemy#cratedb
[PG-MCP]: https://github.com/stuzero/pg-mcp-server
[PostgreSQL basic]: https://www.npmjs.com/package/@modelcontextprotocol/server-postgres
[Quarkus JDBC]: https://github.com/quarkiverse/quarkus-mcp-servers/tree/main/jdbc#readme
[uv package manager]: https://docs.astral.sh/uv/
