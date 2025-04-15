# MCP backlog

## Iteration +1
- Documentation: Configure Claude
  https://github.com/mahawi1992/mcp-documentation-server/blob/main/docs/CLAUDE_DESKTOP_SETUP.md
- Documentation: How to use [MCP Inspector]
- Documentation: How to use `mcptools`
- Issues:
  - `pg-mcp-server`: <https://github.com/stuzero/pg-mcp-server/issues/10>
  - `mcp-db`: <https://github.com/dwarvesf/mcp-db/issues/19>
  - `cratedb-mcp`: <https://github.com/crate/cratedb-mcp/pull/3>
- New servers: Validate `mcp-dbutils` and `postgres-mcp`
- Interoperability with LangChain tools
  https://github.com/crate/langchain-cratedb/issues/43

## Done
- Provide registry information per MCP resource, launch server per MCP tool.
- Launch server using SSE transport. => Better use `mcptools`.
- Other than just selecting a server from the registry per `--server-name`,
  also permit selecting an arbitrary server. => Better use `mcptools`.


[MCP Inspector]: https://github.com/modelcontextprotocol/inspector
