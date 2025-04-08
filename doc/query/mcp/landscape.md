# Database MCP servers for PostgreSQL and CrateDB

This page provides an overview about a fragment of the MCP server landscape,
focusing on those that are talking to PostgreSQL and CrateDB databases.

It enumerates the most popular adapters, and includes their detailed API capabilities.

## cratedb-mcp

The CrateDB MCP server specialises on advanced CrateDB SQL operations by blending in
knowledge base resources from CrateDB's documentation about query optimizations.
It is written in Python, optionally to be invoked with `uv` or `uvx`.

:Homepage: https://github.com/crate/cratedb-mcp
:Install: `uv pip install 'cratedb-mcp @ git+https://github.com/crate/cratedb-mcp@packaging-adjustments'`
:Run: `cratedb-mcp`


### Tools

```yaml
- name: query_sql
  description: Send a SQL query to CrateDB, only 'SELECT' queries are allows, queries that modify data,
    columns or are otherwise deemed un-safe are rejected.
  inputSchema:
    properties:
      query:
        title: Query
        type: string
    required:
    - query
    title: query_sqlArguments
    type: object
- name: get_cratedb_documentation_index
  description: Gets an index with CrateDB documentation links to fetch, should download docs before answering
    questions. Has documentation name, description and link.
  inputSchema:
    properties: {}
    title: get_cratedb_documentation_indexArguments
    type: object
- name: fetch_cratedb_docs
  description: Downloads the latest CrateDB documentation piece by link. Only used to download CrateDB
    docs.
  inputSchema:
    properties:
      link:
        title: Link
        type: string
    required:
    - link
    title: fetch_cratedb_docsArguments
    type: object
- name: get_table_metadata
  description: Returns an aggregation of all CrateDB's schema, tables and their metadata
  inputSchema:
    properties: {}
    title: get_table_metadataArguments
    type: object
- name: get_health
  description: Returns the health of a CrateDB cluster.
  inputSchema:
    properties: {}
    title: get_healthArguments
    type: object
```


## dbhub

DBHub is a universal database gateway implementing the Model Context Protocol (MCP) server interface. This
gateway allows MCP-compatible clients to connect to and explore different databases.
It is written in TypeScript, to be invoked with `npx`.

:Homepage: https://github.com/bytebase/dbhub
:Run: `npx -y @bytebase/dbhub@0.2.3 --transport=stdio --dsn=postgres://crate@localhost:5432/testdrive`


### Prompts

```yaml
- name: generate_sql
  description: Generate SQL queries from natural language descriptions
  arguments:
  - name: description
    description: Natural language description of the SQL query to generate
    required: true
  - name: schema
    description: Optional database schema to use
    required: false
- name: explain_db
  description: Get explanations about database tables, columns, and structures
  arguments:
  - name: schema
    description: Optional database schema to use
    required: false
  - name: table
    description: Optional specific table to explain
    required: false
```

### Resources

```yaml
- uri: db://schemas
  name: schemas
  description: null
  mimeType: null
  size: null
  annotations: null
```

### Resource Templates

```yaml
- uriTemplate: db://schemas/{schemaName}/tables
  name: tables_in_schema
  description: null
  mimeType: null
  annotations: null
- uriTemplate: db://schemas/{schemaName}/tables/{tableName}
  name: table_structure_in_schema
  description: null
  mimeType: null
  annotations: null
- uriTemplate: db://schemas/{schemaName}/tables/{tableName}/indexes
  name: indexes_in_table
  description: null
  mimeType: null
  annotations: null
- uriTemplate: db://schemas/{schemaName}/procedures
  name: procedures_in_schema
  description: null
  mimeType: null
  annotations: null
- uriTemplate: db://schemas/{schemaName}/procedures/{procedureName}
  name: procedure_detail_in_schema
  description: null
  mimeType: null
  annotations: null
```

### Tools

```yaml
- name: run_query
  description: null
  inputSchema:
    type: object
    properties:
      query:
        type: string
        description: SQL query to execute (SELECT only)
    required:
    - query
    additionalProperties: false
    $schema: http://json-schema.org/draft-07/schema#
- name: list_connectors
  description: null
  inputSchema:
    type: object
    properties: {}
    additionalProperties: false
    $schema: http://json-schema.org/draft-07/schema#
```


## mcp-alchemy

The MCP Alchemy MCP server package uses SQLAlchemy to talk to databases and provides quite a range of tools.
It is written in Python, optionally to be invoked with `uv` or `uvx`.

:Homepage: https://github.com/runekaagaard/mcp-alchemy
:Install: `uv pip install 'mcp-alchemy>=2025.4.8' 'sqlalchemy-cratedb>=0.42.0.dev1'`
:Run: `mcp-alchemy`


### Tools

```yaml
- name: all_table_names
  description: Return all table names in the database separated by comma. Connected to crate version 6.0.0
    database '' on localhost as user 'crate'
  inputSchema:
    properties: {}
    title: all_table_namesArguments
    type: object
- name: filter_table_names
  description: Return all table names in the database containing the substring 'q' separated by comma.
    Connected to crate version 6.0.0 database '' on localhost as user 'crate'
  inputSchema:
    properties:
      q:
        title: Q
        type: string
    required:
    - q
    title: filter_table_namesArguments
    type: object
- name: schema_definitions
  description: Returns schema and relation information for the given tables. Connected to crate version
    6.0.0 database '' on localhost as user 'crate'
  inputSchema:
    properties:
      table_names:
        items:
          type: string
        title: Table Names
        type: array
    required:
    - table_names
    title: schema_definitionsArguments
    type: object
- name: execute_query
  description: Execute a SQL query and return results in a readable format. Results will be truncated
    after 4000 characters. Connected to crate version 6.0.0 database '' on localhost as user 'crate'
  inputSchema:
    properties:
      query:
        title: Query
        type: string
      params:
        anyOf:
        - type: object
        - type: 'null'
        default: null
        title: Params
    required:
    - query
    title: execute_queryArguments
    type: object
```


## pg-mcp

The PG-MCP server is specialised to talk to PostgreSQL servers. With a few adjustments,
the adapter can also talk to CrateDB. The project offers rich MCP server capabilities,
and includes advanced client programs for Claude and Gemini that work out of the box.
It is written in Python, optionally to be invoked with `uv` or `uvx`.

:Homepage: https://github.com/crate-workbench/pg-mcp-server
:Run: `python -m cratedb_toolkit.query.mcp.pg_mcp`
:Acquire:
```shell
set -e
TARGET="/tmp/pg-mcp-server"
rm -rf ${TARGET}
git clone --depth 1 --no-checkout --filter=blob:none   https://github.com/crate-workbench/pg-mcp.git ${TARGET}
cd ${TARGET}
git checkout 16d7f61d5b3197777293ebae33b519f14a9d6e55 -- pyproject.toml uv.lock server test.py
cat pyproject.toml | grep -v requires-python | sponge pyproject.toml
uv pip install .
```


### Resource Templates

```yaml
- uriTemplate: pgmcp://{conn_id}/schemas
  name: list_schemas
  description: List all non-system schemas in the database.
  mimeType: null
  annotations: null
- uriTemplate: pgmcp://{conn_id}/schemas/{schema}/tables
  name: list_schema_tables
  description: List all tables in a specific schema with their descriptions.
  mimeType: null
  annotations: null
- uriTemplate: pgmcp://{conn_id}/schemas/{schema}/tables/{table}/columns
  name: get_table_columns
  description: Get columns for a specific table with their descriptions.
  mimeType: null
  annotations: null
- uriTemplate: pgmcp://{conn_id}/schemas/{schema}/tables/{table}/indexes
  name: get_table_indexes
  description: Get indexes for a specific table with their descriptions.
  mimeType: null
  annotations: null
- uriTemplate: pgmcp://{conn_id}/schemas/{schema}/tables/{table}/constraints
  name: get_table_constraints
  description: Get constraints for a specific table with their descriptions.
  mimeType: null
  annotations: null
- uriTemplate: pgmcp://{conn_id}/schemas/{schema}/tables/{table}/indexes/{index}
  name: get_index_details
  description: Get detailed information about a specific index.
  mimeType: null
  annotations: null
- uriTemplate: pgmcp://{conn_id}/schemas/{schema}/tables/{table}/constraints/{constraint}
  name: get_constraint_details
  description: Get detailed information about a specific constraint.
  mimeType: null
  annotations: null
- uriTemplate: pgmcp://{conn_id}/schemas/{schema}/extensions
  name: list_schema_extensions
  description: List all extensions installed in a specific schema.
  mimeType: null
  annotations: null
- uriTemplate: pgmcp://{conn_id}/schemas/{schema}/extensions/{extension}
  name: get_extension_details
  description: Get detailed information about a specific extension in a schema.
  mimeType: null
  annotations: null
- uriTemplate: pgmcp://{conn_id}/schemas/{schema}/tables/{table}/sample
  name: sample_table_data
  description: Get a sample of data from a specific table.
  mimeType: null
  annotations: null
- uriTemplate: pgmcp://{conn_id}/schemas/{schema}/tables/{table}/rowcount
  name: get_table_rowcount
  description: Get the approximate row count for a specific table.
  mimeType: null
  annotations: null
```

### Tools

```yaml
- name: connect
  description: "\n        Register a database connection string and return its connection ID.\n      \
    \  \n        Args:\n            connection_string: PostgreSQL connection string (required)\n     \
    \       ctx: Request context (injected by the framework)\n            \n        Returns:\n       \
    \     Dictionary containing the connection ID\n        "
  inputSchema:
    properties:
      connection_string:
        title: Connection String
        type: string
    required:
    - connection_string
    title: connectArguments
    type: object
- name: disconnect
  description: "\n        Close a specific database connection and remove it from the pool.\n        \n\
    \        Args:\n            conn_id: Connection ID to disconnect (required)\n            ctx: Request\
    \ context (injected by the framework)\n            \n        Returns:\n            Dictionary indicating\
    \ success status\n        "
  inputSchema:
    properties:
      conn_id:
        title: Conn Id
        type: string
    required:
    - conn_id
    title: disconnectArguments
    type: object
- name: pg_query
  description: "\n        Execute a read-only SQL query against the PostgreSQL database.\n        \n \
    \       Args:\n            query: The SQL query to execute (must be read-only)\n            conn_id:\
    \ Connection ID previously obtained from the connect tool\n            params: Parameters for the\
    \ query (optional)\n            \n        Returns:\n            Query results as a list of dictionaries\n\
    \        "
  inputSchema:
    properties:
      query:
        title: Query
        type: string
      conn_id:
        title: Conn Id
        type: string
      params:
        default: null
        title: params
        type: string
    required:
    - query
    - conn_id
    title: pg_queryArguments
    type: object
- name: pg_explain
  description: "\n        Execute an EXPLAIN (FORMAT JSON) query to get PostgreSQL execution plan.\n \
    \       \n        Args:\n            query: The SQL query to analyze\n            conn_id: Connection\
    \ ID previously obtained from the connect tool\n            params: Parameters for the query (optional)\n\
    \            \n        Returns:\n            Complete JSON-formatted execution plan\n        "
  inputSchema:
    properties:
      query:
        title: Query
        type: string
      conn_id:
        title: Conn Id
        type: string
      params:
        default: null
        title: params
        type: string
    required:
    - query
    - conn_id
    title: pg_explainArguments
    type: object
```


## postgres-basic

A basic Model Context Protocol server that provides read-only access to
PostgreSQL databases per `query` tool.
It is written in TypeScript, to be invoked with `npx`.

:Homepage: https://www.npmjs.com/package/@modelcontextprotocol/server-postgres
:Run: `npx -y @modelcontextprotocol/server-postgres@0.6 postgresql://crate@localhost:5432/testdrive`


### Tools

```yaml
- name: query
  description: Run a read-only SQL query
  inputSchema:
    type: object
    properties:
      sql:
        type: string
```


## quarkus

The Quarkus MCP server communicates with databases using JDBC, providing quite a range of tools.
It is written in Java, to be invoked with `jbang`.

:Homepage: https://github.com/quarkiverse/quarkus-mcp-servers
:Run: `jbang run --java=21 jdbc@quarkiverse/quarkus-mcp-servers jdbc:postgresql://localhost:5432/testdrive -u crate`


### Prompts

```yaml
- name: er_diagram
  description: Visualize ER diagram
  arguments: []
- name: sample_data
  description: Creates sample data and perform analysis
  arguments:
  - name: topic
    description: The topic
    required: true
```

### Tools

```yaml
- name: create_table
  description: Create new table in the jdbc database
  inputSchema:
    type: object
    properties:
      query:
        type: string
        description: CREATE TABLE SQL statement
    required:
    - query
- name: database_info
  description: Get information about the database. Run this before anything else to know the SQL dialect,
    keywords etc.
  inputSchema:
    type: object
    properties: {}
    required: []
- name: describe_table
  description: Describe table
  inputSchema:
    type: object
    properties:
      catalog:
        type: string
        description: Catalog name
      schema:
        type: string
        description: Schema name
      table:
        type: string
        description: Table name
    required:
    - table
- name: list_tables
  description: List all tables in the jdbc database
  inputSchema:
    type: object
    properties: {}
    required: []
- name: read_query
  description: Execute a SELECT query on the jdbc database
  inputSchema:
    type: object
    properties:
      query:
        type: string
        description: SELECT SQL query to execute
    required:
    - query
- name: write_query
  description: Execute a INSERT, UPDATE or DELETE query on the jdbc database
  inputSchema:
    type: object
    properties:
      query:
        type: string
        description: INSERT, UPDATE or DELETE SQL query to execute
    required:
    - query
```


:::{note}
This page was generated automatically, please do not edit manually. To rebuild, use this command:
```shell
uvx 'cratedb-toolkit[mcp]' query mcp inquire --format=markdown | sponge doc/query/mcp/landscape.md
```
:::

:::{seealso}
Ready-to-run example programs about all the adapters are available per
example collection about [exploring MCP with CrateDB].
[exploring MCP with CrateDB]: https://github.com/crate/cratedb-examples/tree/main/framework/mcp.
:::


