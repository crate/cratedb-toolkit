# MCP server landscape for PostgreSQL and CrateDB

An overview about a fragment of the MCP server landscape,
focusing on the most popular ones that can connect to both PostgreSQL and CrateDB databases.

## cratedb-mcp

The CrateDB MCP server specialises on advanced CrateDB SQL operations by blending in
knowledge base resources from CrateDB's documentation about query optimizations.
It is written in Python, optionally to be invoked with `uv` or `uvx`.

:Homepage: <https://cratedb.com/docs/guide/integrate/mcp/cratedb-mcp.html>
:Validated with CrateDB: True
:Install: `uv pip install cratedb-mcp`
:Run: `cratedb-mcp serve`
:Example:
  ```shell
  export CRATEDB_MCP_HTTP_URL=http://localhost:4200
  mcpt call query_sql --params '{"query":"SELECT * FROM sys.summits LIMIT 3"}' \
    uvx cratedb-mcp serve \
    | jq
  ```


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

:Homepage: <https://github.com/bytebase/dbhub>
:Validated with CrateDB: True
:Run: `npx -y @bytebase/dbhub --transport=stdio --dsn=postgres://crate@localhost:5432/testdrive`
:Example:
  ```shell
  mcpt call run_query --params '{"query":"SELECT * FROM sys.summits LIMIT 3"}' \
    npx -y @bytebase/dbhub --transport=stdio --dsn=postgres://crate@localhost:5432/testdrive \
    | jq
  ```


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
  description: Run a SQL query on the current database
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
  description: List all available database connectors
  inputSchema:
    type: object
    properties: {}
    additionalProperties: false
    $schema: http://json-schema.org/draft-07/schema#
```


## mcp-alchemy

The MCP Alchemy MCP server package uses SQLAlchemy to connect to databases and provides quite a range of tools.
It is written in Python, optionally to be invoked with `uv` or `uvx`.

:Homepage: <https://github.com/runekaagaard/mcp-alchemy>
:Validated with CrateDB: True
:Install: `uv pip install 'mcp-alchemy>=2025.4.8' 'sqlalchemy-cratedb>=0.42.0.dev1'`
:Run: `mcp-alchemy`
:Example:
  ```shell
  export DB_URL=crate://crate@localhost:4200/?schema=testdrive
  mcpt call execute_query --params '{"query":"SELECT * FROM sys.summits LIMIT 3"}' \
    uvx --with='sqlalchemy-cratedb>=0.42.0.dev2' mcp-alchemy
  ```


### Tools

```yaml
- name: all_table_names
  description: Return all table names in the database separated by comma. Connected to crate version 5.10.4
    database '' on localhost as user 'crate'
  inputSchema:
    properties: {}
    title: all_table_namesArguments
    type: object
- name: filter_table_names
  description: Return all table names in the database containing the substring 'q' separated by comma.
    Connected to crate version 5.10.4 database '' on localhost as user 'crate'
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
    5.10.4 database '' on localhost as user 'crate'
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
    after 4000 characters. Connected to crate version 5.10.4 database '' on localhost as user 'crate'
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


## mcp-dbutils

DButils is an all-in-one MCP service that enables your AI to do data analysis by harnessing versatile
types of database (sqlite, mysql, postgres, and more) within a unified configuration of connections
in a secured way (like SSL).
It is written in Python, optionally to be invoked with `uv` or `uvx`.

:Homepage: <https://github.com/donghao1393/mcp-dbutils>
:Validated with CrateDB: False
:Install: `uv pip install 'mcp-dbutils'`
:Run: `mcp-dbutils --config=config.yaml`


### Tools

```yaml
- name: dbutils-run-query
  description: Execute read-only SQL query on database connection
  inputSchema:
    type: object
    properties:
      connection:
        type: string
        description: Database connection name
      sql:
        type: string
        description: SQL query (SELECT only)
    required:
    - connection
    - sql
- name: dbutils-list-tables
  description: List all available tables in the specified database connection
  inputSchema:
    type: object
    properties:
      connection:
        type: string
        description: Database connection name
    required:
    - connection
- name: dbutils-describe-table
  description: Get detailed information about a table's structure
  inputSchema:
    type: object
    properties:
      connection:
        type: string
        description: Database connection name
      table:
        type: string
        description: Table name to describe
    required:
    - connection
    - table
- name: dbutils-get-ddl
  description: Get DDL statement for creating the table
  inputSchema:
    type: object
    properties:
      connection:
        type: string
        description: Database connection name
      table:
        type: string
        description: Table name to get DDL for
    required:
    - connection
    - table
- name: dbutils-list-indexes
  description: List all indexes on the specified table
  inputSchema:
    type: object
    properties:
      connection:
        type: string
        description: Database connection name
      table:
        type: string
        description: Table name to list indexes for
    required:
    - connection
    - table
- name: dbutils-get-stats
  description: Get table statistics like row count and size
  inputSchema:
    type: object
    properties:
      connection:
        type: string
        description: Database connection name
      table:
        type: string
        description: Table name to get statistics for
    required:
    - connection
    - table
- name: dbutils-list-constraints
  description: List all constraints (primary key, foreign keys, etc) on the table
  inputSchema:
    type: object
    properties:
      connection:
        type: string
        description: Database connection name
      table:
        type: string
        description: Table name to list constraints for
    required:
    - connection
    - table
- name: dbutils-explain-query
  description: Get execution plan for a SQL query
  inputSchema:
    type: object
    properties:
      connection:
        type: string
        description: Database connection name
      sql:
        type: string
        description: SQL query to explain
    required:
    - connection
    - sql
- name: dbutils-get-performance
  description: Get database performance statistics
  inputSchema:
    type: object
    properties:
      connection:
        type: string
        description: Database connection name
    required:
    - connection
- name: dbutils-analyze-query
  description: Analyze a SQL query for performance
  inputSchema:
    type: object
    properties:
      connection:
        type: string
        description: Database connection name
      sql:
        type: string
        description: SQL query to analyze
    required:
    - connection
    - sql
```


## pg-mcp

The PG-MCP server is specialised to connect to PostgreSQL servers. With a few adjustments,
the adapter can also connect to CrateDB. The project offers rich MCP server capabilities,
and includes advanced client programs for Claude and Gemini that work out of the box.
It is written in Python, optionally to be invoked with `uv` or `uvx`.

:Homepage: <https://github.com/crate-workbench/pg-mcp-server>
:Validated with CrateDB: True
:Preinstall:
  ```shell
  set -e
  TARGET="/tmp/pg-mcp-server"
  rm -rf ${TARGET}
  git clone --depth 1 --no-checkout --filter=blob:none   https://github.com/crate-workbench/pg-mcp.git ${TARGET}
  cd ${TARGET}
  git checkout 8031f7c23472d19ea57fec90ebf25f63ab273f3c -- pyproject.toml uv.lock server test.py
  cat pyproject.toml | grep -v requires-python | sponge pyproject.toml
  uv pip install .
  ```
:Run: `python -m cratedb_toolkit.query.mcp.pg_mcp`


### Prompts

```yaml
- name: nl_to_sql_prompt
  description: "\n        Prompt to guide AI agents in converting natural language queries to SQL with\
    \ PostgreSQL.\n\n        Args:\n            query: The natural language query to convert to SQL\n\
    \            schema_json: JSON representation of the database schema (optional, can be fetched by\
    \ server)\n        "
  arguments:
  - name: query
    description: null
    required: true
  - name: schema_json
    description: null
    required: false
```

### Resource Templates

```yaml
- uriTemplate: pgmcp://{conn_id}/
  name: db_info
  description: "\n        Get the complete database information including all schemas, tables, columns,\
    \ and constraints.\n        Returns a comprehensive JSON structure with the entire database structure.\n\
    \        "
  mimeType: null
  annotations: null
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

:Homepage: <https://www.npmjs.com/package/@modelcontextprotocol/server-postgres>
:Validated with CrateDB: True
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


## postgres-mcp (Postgres Pro)

Postgres Pro is an open-source Model Context Protocol (MCP) server
with index tuning, explain plans, health checks, and safe SQL execution.
It is written in Python, optionally to be invoked with `uv` or `uvx`.

:Homepage: <https://github.com/crystaldba/postgres-mcp>
:Validated with CrateDB: False
:Install: `uv pip install 'postgres-mcp'`
:Run: `postgres-mcp postgresql://crate@localhost:5432/testdrive --access-mode=unrestricted`
:Example:
  ```shell
  mcpt call execute_sql --params '{"sql":"SELECT * FROM sys.summits LIMIT 3"}' \
    uvx postgres-mcp postgresql://crate@localhost:5432/testdrive --access-mode=unrestricted
  ```


### Tools

```yaml
- name: list_schemas
  description: List all schemas in the database
  inputSchema:
    properties: {}
    title: list_schemasArguments
    type: object
- name: list_objects
  description: List objects in a schema
  inputSchema:
    properties:
      schema_name:
        description: Schema name
        title: Schema Name
        type: string
      object_type:
        default: table
        description: 'Object type: ''table'', ''view'', ''sequence'', or ''extension'''
        title: Object Type
        type: string
    required:
    - schema_name
    title: list_objectsArguments
    type: object
- name: get_object_details
  description: Show detailed information about a database object
  inputSchema:
    properties:
      schema_name:
        description: Schema name
        title: Schema Name
        type: string
      object_name:
        description: Object name
        title: Object Name
        type: string
      object_type:
        default: table
        description: 'Object type: ''table'', ''view'', ''sequence'', or ''extension'''
        title: Object Type
        type: string
    required:
    - schema_name
    - object_name
    title: get_object_detailsArguments
    type: object
- name: explain_query
  description: Explains the execution plan for a SQL query, showing how the database will execute it and
    provides detailed cost estimates.
  inputSchema:
    properties:
      sql:
        description: SQL query to explain
        title: Sql
        type: string
      analyze:
        default: false
        description: When True, actually runs the query to show real execution statistics instead of estimates.
          Takes longer but provides more accurate information.
        title: Analyze
        type: boolean
      hypothetical_indexes:
        default: []
        description: "A list of hypothetical indexes to simulate. Each index must be a dictionary with\
          \ these keys:\n    - 'table': The table name to add the index to (e.g., 'users')\n    - 'columns':\
          \ List of column names to include in the index (e.g., ['email'] or ['last_name', 'first_name'])\n\
          \    - 'using': Optional index method (default: 'btree', other options include 'hash', 'gist',\
          \ etc.)\n\nExamples: [\n    {\"table\": \"users\", \"columns\": [\"email\"], \"using\": \"btree\"\
          },\n    {\"table\": \"orders\", \"columns\": [\"user_id\", \"created_at\"]}\n]\nIf there is\
          \ no hypothetical index, you can pass an empty list."
        items:
          type: object
        title: Hypothetical Indexes
        type: array
    required:
    - sql
    title: explain_queryArguments
    type: object
- name: analyze_workload_indexes
  description: Analyze frequently executed queries in the database and recommend optimal indexes
  inputSchema:
    properties:
      max_index_size_mb:
        default: 10000
        description: Max index size in MB
        title: Max Index Size Mb
        type: integer
    title: analyze_workload_indexesArguments
    type: object
- name: analyze_query_indexes
  description: Analyze a list of (up to 10) SQL queries and recommend optimal indexes
  inputSchema:
    properties:
      queries:
        description: List of Query strings to analyze
        items:
          type: string
        title: Queries
        type: array
      max_index_size_mb:
        default: 10000
        description: Max index size in MB
        title: Max Index Size Mb
        type: integer
    required:
    - queries
    title: analyze_query_indexesArguments
    type: object
- name: analyze_db_health
  description: 'Analyzes database health. Here are the available health checks:

    - index - checks for invalid, duplicate, and bloated indexes

    - connection - checks the number of connection and their utilization

    - vacuum - checks vacuum health for transaction id wraparound

    - sequence - checks sequences at risk of exceeding their maximum value

    - replication - checks replication health including lag and slots

    - buffer - checks for buffer cache hit rates for indexes and tables

    - constraint - checks for invalid constraints

    - all - runs all checks

    You can optionally specify a single health check or a comma-separated list of health checks. The default
    is ''all'' checks.'
  inputSchema:
    properties:
      health_type:
        default: all
        description: 'Optional. Valid values are: all, buffer, connection, constraint, index, replication,
          sequence, vacuum.'
        title: Health Type
        type: string
    title: analyze_db_healthArguments
    type: object
- name: get_top_queries
  description: Reports the slowest SQL queries based on execution time, using data from the 'pg_stat_statements'
    extension.
  inputSchema:
    properties:
      limit:
        default: 10
        description: Number of slow queries to return
        title: Limit
        type: integer
      sort_by:
        default: mean
        description: 'Sort criteria: ''total'' for total execution time or ''mean'' for mean execution
          time per call'
        title: Sort By
        type: string
    title: get_top_queriesArguments
    type: object
- name: execute_sql
  description: Execute any SQL query
  inputSchema:
    properties:
      sql:
        default: all
        description: SQL to run
        title: Sql
        type: string
    title: execute_sqlArguments
    type: object
```


## quarkus

The Quarkus MCP server communicates with databases using JDBC, providing quite a range of tools.
It is written in Java, to be invoked with `jbang`.

:Homepage: <https://github.com/quarkiverse/quarkus-mcp-servers>
:Validated with CrateDB: True
:Run: `jbang run --java=21 jdbc@quarkiverse/quarkus-mcp-servers jdbc:postgresql://localhost:5432/testdrive -u crate`
:Example:
  ```shell
  mcpt call read_query --params '{"query":"SELECT * FROM sys.summits LIMIT 3"}' \
    jbang run --java=21 jdbc@quarkiverse/quarkus-mcp-servers jdbc:postgresql://localhost:5432/testdrive -u crate \
    | jq
  ```


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


