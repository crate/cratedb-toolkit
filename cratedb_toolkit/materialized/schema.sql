-- Set up the materialized view management database table schema.
CREATE TABLE IF NOT EXISTS {materialized_table.fullname} (

    "id" TEXT NOT NULL PRIMARY KEY,

    -- Target: The database table to be populated.
    "table_schema" TEXT,                        -- The source table schema.
    "table_name" TEXT,                          -- The source table name.

    -- The SQL statement defining the emulated materialized view.
    "sql" TEXT

);
