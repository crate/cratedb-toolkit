-- Set up the retention policy database table schema.
CREATE TABLE IF NOT EXISTS {policy_table.fullname} (

    -- Source: The database table operated upon.
    "table_schema" TEXT,                        -- The source table schema.
    "table_name" TEXT,                          -- The source table name.
    "partition_column" TEXT NOT NULL,           -- The source table column name used for partitioning.

    -- Retention parameters.
    "retention_period" INTEGER NOT NULL,        -- Retention period in days.
                                                -- TODO: Provide better description what this means.

    -- Target: Where data is moved/relocated to.
    "reallocation_attribute_name" TEXT,         -- TODO: Describe the meaning of this column value.
    "reallocation_attribute_value" TEXT,        -- TODO: Describe the meaning of this column value.
    "target_repository_name" TEXT,              -- The name of a repository created with `CREATE REPOSITORY ...`.

    -- Strategy to apply for data retention.
    "strategy" TEXT NOT NULL,

    PRIMARY KEY ("table_schema", "table_name", "strategy")
)
CLUSTERED INTO 1 SHARDS;
