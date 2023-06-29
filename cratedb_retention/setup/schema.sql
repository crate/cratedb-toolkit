-- Set up the retention policy database table schema.
CREATE TABLE IF NOT EXISTS {policy_table.fullname} (

    -- Source: The database table operated upon.
    "table_schema" TEXT,                        -- The source table schema.
    "table_name" TEXT,                          -- The source table name.
    "partition_column" TEXT NOT NULL,           -- The source table column name used for partitioning.

    -- Retention parameters.
    "retention_period" INTEGER NOT NULL,        -- Retention period in days. The number of days data gets
                                                -- retained before applying the retention policy.

    -- Target: Where data is moved/relocated to.

    -- Targeting specific nodes.
    -- You may want to designate dedicated nodes to be responsible for hot or cold storage types.
    -- To do that, you can assign attributes to specific nodes, effectively tagging them.
    -- https://crate.io/docs/crate/reference/en/latest/config/node.html#custom-attributes
    "reallocation_attribute_name" TEXT,         -- Name of the node-specific custom attribute.
    "reallocation_attribute_value" TEXT,        -- Value of the node-specific custom attribute.

    -- Targeting an S3 repository.
    "target_repository_name" TEXT,              -- The name of a repository created with `CREATE REPOSITORY ...`.

    -- Strategy to apply for data retention.
    "strategy" TEXT NOT NULL,

    PRIMARY KEY ("table_schema", "table_name", "strategy")
)
CLUSTERED INTO 1 SHARDS;
