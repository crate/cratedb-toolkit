CREATE TABLE IF NOT EXISTS doc.retention_policies (
   "table_schema" TEXT,
   "table_name" TEXT,
   "partition_column" TEXT NOT NULL,
   "retention_period" INTEGER NOT NULL,
   "reallocation_attribute_name" TEXT,
   "reallocation_attribute_value" TEXT,
   "target_repository_name" TEXT,
   "strategy" TEXT NOT NULL,
   PRIMARY KEY ("table_schema", "table_name", "strategy")
)
CLUSTERED INTO 1 SHARDS;
