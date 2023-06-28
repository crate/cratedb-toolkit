-- Copyright (c) 2021-2023, Crate.io Inc.
-- Distributed under the terms of the AGPLv3 license, see LICENSE.

-- SQL DQL clause for selecting records from the retention policy database table,
-- used for all strategy implementations. It can be interpolated into other SQL
-- templates by using the `policy_dql` template variable.

-- The retention policy database table is called `"ext"."retention_policy"` by default.

SELECT strategy,
       p.table_schema,
       p.table_name,
       QUOTE_IDENT(p.table_schema) || '.' || QUOTE_IDENT(p.table_name),
       QUOTE_IDENT(r.partition_column),
       TRY_CAST(p.values[r.partition_column] AS BIGINT),
       reallocation_attribute_name,
       reallocation_attribute_value,
       target_repository_name
FROM "information_schema"."table_partitions" p
JOIN {policy_table.fullname} r ON
  p.table_schema = r.table_schema AND
  p.table_name = r.table_name AND
  p.values[r.partition_column] < '{cutoff_day}'::TIMESTAMP - (r.retention_period || ' days')::INTERVAL
