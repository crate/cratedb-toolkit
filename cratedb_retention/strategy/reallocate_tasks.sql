-- Copyright (c) 2021-2023, Crate.io Inc.
-- Distributed under the terms of the AGPLv3 license, see LICENSE.

WITH partition_allocations AS (
  SELECT DISTINCT s.schema_name AS table_schema,
                  s.table_name,
                  s.partition_ident,
                  n.attributes
  FROM sys.shards s
  JOIN sys.nodes n ON s.node['id'] = n.id
)
{policy_dql}
JOIN partition_allocations a ON a.table_schema = p.table_schema
  AND a.table_name = p.table_name
  AND p.partition_ident = a.partition_ident
  AND attributes[r.reallocation_attribute_name] <> r.reallocation_attribute_value
WHERE r.strategy = 'reallocate'
ORDER BY 5 ASC;
