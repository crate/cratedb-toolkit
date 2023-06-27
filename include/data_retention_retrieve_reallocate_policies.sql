WITH partition_allocations AS (
  SELECT DISTINCT s.schema_name AS table_schema,
                  s.table_name,
                  s.partition_ident,
                  n.attributes
  FROM sys.shards s
  JOIN sys.nodes n ON s.node['id'] = n.id
)
SELECT QUOTE_IDENT(p.table_schema),
       QUOTE_IDENT(p.table_name),
       QUOTE_IDENT(p.table_schema) || '.' || QUOTE_IDENT(p.table_name),
       QUOTE_IDENT(r.partition_column),
       TRY_CAST(p.values[r.partition_column] AS BIGINT),
       reallocation_attribute_name,
       reallocation_attribute_value
FROM information_schema.table_partitions p
JOIN doc.retention_policies r ON p.table_schema = r.table_schema
  AND p.table_name = r.table_name
  AND p.values[r.partition_column] < %(day)s::TIMESTAMP - (r.retention_period || ' days')::INTERVAL
JOIN partition_allocations a ON a.table_schema = p.table_schema
  AND a.table_name = p.table_name
  AND p.partition_ident = a.partition_ident
  AND attributes[r.reallocation_attribute_name] <> r.reallocation_attribute_value
WHERE r.strategy = 'reallocate'
ORDER BY 5 ASC;
