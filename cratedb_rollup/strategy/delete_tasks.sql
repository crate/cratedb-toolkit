-- Copyright (c) 2021-2023, Crate.io Inc.
-- Distributed under the terms of the AGPLv3 license, see LICENSE.

SELECT QUOTE_IDENT(p.table_schema) || '.' || QUOTE_IDENT(p.table_name),
       QUOTE_IDENT(r.partition_column),
       TRY_CAST(p.values[r.partition_column] AS BIGINT)
FROM information_schema.table_partitions p
JOIN doc.retention_policies r ON p.table_schema = r.table_schema
  AND p.table_name = r.table_name
  AND p.values[r.partition_column] < '{day}'::TIMESTAMP - (r.retention_period || ' days')::INTERVAL
WHERE r.strategy = 'delete';