-- Copyright (c) 2021-2023, Crate.io Inc.
-- Distributed under the terms of the AGPLv3 license, see LICENSE.

-- intentionally not adding QUOTE_IDENT to the first two columns, as they are used in an already quoted string later on
SELECT p.table_schema,
       p.table_name,
       QUOTE_IDENT(p.table_schema) || '.' || QUOTE_IDENT(p.table_name),
       QUOTE_IDENT(r.partition_column),
       TRY_CAST(p.values[r.partition_column] AS BIGINT),
       target_repository_name
FROM information_schema.table_partitions p
JOIN {policy_table.fullname} r ON p.table_schema = r.table_schema
  AND p.table_name = r.table_name
  AND p.values[r.partition_column] < '{cutoff_day}'::TIMESTAMP - (r.retention_period || ' days')::INTERVAL
WHERE r.strategy = 'snapshot';
