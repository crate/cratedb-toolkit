(xmover-queries)=
# XMover Query Gallery

## Shard Distribution over Nodes

```sql
select node['name'], sum(size) / 1024^3, count(id)  from sys.shards  group by 1  order by 1 asc;
+--------------+-----------------------------+-----------+
| node['name'] | (sum(size) / 1.073741824E9) | count(id) |
+--------------+-----------------------------+-----------+
| data-hot-0   |          1862.5866614403203 |       680 |
| data-hot-1   |          1866.0331328986213 |       684 |
| data-hot-2   |          1856.6581886671484 |      1043 |
| data-hot-3   |          1208.932889252901  |       477 |
| data-hot-4   |          1861.7727940855548 |       674 |
| data-hot-5   |          1863.4315695902333 |       744 |
| data-hot-6   |          1851.3522544233128 |       948 |
| NULL         |             0.0             |        35 |
+--------------+-----------------------------+-----------+
SELECT 8 rows in set (0.061 sec)
```
## Shard Distribution PRIMARY/REPLICAS over nodes

```sql

select node['name'], primary,  sum(size) / 1024^3, count(id)  from sys.shards  group by 1,2  order by 1 asc;
+--------------+---------+-----------------------------+-----------+
| node['name'] | primary | (sum(size) / 1.073741824E9) | count(id) |
+--------------+---------+-----------------------------+-----------+
| data-hot-0   | TRUE    |       1459.3267894154415    |       447 |
| data-hot-0   | FALSE   |        403.25987202487886   |       233 |
| data-hot-1   | TRUE    |       1209.6781993638724    |       374 |
| data-hot-1   | FALSE   |        656.3549335347489    |       310 |
| data-hot-2   | TRUE    |       1624.9012612393126    |       995 |
| data-hot-2   | FALSE   |        231.5014410642907    |        48 |
| data-hot-3   | TRUE    |          6.339549297466874  |        58 |
| data-hot-3   | FALSE   |       1202.486775631085     |       419 |
| data-hot-4   | FALSE   |        838.5498185381293    |       225 |
| data-hot-4   | TRUE    |       1023.1511942362413    |       449 |
| data-hot-5   | FALSE   |       1002.365406149067     |       422 |
| data-hot-5   | TRUE    |        860.9174101138487    |       322 |
| data-hot-6   | FALSE   |       1850.3959310995415    |       940 |
| data-hot-6   | TRUE    |          0.9159421799704432 |         8 |
| NULL         | FALSE   |          0.0                |        35 |
+--------------+---------+-----------------------------+-----------+

```

## Nodes available Space

```sql
+------------+--------------------+-----------------------------------------------+
| name       | attributes['zone'] | (fs[1]['disks']['available'] / 1.073741824E9) |
+------------+--------------------+-----------------------------------------------+
| data-hot-5 | us-west-2a         |                            142.3342628479004  |
| data-hot-0 | us-west-2a         |                            142.03089141845703 |
| data-hot-6 | us-west-2b         |                            159.68728256225586 |
| data-hot-3 | us-west-2b         |                            798.8147850036621  |
| data-hot-2 | us-west-2b         |                            156.79160690307617 |
| data-hot-1 | us-west-2c         |                            145.73613739013672 |
| data-hot-4 | us-west-2c         |                            148.39511108398438 |
+------------+--------------------+-----------------------------------------------+
```

## List biggest SHARDS on a particular Nodes

```sql
select node['name'], table_name, schema_name, id,  sum(size) / 1024^3 from sys.shards
    where node['name'] = 'data-hot-2'
    AND routing_state = 'STARTED'
    AND recovery['files']['percent'] = 0
    group by 1,2,3,4  order by 5  desc limit 8;
+--------------+-----------------------+-------------+----+-----------------------------+
| node['name'] | table_name            | schema_name | id | (sum(size) / 1.073741824E9) |
+--------------+-----------------------+-------------+----+-----------------------------+
| data-hot-2   | bottleFieldData    | curvo          |  5 |         135.568662205711    |
| data-hot-2   | bottleFieldData    | curvo          |  8 |         134.813782049343    |
| data-hot-2   | bottleFieldData    | curvo          |  3 |         133.43549298401922  |
| data-hot-2   | bottleFieldData    | curvo          | 11 |         130.10448653809726  |
| data-hot-2   | turtleFieldData    | curvo          | 31 |          54.642812703736126 |
| data-hot-2   | turtleFieldData    | curvo          | 29 |          54.06101848650724  |
| data-hot-2   | turtleFieldData    | curvo          |  5 |          53.96749582327902  |
| data-hot-2   | turtleFieldData    | curvo          | 21 |          53.72262619435787  |
+--------------+-----------------------+-------------+----+-----------------------------+
SELECT 8 rows in set (0.062 sec)
```

## Move REROUTE
```sql

alter table "curvo"."bottlefieldData" reroute move shard 21 from 'data-hot-2' to 'data-hot-3';
```
---

```sql

WITH shard_summary AS (
    SELECT
        node['name'] AS node_name,
        table_name,
        schema_name,
        CASE
            WHEN "primary" = true THEN 'PRIMARY'
            ELSE 'REPLICA'
        END AS shard_type,
        COUNT(*) AS shard_count,
        SUM(size) / 1024^3 AS total_size_gb
    FROM sys.shards
    WHERE table_name = 'orderffD'
        AND routing_state = 'STARTED'
        AND recovery['files']['percent'] = 0
    GROUP BY node['name'], table_name, schema_name, "primary"
)
SELECT
    node_name,
    table_name,
    schema_name,
    shard_type,
    shard_count,
    ROUND(total_size_gb, 2) AS total_size_gb,
    ROUND(total_size_gb / shard_count, 2) AS avg_shard_size_gb
FROM shard_summary
ORDER BY node_name, shard_type DESC, total_size_gb DESC;
```

```sql
-- Comprehensive shard distribution showing both node and zone details
SELECT
    n.attributes['zone'] AS zone,
    s.node['name'] AS node_name,
    s.table_name,
    s.schema_name,
    CASE
        WHEN s."primary" = true THEN 'PRIMARY'
        ELSE 'REPLICA'
    END AS shard_type,
    s.id AS shard_id,
    s.size / 1024^3 AS shard_size_gb,
    s.num_docs,
    s.state
FROM sys.shards s
JOIN sys.nodes n ON s.node['id'] = n.id
WHERE s.table_name = 'your_table_name'  -- Replace with your specific table name
    AND s.routing_state = 'STARTED'
    AND s.recovery['files']['percent'] = 0
ORDER BY
    n.attributes['zone'],
    s.node['name'],
    s."primary" DESC,  -- Primary shards first
    s.id;

-- Summary by zone and shard type
SELECT
    n.attributes['zone'] AS zone,
    CASE
        WHEN s."primary" = true THEN 'PRIMARY'
        ELSE 'REPLICA'
    END AS shard_type,
    COUNT(*) AS shard_count,
    COUNT(DISTINCT s.node['name']) AS nodes_with_shards,
    ROUND(SUM(s.size) / 1024^3, 2) AS total_size_gb,
    ROUND(AVG(s.size) / 1024^3, 3) AS avg_shard_size_gb,
    SUM(s.num_docs) AS total_documents
FROM sys.shards s
JOIN sys.nodes n ON s.node['id'] = n.id
WHERE s.table_name = 'orderffD'  -- Replace with your specific table name
    AND s.routing_state = 'STARTED'
    AND s.recovery['files']['percent'] = 0
GROUP BY n.attributes['zone'], s."primary"
ORDER BY zone, shard_type DESC;

```

## Relocation

```sql
SELECT
        table_name,
        shard_id,
        current_state,
        explanation,
        node_id
    FROM sys.allocations
    WHERE current_state != 'STARTED' and table_name = 'dispatchio'            and shard_id = 19
    ORDER BY current_state, table_name, shard_id;

+-----------------------+----------+---------------+-------------+------------------------+
| table_name            | shard_id | current_state | explanation | node_id                |
+-----------------------+----------+---------------+-------------+------------------------+
| dispatchio            |       19 | RELOCATING    |        NULL | ZH6fBanGSjanGqeSh-sw0A |
+-----------------------+----------+---------------+-------------+------------------------+
```

```sql
SELECT
        COUNT(*) as recovering_shards
    FROM sys.shards
    WHERE state = 'RECOVERING' OR routing_state IN ('INITIALIZING', 'RELOCATING');

```

```sql
SELECT
        table_name,
        shard_id,
        current_state,
        explanation,
        node_id
    FROM sys.allocations
    WHERE current_state != 'STARTED' and table_name = 'dispatchio' and shard_id = 19
    ORDER BY current_state, table_name, shard_id;
```
