# Copyright (c) 2021-2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
from cratedb_toolkit.wtf.model import InfoElement, LogElement
from cratedb_toolkit.wtf.util import get_single_value


class Library:
    """
    A collection of SQL queries and utilities suitable for diagnostics on CrateDB.

    Credits to the many authors and contributors of CrateDB diagnostics utilities,
    dashboards, and cheat sheets.

    Acknowledgements: Baurzhan Sakhariev, Eduardo Legatti, Georg Traar, Hernan
    Cianfagna, Ivan Sanchez Valencia, Karyn Silva de Azevedo, Niklas Schmidtmer,
    Walter Behmann.

    References:
    - https://community.cratedb.com/t/similar-elasticsearch-commands/1455/4
    - CrateDB Admin UI.
    - CrateDB Grafana General Diagnostics Dashboard.
    - Debugging CrateDB - Queries Cheat Sheet.
    """

    class Health:
        """
        CrateDB health check queries.
        """

        backups_recent = InfoElement(
            name="backups_recent",
            label="Recent Backups",
            sql="""
                SELECT repository, name, finished, state
                FROM sys.snapshots
                ORDER BY finished DESC
                LIMIT 10;
            """,
            description="Most recent 10 backups",
        )

        cluster_name = InfoElement(
            name="cluster_name",
            label="Cluster name",
            sql=r"SELECT name FROM sys.cluster;",
            transform=get_single_value("name"),
        )

        nodes_count = InfoElement(
            name="cluster_nodes_count",
            label="Total number of cluster nodes",
            sql=r"SELECT COUNT(*) AS count FROM sys.nodes;",
            transform=get_single_value("count"),
        )
        nodes_list = InfoElement(
            name="cluster_nodes_list",
            label="Cluster Nodes",
            sql="SELECT * FROM sys.nodes ORDER BY hostname;",
            description="Telemetry information for all cluster nodes.",
        )
        table_health = InfoElement(
            name="table_health",
            label="Table Health",
            sql="SELECT health, COUNT(*) AS table_count FROM sys.health GROUP BY health;",
            description="Table health short summary",
        )

    class JobInfo:
        """
        Information distilled from `sys.jobs_log` and `sys.jobs`.
        """

        age_range = InfoElement(
            name="age_range",
            label="Query age range",
            description="Timestamps of first and last job",
            sql="""
                SELECT
                    MIN(started) AS "first_job",
                    MAX(started) AS "last_job"
                FROM sys.jobs_log;
            """,
        )
        by_user = InfoElement(
            name="by_user",
            label="Queries by user",
            sql=r"""
                SELECT
                  username,
                  COUNT(username) AS count
                FROM sys.jobs_log
                GROUP BY username
                ORDER BY count DESC;
            """,
            description="Total number of queries per user.",
        )

        duration_buckets = InfoElement(
            name="duration_buckets",
            label="Query Duration Distribution (Buckets)",
            sql="""
                WITH dur AS (
                        SELECT
                            ended-started::LONG AS duration
                        FROM sys.jobs_log
                    ),
                    pct AS (
                        SELECT
                            [0.25,0.5,0.75,0.99,0.999,1] pct_in,
                            percentile(duration,[0.25,0.5,0.75,0.99,0.999,1]) as pct,
                            count(*) cnt
                        FROM dur
                    )
                    SELECT
                        UNNEST(pct_in) * 100 AS bucket,
                        cnt - CEIL(UNNEST(pct_in) * cnt) AS count,
                        CEIL(UNNEST(pct)) duration
                        ---cnt
                    FROM pct;
                """,
            description="Distribution of query durations, bucketed.",
        )
        duration_percentiles = InfoElement(
            name="duration_percentiles",
            label="Query Duration Distribution (Percentiles)",
            sql="""
                SELECT
                    min(ended-started::LONG) AS min,
                    percentile(ended-started::LONG, 0.50) AS p50,
                    percentile(ended-started::LONG, 0.90) AS p90,
                    percentile(ended-started::LONG, 0.99) AS p99,
                    MAX(ended-started::LONG) AS max
                FROM
                    sys.jobs_log
                LIMIT 50;
                """,
            description="Distribution of query durations, percentiles.",
        )
        history100 = InfoElement(
            name="history",
            label="Query History",
            sql="""
                SELECT
                  started AS "time",
                  stmt,
                  (ended::LONG - started::LONG) AS duration,
                  username
                FROM sys.jobs_log
                WHERE stmt NOT ILIKE '%snapshot%'
                ORDER BY time DESC
                LIMIT 100;
            """,
            transform=lambda x: list(reversed(x)),
            description="Statements and durations of the 100 recent queries / jobs.",
        )
        history_count = InfoElement(
            name="history_count",
            label="Query History Count",
            sql="""
                SELECT
                  COUNT(*) AS job_count
                FROM
                    sys.jobs_log;
            """,
            transform=get_single_value("job_count"),
            description="Total number of queries on this node.",
        )
        performance15min = InfoElement(
            name="performance15min",
            label="Query performance 15min",
            sql=r"""
                SELECT
                    CURRENT_TIMESTAMP AS last_timestamp,
                    (ended / 10000) * 10000 + 5000 AS ended_time,
                    COUNT(*) / 10.0 AS qps,
                    TRUNC(AVG(ended::BIGINT - started::BIGINT), 2) AS duration,
                    UPPER(regexp_matches(stmt,'^\s*(\w+).*')[1]) AS query_type
                FROM
                    sys.jobs_log
                WHERE
                    ended > now() - ('15 minutes')::INTERVAL
                GROUP BY 1, 2, 5
                ORDER BY ended_time ASC;
            """,
            description="The query performance within the last 15 minutes, including two metrics: "
            "queries per second, and query speed (ms).",
        )
        running = InfoElement(
            name="running",
            label="Currently Running Queries",
            sql="""
                SELECT
                  started AS "time",
                  stmt,
                  (CURRENT_TIMESTAMP::LONG - started::LONG) AS duration,
                  username
                FROM sys.jobs
                WHERE stmt NOT ILIKE '%snapshot%'
                ORDER BY time;
            """,
            description="Statements and durations of currently running queries / jobs.",
        )
        running_count = InfoElement(
            name="running_count",
            label="Number of running queries",
            sql="""
                SELECT
                    COUNT(*) AS job_count
                FROM
                    sys.jobs;
            """,
            transform=get_single_value("job_count"),
            description="Total number of currently running queries.",
        )
        top100_count = InfoElement(
            name="top100_count",
            label="Query frequency",
            description="The 100 most frequent queries.",
            sql="""
                SELECT
                  stmt,
                  COUNT(stmt) AS stmt_count,
                  MAX((ended::LONG - started::LONG) ) AS max_duration,
                  MIN((ended::LONG - started::LONG) ) AS min_duration,
                  AVG((ended::LONG - started::LONG) ) AS avg_duration,
                  PERCENTILE((ended::LONG - started::LONG), 0.99) AS p90
                FROM sys.jobs_log
                GROUP BY stmt
                ORDER BY stmt_count DESC
                LIMIT 100;
            """,
        )
        top100_duration_individual = InfoElement(
            name="top100_duration_individual",
            label="Individual Query Duration",
            description="The 100 queries by individual duration.",
            sql="""
                SELECT
                  (ended::LONG - started::LONG) AS duration,
                  stmt
                FROM sys.jobs_log
                ORDER BY duration DESC
                LIMIT 100;
            """,
            unit="ms",
        )
        top100_duration_total = InfoElement(
            name="top100_duration_total",
            label="Total Query Duration",
            description="The 100 queries by total duration.",
            sql="""
                SELECT
                  SUM(ended::LONG - started::LONG) AS total_duration,
                  stmt,
                  COUNT(stmt) AS stmt_count
                FROM sys.jobs_log
                GROUP BY stmt
                ORDER BY total_duration DESC
                LIMIT 100;
            """,
            unit="ms",
        )

    class Logs:
        """
        Access `sys.jobs_log` for logging purposes.
        """

        """
        TODO: Implement `tail` in one way or another.
              -- https://stackoverflow.com/q/4714975

        @seut says:
        why? whats the issue with sorting it desc by ended? As the table will be computed by results of
        all nodes inside the cluster, the natural ordering might not be deterministic.

        Ideas::

            SELECT * FROM sys.jobs_log OFFSET -10;
            SELECT * FROM sys.jobs_log OFFSET (SELECT count(*) FROM sys.jobs_log)-10;

        - https://cratedb.com/docs/crate/reference/en/latest/general/builtins/scalar-functions.html#to-char-expression-format-string
        - https://cratedb.com/docs/crate/reference/en/latest/general/builtins/scalar-functions.html#date-format-format-string-timezone-timestamp
        """

        user_queries_latest = LogElement(
            name="user_queries_latest",
            label="Latest User Queries",
            sql=r"""
                SELECT
                    DATE_FORMAT('%Y-%m-%dT%H:%i:%s.%f', started) AS started,
                    DATE_FORMAT('%Y-%m-%dT%H:%i:%s.%f', ended) AS ended,
                    classification, stmt, username, node
                FROM
                    sys.jobs_log
                WHERE
                    stmt NOT LIKE '%sys.%' AND
                    stmt NOT LIKE '%information_schema.%'
                ORDER BY ended DESC
                LIMIT {limit};
            """,
        )

    class Replication:
        """
        Information about logical replication.
        """

        # https://github.com/crate/crate/blob/master/docs/admin/logical-replication.rst#monitoring
        subscriptions = """
        SELECT s.subname, s.subpublications, sr.srrelid::text, sr.srsubstate, sr.srsubstate_reason
        FROM pg_subscription s
        JOIN pg_subscription_rel sr ON s.oid = sr.srsubid
        ORDER BY s.subname;
        """

    class Resources:
        """
        About system resources.
        """

        # TODO: Needs templating.
        column_cardinality = """
        SELECT tablename, attname, n_distinct
        FROM pg_stats
        WHERE schemaname = '...'
        AND tablename IN (...)
        AND attname IN (...);
        """

        file_descriptors = """
        SELECT
            name AS node_name,
            process['open_file_descriptors'] AS "open_file_descriptors",
            process['max_open_file_descriptors'] AS max_open_file_descriptors
        FROM sys.nodes
        ORDER BY node_name;
        """

        heap_usage = """
        SELECT
            name AS node_name,
            heap['used'] / heap['max']::DOUBLE AS heap_used
        FROM sys.nodes
        ORDER BY node_name;
        """

        tcp_connections = """
        SELECT
            name AS node_name,
            connections
        FROM sys.nodes
        ORDER BY node_name;
        """

        # TODO: Q: Why "14"? Is it about only getting information about the `write` thread pool?
        #       A: Yes, the `write` thread pool will be exposed as the last entry inside this array.
        #          But this may change in future.
        thread_pools = """
        SELECT
            name AS node_name,
            thread_pools[14]['queue'],
            thread_pools[14]['active'],
            thread_pools[14]['threads']
        FROM sys.nodes
        ORDER BY node_name;
        """

    class Settings:
        """
        Reflect cluster settings.
        """

        info = """
        SELECT
            name,
            master_node,
            settings['cluster']['routing']['allocation']['cluster_concurrent_rebalance']
                AS cluster_concurrent_rebalance,
            settings['indices']['recovery']['max_bytes_per_sec'] AS max_bytes_per_sec
        FROM sys.cluster
        LIMIT 1;
        """

    class Shards:
        """
        Information about shard / node / table / partition allocation and rebalancing.
        """

        # https://cratedb.com/docs/crate/reference/en/latest/admin/system-information.html#example
        # TODO: Needs templating.
        for_table = """
        SELECT
            schema_name,
            table_name,
            id,
            partition_ident,
            num_docs,
            primary,
            relocating_node,
            routing_state,
            state,
            orphan_partition
        FROM sys.shards
        WHERE schema_name = '{schema_name}' AND table_name = '{table_name}';
        """

        # Identify the location of the shards for each partition.
        # TODO: Needs templating.
        location_for_partition = """
        SELECT   table_partitions.table_schema,
                 table_partitions.table_name,
                 table_partitions.values[{partition_column}]::TIMESTAMP,
                 shards.primary,
                 shards.node['name']
        FROM sys.shards
        JOIN information_schema.table_partitions ON shards.partition_ident=table_partitions.partition_ident
        WHERE table_partitions.table_name = {table_name}
        ORDER BY 1,2,3,4,5;
        """

        allocation = InfoElement(
            name="shard_allocation",
            sql="""
                SELECT
                    IF(primary = TRUE, 'primary', 'replica') AS shard_type,
                    COUNT(*) AS shards
                  FROM sys.allocations
                  WHERE current_state != 'STARTED'
                  GROUP BY 1
            """,
            label="Shard Allocation",
            description="Support identifying issues with shard allocation.",
        )

        max_checkpoint_delta = InfoElement(
            name="max_checkpoint_delta",
            sql="""
                SELECT
                    COALESCE(MAX(seq_no_stats['local_checkpoint'] - seq_no_stats['global_checkpoint']), 0)
                    AS max_checkpoint_delta
                FROM sys.shards;
            """,
            transform=get_single_value("max_checkpoint_delta"),
            label="Delta between local and global checkpoint",
            description="If the delta between the local and global checkpoint is significantly large, "
            "shard replication might have stalled or slowed down.",
        )

        # data-hot-2 	 262
        # data-hot-1 	 146
        node_shard_distribution = InfoElement(
            name="node_shard_distribution",
            label="Shard Distribution",
            sql="""
                SELECT
                    node['name'] AS node_name,
                    COUNT(*) AS num_shards
                FROM sys.shards
                WHERE primary = true
                GROUP BY node_name;
            """,
            description="Shard distribution across nodes.",
        )

        not_started = InfoElement(
            name="shard_not_started",
            label="Shards not started",
            sql="""
                SELECT *
                FROM sys.allocations
                WHERE current_state != 'STARTED';
            """,
            description="Information about shards which have not been started.",
        )
        not_started_count = InfoElement(
            name="shard_not_started_count",
            label="Number of shards not started",
            description="Total number of shards which have not been started.",
            sql="""
                SELECT COUNT(*) AS not_started_count
                FROM sys.allocations
                WHERE current_state != 'STARTED';
            """,
            transform=get_single_value("not_started_count"),
        )

        rebalancing_progress = InfoElement(
            name="shard_rebalancing_progress",
            label="Shard Rebalancing Progress",
            sql="""
                SELECT
                    table_name,
                    schema_name,
                    recovery['stage'] AS recovery_stage,
                    AVG(recovery['size']['percent']) AS progress,
                    COUNT(*) AS count
                FROM
                    sys.shards
                GROUP BY table_name, schema_name, recovery_stage;
            """,
            description="Information about rebalancing progress.",
        )

        rebalancing_status = InfoElement(
            name="shard_rebalancing_status",
            label="Shard Rebalancing Status",
            sql="""
                SELECT node['name'], id, recovery['stage'], recovery['size']['percent'], routing_state, state
                FROM sys.shards
                WHERE routing_state IN ('INITIALIZING', 'RELOCATING')
                ORDER BY id;
            """,
            description="Information about rebalancing activities.",
        )

        table_allocation = InfoElement(
            name="table_allocation",
            label="Table Allocations",
            sql="""
                SELECT
                    table_schema, table_name, node_id, shard_id, partition_ident, current_state, decisions, explanation
                FROM
                    sys.allocations;
            """,
            description="Table allocation across nodes, shards, and partitions.",
        )

        table_allocation_special = InfoElement(
            name="table_allocation_special",
            label="Table Allocations Special",
            sql="""
                SELECT decisions[2]['node_name'] AS node_name, COUNT(*) AS table_count
                FROM sys.allocations
                GROUP BY decisions[2]['node_name'];
            """,
            description="Table allocation. Special.",
        )

        table_shard_count = InfoElement(
            name="table_shard_count",
            label="Table Shard Count",
            sql="""
                SELECT
                    table_schema,
                    table_name,
                    SUM(number_of_shards) AS num_shards
                FROM
                    information_schema.table_partitions
                WHERE
                    closed = false
                GROUP BY table_schema, table_name;
            """,
            description="Total number of shards per table.",
        )

        total_count = InfoElement(
            name="shard_total_count",
            label="Number of shards",
            description="Total number of shards.",
            sql="""
                SELECT COUNT(*) AS shard_count
                FROM sys.shards
            """,
            transform=get_single_value("shard_count"),
        )

        # TODO: Are both `translog_uncommitted` items sensible?
        translog_uncommitted = InfoElement(
            name="translog_uncommitted",
            label="Uncommitted Translog",
            description="Check if translogs are committed properly by comparing the "
            "`flush_threshold_size` with the `uncommitted_size` of a shard.",
            sql="""
                SELECT
                  sh.table_name,
                  sh.partition_ident,
                  SUM(sh.translog_stats['uncommitted_size']) / POWER(1024, 3) as "translog_uncomitted_in_gib"
                FROM information_schema.table_partitions tp
                JOIN sys.shards sh USING (table_name, partition_ident)
                WHERE sh.translog_stats['uncommitted_size'] > settings['translog']['flush_threshold_size']
                GROUP BY 1, 2
                ORDER BY 3 DESC;
            """,
        )
        translog_uncommitted_size = InfoElement(
            name="translog_uncommitted_size",
            label="Total uncommitted translog size",
            description="A large number of uncommitted translog operations can indicate issues with shard replication.",
            sql="""
                SELECT COALESCE(SUM(translog_stats['uncommitted_size']), 0) AS translog_uncommitted_size
                FROM sys.shards;
            """,
            transform=get_single_value("translog_uncommitted_size"),
            unit="bytes",
        )
