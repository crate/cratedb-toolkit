"""
XMover - CrateDB Shard Analyzer and Movement Tool

Command Line Interface.
"""

import sys
import time
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel

from cratedb_toolkit.admin.xmover.analysis.shard import (
    ActiveShardMonitor,
    ShardAnalyzer,
    ShardHeatReporter,
    ShardHeatSortByChoice,
    ShardReporter,
)
from cratedb_toolkit.admin.xmover.analysis.table import DistributionAnalyzer
from cratedb_toolkit.admin.xmover.analysis.zone import ZoneReport
from cratedb_toolkit.admin.xmover.model import (
    ShardRelocationConstraints,
    ShardRelocationRequest,
    SizeCriteria,
)
from cratedb_toolkit.admin.xmover.operational.candidates import CandidateFinder
from cratedb_toolkit.admin.xmover.operational.monitor import RecoveryMonitor, RecoveryOptions
from cratedb_toolkit.admin.xmover.operational.recommend import ShardRelocationRecommender
from cratedb_toolkit.admin.xmover.util.database import CrateDBClient
from cratedb_toolkit.admin.xmover.util.error import explain_cratedb_error

console = Console()


@click.group()
@click.version_option()
@click.pass_context
def main(ctx):
    """XMover - CrateDB Shard Analyzer and Movement Tool

    A tool for analyzing CrateDB shard distribution across nodes and availability zones,
    and generating safe SQL commands for shard rebalancing.
    """
    ctx.ensure_object(dict)

    # Test connection on startup
    try:
        client = CrateDBClient()
        if not client.test_connection():
            console.print("[red]Error: Could not connect to CrateDB[/red]")
            console.print("Please check your CRATE_CONNECTION_STRING in .env file")
            sys.exit(1)
        ctx.obj["client"] = client
    except Exception as e:
        console.print(f"[red]Error connecting to CrateDB: {e}[/red]")
        sys.exit(1)


@main.command()
@click.option("--table", "-t", help="Analyze specific table only")
@click.pass_context
def analyze(ctx, table: Optional[str]):
    """Analyze current shard distribution across nodes and zones"""
    client = ctx.obj["client"]
    analyzer = ShardAnalyzer(client)
    reporter = ShardReporter(analyzer)
    reporter.distribution(table=table)


@main.command()
@click.option("--table", "-t", type=click.STRING, default=None, help="Analyze specific table only")
@click.option(
    "--interval", "-i", default=10, help="The number of seconds to wait between shard data captures (default: 10)"
)
@click.option(
    "--watch",
    "-w",
    is_flag=True,
    help="When set the tool will endlessly check and report.",
)
@click.option("--max-results", "-m", default=40, help="The number of shards that will be displayed (default: 40)")
@click.option(
    "--sort-by",
    "-s",
    type=click.Choice([option.name for option in ShardHeatSortByChoice]),
    default="heat",
    help="How the shard table is sorted. Valid values are heat, node or table (default: heat)",
)
@click.pass_context
def shard_heat(ctx, table: str | None, interval: int, watch: bool, max_results: int, sort_by):
    """Monitor shards, pointing out hot ones"""
    client = ctx.obj["client"]
    analyzer = ShardAnalyzer(client)
    reporter = ShardHeatReporter(analyzer)
    reporter.report(table, interval, watch, max_results, ShardHeatSortByChoice[sort_by])


@main.command()
@click.option("--min-size", default=40.0, help="Minimum shard size in GB (default: 40)")
@click.option("--max-size", default=60.0, help="Maximum shard size in GB (default: 60)")
@click.option("--limit", default=20, help="Maximum number of candidates to show (default: 20)")
@click.option("--table", "-t", help="Find candidates for specific table only")
@click.option("--node", help="Only show candidates from this specific source node (e.g., data-hot-4)")
@click.pass_context
def find_candidates(ctx, min_size: float, max_size: float, limit: int, table: Optional[str], node: Optional[str]):
    """Find shard candidates for movement based on size criteria"""
    client = ctx.obj["client"]
    analyzer = ShardAnalyzer(client)
    finder = CandidateFinder(analyzer)
    finder.movement_candidates(
        criteria=SizeCriteria(
            min_size=min_size,
            max_size=max_size,
            table_name=table,
            source_node=node,
        ),
        limit=limit,
    )


@main.command()
@click.option("--table", "-t", help="Generate recommendations for specific table only")
@click.option("--min-size", default=40.0, help="Minimum shard size in GB (default: 40)")
@click.option("--max-size", default=60.0, help="Maximum shard size in GB (default: 60)")
@click.option("--zone-tolerance", default=10.0, help="Zone balance tolerance percentage (default: 10)")
@click.option(
    "--min-free-space", default=100.0, help="Minimum free space required on target nodes in GB (default: 100)"
)
@click.option("--max-moves", default=10, help="Maximum number of move recommendations (default: 10)")
@click.option("--max-disk-usage", default=90.0, help="Maximum disk usage percentage for target nodes (default: 90)")
@click.option("--validate/--no-validate", default=True, help="Validate move safety (default: True)")
@click.option(
    "--prioritize-space/--prioritize-zones",
    default=False,
    help="Prioritize available space over zone balancing (default: False)",
)
@click.option(
    "--dry-run/--execute", default=True, help="Show what would be done without generating SQL commands (default: True)"
)
@click.option(
    "--auto-execute",
    is_flag=True,
    default=False,
    help="DANGER: Automatically execute the SQL commands (requires --execute, asks for confirmation)",
)
@click.option("--node", help="Only recommend moves from this specific source node (e.g., data-hot-4)")
@click.pass_context
def recommend(
    ctx,
    table: Optional[str],
    node: Optional[str],
    min_size: float,
    max_size: float,
    zone_tolerance: float,
    min_free_space: float,
    max_moves: int,
    max_disk_usage: float,
    prioritize_space: bool,
    validate: bool,
    dry_run: bool,
    auto_execute: bool,
):
    """Generate shard movement recommendations for rebalancing"""
    recommender = ShardRelocationRecommender(client=ctx.obj["client"])
    recommender.execute(
        constraints=ShardRelocationConstraints(
            table_name=table,
            source_node=node,
            min_size=min_size,
            max_size=max_size,
            zone_tolerance=zone_tolerance,
            min_free_space=min_free_space,
            max_recommendations=max_moves,
            max_disk_usage=max_disk_usage,
            prioritize_space=prioritize_space,
        ),
        auto_execute=auto_execute,
        validate=validate,
        dry_run=dry_run,
    )


@main.command()
@click.option("--connection-string", help="Override connection string from .env")
@click.pass_context
def test_connection(ctx, connection_string: Optional[str]):
    """Test connection to CrateDB cluster"""
    try:
        if connection_string:
            client = CrateDBClient(connection_string)
        else:
            client = CrateDBClient()

        if client.test_connection():
            console.print("[green]✓ Connection successful![/green]")

            # Get basic cluster info
            nodes = client.get_nodes_info()
            console.print(f"Connected to cluster with {len(nodes)} nodes:")
            for node in nodes:
                console.print(f"  • {node.name} (zone: {node.zone})")
        else:
            console.print("[red]✗ Connection failed[/red]")
            sys.exit(1)

    except Exception as e:
        console.print(f"[red]✗ Connection error: {e}[/red]")
        sys.exit(1)


@main.command()
@click.option("--table", "-t", help="Check balance for specific table only")
@click.option("--tolerance", default=10.0, help="Zone balance tolerance percentage (default: 10)")
@click.pass_context
def check_balance(ctx, table: Optional[str], tolerance: float):
    """Check zone balance for shards"""
    client = ctx.obj["client"]
    report = ZoneReport(client=client)
    report.shard_balance(tolerance=tolerance, table=table)


@main.command()
@click.option("--top-tables", default=10, help="Number of largest tables to analyze (default: 10)")
@click.option("--table", help='Analyze specific table only (e.g., "my_table" or "schema.table")')
@click.pass_context
def shard_distribution(ctx, top_tables: int, table: Optional[str]):
    """Analyze shard distribution anomalies across cluster nodes

    This command analyzes the largest tables in your cluster to detect:
    • Uneven shard count distribution between nodes
    • Storage imbalances across nodes
    • Missing node coverage for tables
    • Document count imbalances indicating data skew

    Results are ranked by impact and severity to help prioritize fixes.

    Examples:
        xmover shard-distribution                    # Analyze top 10 tables
        xmover shard-distribution --top-tables 20   # Analyze top 20 tables
        xmover shard-distribution --table my_table  # Detailed report for specific table
    """
    try:
        client = ctx.obj["client"]
        analyzer = DistributionAnalyzer(client)

        if table:
            # Focused table analysis mode
            console.print(f"[blue]🔍 Analyzing table: {table}...[/blue]")

            # Find table (handles schema auto-detection)
            table_identifier = analyzer.find_table_by_name(table)
            if not table_identifier:
                console.print(f"[red]❌ Table '{table}' not found[/red]")
                return

            # Get detailed distribution
            table_dist = analyzer.get_table_distribution_detailed(table_identifier)
            if not table_dist:
                console.print(f"[red]❌ No shard data found for table '{table_identifier}'[/red]")
                return

            # Display comprehensive health report
            analyzer.format_table_health_report(table_dist)

        else:
            # General anomaly detection mode
            console.print(f"[blue]🔍 Analyzing shard distribution for top {top_tables} tables...[/blue]")
            console.print()

            # Perform analysis
            anomalies, tables_analyzed = analyzer.analyze_distribution(top_tables)

            # Display results
            analyzer.format_distribution_report(anomalies, tables_analyzed)

    except KeyboardInterrupt:
        console.print("\n[yellow]Analysis interrupted by user[/yellow]")
    except Exception as e:
        console.print(f"[red]Error during distribution analysis: {e}[/red]")
        import traceback

        console.print(f"[dim]{traceback.format_exc()}[/dim]")


@main.command()
@click.option("--count", default=10, help="Number of most active shards to show (default: 10)")
@click.option("--interval", default=30, help="Observation interval in seconds (default: 30)")
@click.option(
    "--min-checkpoint-delta",
    default=1000,
    help="Minimum checkpoint progression between snapshots to show shard (default: 1000)",
)
@click.option("--table", "-t", help="Monitor specific table only")
@click.option("--node", "-n", help="Monitor specific node only")
@click.option("--watch", "-w", is_flag=True, help="Continuously monitor (refresh every interval)")
@click.option("--exclude-system", is_flag=True, help="Exclude system tables (gc.*, information_schema.*)")
@click.option("--min-rate", type=float, help="Minimum activity rate (changes/sec) to show")
@click.option("--show-replicas/--hide-replicas", default=True, help="Show replica shards (default: True)")
@click.pass_context
def active_shards(
    ctx,
    count: int,
    interval: int,
    min_checkpoint_delta: int,
    table: Optional[str],
    node: Optional[str],
    watch: bool,
    exclude_system: bool,
    min_rate: Optional[float],
    show_replicas: bool,
):
    """Monitor most active shards by checkpoint progression

    This command takes two snapshots of ALL started shards separated by the
    observation interval, then shows the shards with the highest checkpoint
    progression (activity) between the snapshots.

    Unlike other commands, this tracks ALL shards and filters based on actual
    activity between snapshots, not current state. This captures shards that
    become active during the observation period.

    Useful for identifying which shards are receiving the most write activity
    in your cluster and understanding write patterns.

    Examples:
        xmover active-shards --count 20 --interval 60        # Top 20 over 60 seconds
        xmover active-shards --watch --interval 30           # Continuous monitoring
        xmover active-shards --table my_table --watch        # Monitor specific table
        xmover active-shards --node data-hot-1 --count 5     # Top 5 on specific node
        xmover active-shards --min-checkpoint-delta 500      # Lower activity threshold
        xmover active-shards --exclude-system --min-rate 50  # Skip system tables, min 50/sec
        xmover active-shards --hide-replicas --count 20      # Only primary shards
    """
    client = ctx.obj["client"]
    monitor = ActiveShardMonitor(client)

    def get_filtered_snapshot():
        """Get snapshot with optional filtering"""
        snapshots = client.get_active_shards_snapshot(min_checkpoint_delta=min_checkpoint_delta)

        # Apply table filter if specified
        if table:
            snapshots = [s for s in snapshots if s.table_name == table or f"{s.schema_name}.{s.table_name}" == table]

        # Apply node filter if specified
        if node:
            snapshots = [s for s in snapshots if s.node_name == node]

        # Exclude system tables if requested
        if exclude_system:
            snapshots = [
                s
                for s in snapshots
                if not (
                    s.schema_name.startswith("gc.")
                    or s.schema_name == "information_schema"
                    or s.schema_name == "sys"
                    or s.table_name.endswith("_events")
                    or s.table_name.endswith("_log")
                )
            ]

        return snapshots

    def run_single_analysis():
        """Run a single analysis cycle"""
        if not watch:
            console.print(Panel.fit("[bold blue]Active Shards Monitor[/bold blue]"))

        # Show configuration - simplified for watch mode
        if watch:
            config_parts = [f"{interval}s interval", f"threshold: {min_checkpoint_delta:,}", f"top {count}"]
            if table:
                config_parts.append(f"table: {table}")
            if node:
                config_parts.append(f"node: {node}")
            console.print(f"[dim]{' | '.join(config_parts)}[/dim]")
        else:
            config_info = [
                f"Observation interval: {interval}s",
                f"Min checkpoint delta: {min_checkpoint_delta:,}",
                f"Show count: {count}",
            ]
            if table:
                config_info.append(f"Table filter: {table}")
            if node:
                config_info.append(f"Node filter: {node}")
            if exclude_system:
                config_info.append("Excluding system tables")
            if min_rate:
                config_info.append(f"Min rate: {min_rate}/sec")
            if not show_replicas:
                config_info.append("Primary shards only")

            console.print("[dim]" + " | ".join(config_info) + "[/dim]")
        console.print()

        # Take first snapshot
        if not watch:
            console.print("📷 Taking first snapshot...")
        snapshot1 = get_filtered_snapshot()

        if not snapshot1:
            console.print("[yellow]No started shards found matching criteria[/yellow]")
            return

        if not watch:
            console.print(f"   Tracking {len(snapshot1)} started shards for activity")
            console.print(f"⏱️  Waiting {interval} seconds for activity...")

        # Wait for observation interval
        if watch:
            # Simplified countdown for watch mode
            for remaining in range(interval, 0, -1):
                if remaining % 5 == 0 or remaining <= 3:  # Show fewer updates
                    console.print(f"[dim]⏱️  {remaining}s...[/dim]", end="\r")
                time.sleep(1)
            console.print(" " * 15, end="\r")  # Clear countdown
        else:
            time.sleep(interval)

        # Take second snapshot
        if not watch:
            console.print("📷 Taking second snapshot...")
        snapshot2 = get_filtered_snapshot()

        if not snapshot2:
            console.print("[yellow]No started shards found in second snapshot[/yellow]")
            return

        if not watch:
            console.print(f"   Tracking {len(snapshot2)} started shards for activity")

        # Compare snapshots and show results
        activities = monitor.compare_snapshots(snapshot1, snapshot2, min_activity_threshold=min_checkpoint_delta)

        # Apply additional filters
        if not show_replicas:
            activities = [a for a in activities if a.is_primary]

        if min_rate:
            activities = [a for a in activities if a.activity_rate >= min_rate]

        if not activities:
            console.print(
                f"[green]✅ No shards exceeded activity threshold ({min_checkpoint_delta:,} checkpoint changes)[/green]"
            )
            if min_rate:
                console.print(f"[dim]Also filtered by minimum rate: {min_rate}/sec[/dim]")
        else:
            if not watch:
                overlap_count = len({s.shard_identifier for s in snapshot1} & {s.shard_identifier for s in snapshot2})
                console.print(f"[dim]Analyzed {overlap_count} shards present in both snapshots[/dim]")
            console.print(monitor.format_activity_display(activities, show_count=count, watch_mode=watch))

    try:
        if watch:
            console.print("[dim]Press Ctrl+C to stop monitoring[/dim]")
            console.print()

            while True:
                run_single_analysis()
                if watch:
                    console.print(f"\n[dim]━━━ Next update in {interval}s ━━━[/dim]\n")
                time.sleep(interval)
        else:
            run_single_analysis()

    except KeyboardInterrupt:
        console.print("\n[yellow]Monitoring stopped by user[/yellow]")
    except Exception as e:
        console.print(f"[red]Error during active shards monitoring: {e}[/red]")
        import traceback

        console.print(f"[dim]{traceback.format_exc()}[/dim]")


@main.command()
@click.option("--table", "-t", help="Analyze zones for specific table only")
@click.option("--show-shards/--no-show-shards", default=False, help="Show individual shard details (default: False)")
@click.pass_context
def zone_analysis(ctx, table: Optional[str], show_shards: bool):
    """Detailed analysis of zone distribution and potential conflicts"""
    client = ctx.obj["client"]
    report = ZoneReport(client=client)
    report.distribution_conflicts(shard_details=show_shards, table=table)


@main.command()
@click.argument("schema_table")
@click.argument("shard_id", type=int)
@click.argument("from_node")
@click.argument("to_node")
@click.option("--max-disk-usage", default=90.0, help="Maximum disk usage percentage for target node (default: 90)")
@click.pass_context
def validate_move(ctx, schema_table: str, shard_id: int, from_node: str, to_node: str, max_disk_usage: float):
    """Validate a specific shard move before execution

    SCHEMA_TABLE: Schema and table name (format: schema.table)
    SHARD_ID: Shard ID to move
    FROM_NODE: Source node name
    TO_NODE: Target node name

    Example: xmover validate-move CUROV.maddoxxS 4 data-hot-1 data-hot-3
    """
    recommender = ShardRelocationRecommender(client=ctx.obj["client"])
    recommender.validate(
        request=ShardRelocationRequest(
            schema_table=schema_table,
            shard_id=shard_id,
            from_node=from_node,
            to_node=to_node,
            max_disk_usage=max_disk_usage,
        )
    )


@main.command()
@click.argument("error_message", required=False)
@click.pass_context
def explain_error(ctx, error_message: Optional[str]):
    """Explain CrateDB allocation error messages and provide solutions

    ERROR_MESSAGE: The CrateDB error message to analyze (optional - can be provided interactively)

    Example: xmover explain-error "NO(a copy of this shard is already allocated to this node)"
    """
    explain_cratedb_error(error_message)


@main.command()
@click.option("--table", "-t", help="Monitor recovery for specific table only")
@click.option("--node", "-n", help="Monitor recovery on specific node only")
@click.option("--watch", "-w", is_flag=True, help="Continuously monitor (refresh every 10s)")
@click.option("--refresh-interval", default=10, help="Refresh interval for watch mode (seconds)")
@click.option(
    "--recovery-type", type=click.Choice(["PEER", "DISK", "all"]), default="all", help="Filter by recovery type"
)
@click.option("--include-transitioning", is_flag=True, help="Include completed recoveries still in transitioning state")
@click.pass_context
def monitor_recovery(
    ctx, table: str, node: str, watch: bool, refresh_interval: int, recovery_type: str, include_transitioning: bool
):
    """Monitor active shard recovery operations on the cluster

    This command monitors ongoing shard recoveries by querying sys.allocations
    and sys.shards tables. It shows recovery progress, type (PEER/DISK), and timing.

    By default, only shows actively progressing recoveries. Use --include-transitioning
    to also see completed recoveries that haven't fully transitioned to STARTED state.

    Examples:
        xmover monitor-recovery                        # Show active recoveries only
        xmover monitor-recovery --include-transitioning # Show active + transitioning
        xmover monitor-recovery --table myTable       # Monitor specific table
        xmover monitor-recovery --watch                # Continuous monitoring
        xmover monitor-recovery --recovery-type PEER  # Only PEER recoveries
    """
    recovery_monitor = RecoveryMonitor(
        client=ctx.obj["client"],
        options=RecoveryOptions(
            table=table,
            node=node,
            refresh_interval=refresh_interval,
            recovery_type=recovery_type,
            include_transitioning=include_transitioning,
        ),
    )
    recovery_monitor.start(watch=watch, debug=ctx.obj.get("debug"))


if __name__ == "__main__":
    main()
