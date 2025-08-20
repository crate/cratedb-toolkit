"""
Command line interface for XMover - CrateDB Shard Analyzer and Movement Tool
"""

import sys
import time
from typing import Any, Dict, List, Optional, cast

import click
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .analyzer import MoveRecommendation, RecoveryMonitor, ShardAnalyzer
from .database import CrateDBClient, ShardInfo

console = Console()


def format_size(size_gb: float) -> str:
    """Format size in GB with appropriate precision"""
    if size_gb >= 1000:
        return f"{size_gb / 1000:.1f}TB"
    elif size_gb >= 1:
        return f"{size_gb:.1f}GB"
    else:
        return f"{size_gb * 1000:.0f}MB"


def format_percentage(value: float) -> str:
    """Format percentage with color coding"""
    color = "green"
    if value > 80:
        color = "red"
    elif value > 70:
        color = "yellow"
    return f"[{color}]{value:.1f}%[/{color}]"


def format_translog_info(recovery_info) -> str:
    """Format translog size information with color coding"""
    tl_bytes = recovery_info.translog_size_bytes

    # Only show if significant (>10MB for production)
    if tl_bytes < 10 * 1024 * 1024:  # 10MB for production
        return ""

    tl_gb = recovery_info.translog_size_gb

    # Color coding based on size
    if tl_gb >= 5.0:
        color = "red"
    elif tl_gb >= 1.0:
        color = "yellow"
    else:
        color = "green"

    # Format size
    if tl_gb >= 1.0:
        size_str = f"{tl_gb:.1f}GB"
    else:
        size_str = f"{tl_gb * 1000:.0f}MB"

    return f" [dim]([{color}]TL:{size_str}[/{color}])[/dim]"


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

    console.print(Panel.fit("[bold blue]CrateDB Cluster Analysis[/bold blue]"))

    # Get cluster overview (includes all shards for complete analysis)
    overview = analyzer.get_cluster_overview()

    # Cluster summary table
    summary_table = Table(title="Cluster Summary", box=box.ROUNDED)
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Value", style="magenta")

    summary_table.add_row("Nodes", str(overview["nodes"]))
    summary_table.add_row("Availability Zones", str(overview["zones"]))
    summary_table.add_row("Total Shards", str(overview["total_shards"]))
    summary_table.add_row("Primary Shards", str(overview["primary_shards"]))
    summary_table.add_row("Replica Shards", str(overview["replica_shards"]))
    summary_table.add_row("Total Size", format_size(overview["total_size_gb"]))

    console.print(summary_table)
    console.print()

    # Disk watermarks table
    if overview.get("watermarks"):
        watermarks_table = Table(title="Disk Allocation Watermarks", box=box.ROUNDED)
        watermarks_table.add_column("Setting", style="cyan")
        watermarks_table.add_column("Value", style="magenta")

        watermarks = overview["watermarks"]
        watermarks_table.add_row("Low Watermark", str(watermarks.get("low", "Not set")))
        watermarks_table.add_row("High Watermark", str(watermarks.get("high", "Not set")))
        watermarks_table.add_row("Flood Stage", str(watermarks.get("flood_stage", "Not set")))
        watermarks_table.add_row(
            "Enable for Single Node", str(watermarks.get("enable_for_single_data_node", "Not set"))
        )

        console.print(watermarks_table)
        console.print()

    # Zone distribution table
    zone_table = Table(title="Zone Distribution", box=box.ROUNDED)
    zone_table.add_column("Zone", style="cyan")
    zone_table.add_column("Shards", justify="right", style="magenta")
    zone_table.add_column("Percentage", justify="right", style="green")

    total_shards = overview["total_shards"]
    for zone, count in overview["zone_distribution"].items():
        percentage = (count / total_shards * 100) if total_shards > 0 else 0
        zone_table.add_row(zone, str(count), f"{percentage:.1f}%")

    console.print(zone_table)
    console.print()

    # Node health table
    node_table = Table(title="Node Health", box=box.ROUNDED)
    node_table.add_column("Node", style="cyan")
    node_table.add_column("Zone", style="blue")
    node_table.add_column("Shards", justify="right", style="magenta")
    node_table.add_column("Size", justify="right", style="green")
    node_table.add_column("Disk Usage", justify="right")
    node_table.add_column("Available Space", justify="right", style="green")
    node_table.add_column("Until Low WM", justify="right", style="yellow")
    node_table.add_column("Until High WM", justify="right", style="red")

    for node_info in overview["node_health"]:
        # Format watermark remaining capacity
        low_wm_remaining = (
            format_size(node_info["remaining_to_low_watermark_gb"])
            if node_info["remaining_to_low_watermark_gb"] > 0
            else "[red]Exceeded[/red]"
        )
        high_wm_remaining = (
            format_size(node_info["remaining_to_high_watermark_gb"])
            if node_info["remaining_to_high_watermark_gb"] > 0
            else "[red]Exceeded[/red]"
        )

        node_table.add_row(
            node_info["name"],
            node_info["zone"],
            str(node_info["shards"]),
            format_size(node_info["size_gb"]),
            format_percentage(node_info["disk_usage_percent"]),
            format_size(node_info["available_space_gb"]),
            low_wm_remaining,
            high_wm_remaining,
        )

    console.print(node_table)

    # Table-specific analysis if requested
    if table:
        console.print()
        console.print(Panel.fit(f"[bold blue]Analysis for table: {table}[/bold blue]"))

        stats = analyzer.analyze_distribution(table)

        table_summary = Table(title=f"Table {table} Distribution", box=box.ROUNDED)
        table_summary.add_column("Metric", style="cyan")
        table_summary.add_column("Value", style="magenta")

        table_summary.add_row("Total Shards", str(stats.total_shards))
        table_summary.add_row("Total Size", format_size(stats.total_size_gb))
        table_summary.add_row("Zone Balance Score", f"{stats.zone_balance_score:.1f}/100")
        table_summary.add_row("Node Balance Score", f"{stats.node_balance_score:.1f}/100")

        console.print(table_summary)


@main.command()
@click.option("--table", "-t", help="Find candidates for specific table only")
@click.option("--min-size", default=40.0, help="Minimum shard size in GB (default: 40)")
@click.option("--max-size", default=60.0, help="Maximum shard size in GB (default: 60)")
@click.option("--limit", default=20, help="Maximum number of candidates to show (default: 20)")
@click.option("--node", help="Only show candidates from this specific source node (e.g., data-hot-4)")
@click.pass_context
def find_candidates(ctx, table: Optional[str], min_size: float, max_size: float, limit: int, node: Optional[str]):
    """Find shard candidates for movement based on size criteria

    Results are sorted by nodes with least available space first,
    then by shard size (smallest first) for easier moves.
    """
    client = ctx.obj["client"]
    analyzer = ShardAnalyzer(client)

    console.print(Panel.fit(f"[bold blue]Finding Moveable Shards ({min_size}-{max_size}GB)[/bold blue]"))

    if node:
        console.print(f"[dim]Filtering: Only showing candidates from source node '{node}'[/dim]")

    # Find moveable candidates (only healthy shards suitable for operations)
    candidates = analyzer.find_moveable_shards(min_size, max_size, table)

    # Filter by node if specified
    if node:
        candidates = [c for c in candidates if c.node_name == node]

    if not candidates:
        if node:
            console.print(f"[yellow]No moveable shards found on node '{node}' in the specified size range.[/yellow]")
            console.print("[dim]Tip: Try different size ranges or remove --node filter to see all candidates[/dim]")
        else:
            console.print("[yellow]No moveable shards found in the specified size range.[/yellow]")
        return

    # Show limited results
    shown_candidates = candidates[:limit]

    candidates_table = Table(
        title=f"Moveable Shard Candidates (showing {len(shown_candidates)} of {len(candidates)})", box=box.ROUNDED
    )
    candidates_table.add_column("Table", style="cyan")
    candidates_table.add_column("Shard ID", justify="right", style="magenta")
    candidates_table.add_column("Type", style="blue")
    candidates_table.add_column("Node", style="green")
    candidates_table.add_column("Zone", style="yellow")
    candidates_table.add_column("Size", justify="right", style="red")
    candidates_table.add_column("Node Free Space", justify="right", style="white")
    candidates_table.add_column("Documents", justify="right", style="dim")

    # Create a mapping of node names to available space for display
    node_space_map = {node.name: node.available_space_gb for node in analyzer.nodes}

    for shard in shown_candidates:
        node_free_space = node_space_map.get(shard.node_name, 0)
        candidates_table.add_row(
            f"{shard.schema_name}.{shard.table_name}",
            str(shard.shard_id),
            shard.shard_type,
            shard.node_name,
            shard.zone,
            format_size(shard.size_gb),
            format_size(node_free_space),
            f"{shard.num_docs:,}",
        )

    console.print(candidates_table)

    if len(candidates) > limit:
        console.print(f"\n[dim]... and {len(candidates) - limit} more candidates[/dim]")


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
    min_size: float,
    max_size: float,
    zone_tolerance: float,
    min_free_space: float,
    max_moves: int,
    max_disk_usage: float,
    validate: bool,
    prioritize_space: bool,
    dry_run: bool,
    auto_execute: bool,
    node: Optional[str],
):
    """Generate shard movement recommendations for rebalancing"""
    client = ctx.obj["client"]
    analyzer = ShardAnalyzer(client)

    # Safety check for auto-execute
    if auto_execute and dry_run:
        console.print("[red]❌ Error: --auto-execute requires --execute flag[/red]")
        console.print("[dim]Use: --execute --auto-execute[/dim]")
        return

    mode_text = "DRY RUN - Analysis Only" if dry_run else "EXECUTION MODE"
    console.print(
        Panel.fit(
            f"[bold blue]Generating Rebalancing Recommendations[/bold blue] - "
            f"[bold {'green' if dry_run else 'red'}]{mode_text}[/bold {'green' if dry_run else 'red'}]"
        )
    )
    console.print("[dim]Note: Only analyzing healthy shards (STARTED + 100% recovered) for safe operations[/dim]")
    console.print("[dim]Zone conflict detection: Prevents moves that would violate CrateDB's zone awareness[/dim]")
    if prioritize_space:
        console.print("[dim]Mode: Prioritizing available space over zone balancing[/dim]")
    else:
        console.print("[dim]Mode: Prioritizing zone balancing over available space[/dim]")

    if node:
        console.print(f"[dim]Filtering: Only showing moves from source node '{node}'[/dim]")

    console.print(f"[dim]Safety thresholds: Max disk usage {max_disk_usage}%, Min free space {min_free_space}GB[/dim]")

    if dry_run:
        console.print("[green]Running in DRY RUN mode - no SQL commands will be generated[/green]")
    else:
        console.print("[red]EXECUTION MODE - SQL commands will be generated for actual moves[/red]")
    console.print()

    recommendations = analyzer.generate_rebalancing_recommendations(
        table_name=table,
        min_size_gb=min_size,
        max_size_gb=max_size,
        zone_tolerance_percent=zone_tolerance,
        min_free_space_gb=min_free_space,
        max_recommendations=max_moves,
        prioritize_space=prioritize_space,
        source_node=node,
        max_disk_usage_percent=max_disk_usage,
    )

    if not recommendations:
        if node:
            console.print(f"[yellow]No safe recommendations found for node '{node}'[/yellow]")
            console.print("[dim]This could be due to:[/dim]")
            console.print("[dim]  • Zone conflicts preventing safe moves[/dim]")
            console.print(f"[dim]  • Target nodes exceeding {max_disk_usage}% disk usage threshold[/dim]")
            console.print(f"[dim]  • Insufficient free space on target nodes (need {min_free_space}GB)[/dim]")
            console.print(f"[dim]  • No shards in size range {min_size}-{max_size}GB[/dim]")
            console.print("[dim]Suggestions:[/dim]")
            console.print("[dim]  • Try: --max-disk-usage 95 (allow higher disk usage)[/dim]")
            console.print("[dim]  • Try: --min-free-space 50 (reduce space requirements)[/dim]")
            console.print("[dim]  • Try: different size ranges or remove --node filter[/dim]")
        else:
            console.print("[green]No rebalancing recommendations needed. Cluster appears well balanced![/green]")
        return

    # Show recommendations table
    rec_table = Table(title=f"Rebalancing Recommendations ({len(recommendations)} moves)", box=box.ROUNDED)
    rec_table.add_column("Table", style="cyan")
    rec_table.add_column("Shard", justify="right", style="magenta")
    rec_table.add_column("Type", style="blue")
    rec_table.add_column("From Node", style="red")
    rec_table.add_column("To Node", style="green")
    rec_table.add_column("Target Free Space", justify="right", style="cyan")
    rec_table.add_column("Zone Change", style="yellow")
    rec_table.add_column("Size", justify="right", style="white")
    rec_table.add_column("Reason", style="dim")
    if validate:
        rec_table.add_column("Safety Check", style="bold")

    # Create a mapping of node names to available space for display
    node_space_map = {node.name: node.available_space_gb for node in analyzer.nodes}

    for rec in recommendations:
        zone_change = f"{rec.from_zone} → {rec.to_zone}" if rec.from_zone != rec.to_zone else rec.from_zone
        target_free_space = node_space_map.get(rec.to_node, 0)

        row = [
            f"{rec.schema_name}.{rec.table_name}",
            str(rec.shard_id),
            rec.shard_type,
            rec.from_node,
            rec.to_node,
            format_size(target_free_space),
            zone_change,
            format_size(rec.size_gb),
            rec.reason,
        ]

        if validate:
            is_safe, safety_msg = analyzer.validate_move_safety(rec, max_disk_usage_percent=max_disk_usage)
            safety_status = "[green]✓ SAFE[/green]" if is_safe else f"[red]✗ {safety_msg}[/red]"
            row.append(safety_status)

        rec_table.add_row(*row)

    console.print(rec_table)
    console.print()

    # Generate SQL commands or show dry-run analysis
    if dry_run:
        console.print(Panel.fit("[bold yellow]Dry Run Analysis - No Commands Generated[/bold yellow]"))
        console.print("[dim]# This is a dry run - showing what would be recommended[/dim]")
        console.print("[dim]# Use --execute flag to generate actual SQL commands[/dim]")
        console.print()

        safe_moves = 0
        zone_conflicts = 0
        space_issues = 0

        for i, rec in enumerate(recommendations, 1):
            if validate:
                is_safe, safety_msg = analyzer.validate_move_safety(rec, max_disk_usage_percent=max_disk_usage)
                if not is_safe:
                    if "zone conflict" in safety_msg.lower():
                        zone_conflicts += 1
                        console.print(f"[yellow]⚠ Move {i}: WOULD BE SKIPPED - {safety_msg}[/yellow]")
                    elif "space" in safety_msg.lower():
                        space_issues += 1
                        console.print(f"[yellow]⚠ Move {i}: WOULD BE SKIPPED - {safety_msg}[/yellow]")
                    else:
                        console.print(f"[yellow]⚠ Move {i}: WOULD BE SKIPPED - {safety_msg}[/yellow]")
                    continue
                safe_moves += 1

            console.print(f"[green]✓ Move {i}: WOULD EXECUTE - {rec.reason}[/green]")
            console.print(f"[dim]  Target SQL: {rec.to_sql()}[/dim]")

        console.print()
        console.print("[bold]Dry Run Summary:[/bold]")
        console.print(f"  • Safe moves that would execute: [green]{safe_moves}[/green]")
        console.print(f"  • Zone conflicts prevented: [yellow]{zone_conflicts}[/yellow]")
        console.print(f"  • Space-related issues: [yellow]{space_issues}[/yellow]")
        if safe_moves > 0:
            console.print(
                f"\n[green]✓ Ready to execute {safe_moves} safe moves. Use --execute to generate SQL commands.[/green]"
            )
        else:
            console.print("\n[yellow]⚠ No safe moves identified. Review cluster balance or adjust parameters.[/yellow]")
    else:
        console.print(Panel.fit("[bold green]Generated SQL Commands[/bold green]"))
        console.print("[dim]# Copy and paste these commands to execute the moves[/dim]")
        console.print("[dim]# ALWAYS test in a non-production environment first![/dim]")
        console.print("[dim]# These commands only operate on healthy shards (STARTED + fully recovered)[/dim]")
        console.print("[dim]# Commands use quoted identifiers for schema and table names[/dim]")
        console.print()

        safe_moves = 0
        zone_conflicts = 0
        for i, rec in enumerate(recommendations, 1):
            if validate:
                is_safe, safety_msg = analyzer.validate_move_safety(rec, max_disk_usage_percent=max_disk_usage)
                if not is_safe:
                    if "Zone conflict" in safety_msg:
                        zone_conflicts += 1
                        console.print(f"-- Move {i}: SKIPPED - {safety_msg}")
                        console.print("--   Tip: Try moving to a different zone or check existing shard distribution")
                    else:
                        console.print(f"-- Move {i}: SKIPPED - {safety_msg}")
                    continue
                safe_moves += 1

            console.print(f"-- Move {i}: {rec.reason}")
            console.print(f"{rec.to_sql()}")
        console.print()

        # Auto-execution if requested
        if auto_execute:
            _execute_recommendations_safely(client, recommendations, validate)

    if validate and safe_moves < len(recommendations):
        if zone_conflicts > 0:
            console.print(f"[yellow]Warning: {zone_conflicts} moves skipped due to zone conflicts[/yellow]")
            console.print("[yellow]Tip: Use 'find-candidates' to see current shard distribution across zones[/yellow]")
        console.print(
            f"[yellow]Warning: Only {safe_moves} of {len(recommendations)} moves passed safety validation[/yellow]"
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
    analyzer = ShardAnalyzer(client)

    console.print(Panel.fit("[bold blue]Zone Balance Check[/bold blue]"))
    console.print("[dim]Note: Analyzing all shards regardless of state for complete cluster view[/dim]")
    console.print()

    zone_stats = analyzer.check_zone_balance(table, tolerance)

    if not zone_stats:
        console.print("[yellow]No shards found for analysis[/yellow]")
        return

    # Calculate totals and targets
    total_shards = sum(stats["TOTAL"] for stats in zone_stats.values())
    zones = list(zone_stats.keys())
    target_per_zone = total_shards // len(zones) if zones else 0
    tolerance_range = (target_per_zone * (1 - tolerance / 100), target_per_zone * (1 + tolerance / 100))

    balance_table = Table(title=f"Zone Balance Analysis (Target: {target_per_zone} ±{tolerance}%)", box=box.ROUNDED)
    balance_table.add_column("Zone", style="cyan")
    balance_table.add_column("Primary", justify="right", style="blue")
    balance_table.add_column("Replica", justify="right", style="green")
    balance_table.add_column("Total", justify="right", style="magenta")
    balance_table.add_column("Status", style="bold")

    for zone, stats in zone_stats.items():
        total = stats["TOTAL"]

        if tolerance_range[0] <= total <= tolerance_range[1]:
            status = "[green]✓ Balanced[/green]"
        elif total < tolerance_range[0]:
            status = f"[yellow]⚠ Under ({total - target_per_zone:+})[/yellow]"
        else:
            status = f"[red]⚠ Over ({total - target_per_zone:+})[/red]"

        balance_table.add_row(zone, str(stats["PRIMARY"]), str(stats["REPLICA"]), str(total), status)

    console.print(balance_table)


@main.command()
@click.option("--table", "-t", help="Analyze zones for specific table only")
@click.option("--show-shards/--no-show-shards", default=False, help="Show individual shard details (default: False)")
@click.pass_context
def zone_analysis(ctx, table: Optional[str], show_shards: bool):
    """Detailed analysis of zone distribution and potential conflicts"""
    client = ctx.obj["client"]

    console.print(Panel.fit("[bold blue]Detailed Zone Analysis[/bold blue]"))
    console.print("[dim]Comprehensive zone distribution analysis for CrateDB cluster[/dim]")
    console.print()

    # Get all shards for analysis
    shards = client.get_shards_info(table_name=table, for_analysis=True)

    if not shards:
        console.print("[yellow]No shards found for analysis[/yellow]")
        return

    # Organize by table and shard
    tables: Dict[str, Dict[str, List[ShardInfo]]] = {}
    for shard in shards:
        table_key = f"{shard.schema_name}.{shard.table_name}"
        if table_key not in tables:
            tables[table_key] = {}

        shard_key = shard.shard_id
        if shard_key not in tables[table_key]:
            tables[table_key][shard_key] = []

        tables[table_key][shard_key].append(shard)

    # Analyze each table
    zone_conflicts = 0
    under_replicated = 0

    for table_name, table_shards in tables.items():
        console.print(f"\n[bold cyan]Table: {table_name}[/bold cyan]")

        # Create analysis table
        analysis_table = Table(title=f"Shard Distribution for {table_name}", box=box.ROUNDED)
        analysis_table.add_column("Shard ID", justify="right", style="magenta")
        analysis_table.add_column("Primary Zone", style="blue")
        analysis_table.add_column("Replica Zones", style="green")
        analysis_table.add_column("Total Copies", justify="right", style="cyan")
        analysis_table.add_column("Status", style="bold")

        for shard_id, shard_copies in sorted(table_shards.items()):
            primary_zone = "Unknown"
            replica_zones = set()
            total_copies = len(shard_copies)
            zones_with_copies = set()

            for shard_copy in shard_copies:
                zones_with_copies.add(shard_copy.zone)
                if shard_copy.is_primary:
                    primary_zone = shard_copy.zone
                else:
                    replica_zones.add(shard_copy.zone)

            # Determine status
            status_parts = []
            if len(zones_with_copies) == 1:
                zone_conflicts += 1
                status_parts.append("[red]⚠ ZONE CONFLICT[/red]")

            if total_copies < 2:  # Assuming we want at least 1 replica
                under_replicated += 1
                status_parts.append("[yellow]⚠ Under-replicated[/yellow]")

            if not status_parts:
                status_parts.append("[green]✓ Good[/green]")

            replica_zones_str = ", ".join(sorted(replica_zones)) if replica_zones else "None"

            analysis_table.add_row(
                str(shard_id), primary_zone, replica_zones_str, str(total_copies), " ".join(status_parts)
            )

            # Show individual shard details if requested
            if show_shards:
                for shard_copy in shard_copies:
                    health_indicator = "✓" if shard_copy.routing_state == "STARTED" else "⚠"
                    console.print(
                        f"    {health_indicator} {shard_copy.shard_type} "
                        f"on {shard_copy.node_name} ({shard_copy.zone}) - {shard_copy.routing_state}"
                    )

        console.print(analysis_table)

    # Summary
    console.print("\n[bold]Zone Analysis Summary:[/bold]")
    console.print(f"  • Tables analyzed: [cyan]{len(tables)}[/cyan]")
    console.print(f"  • Zone conflicts detected: [red]{zone_conflicts}[/red]")
    console.print(f"  • Under-replicated shards: [yellow]{under_replicated}[/yellow]")

    if zone_conflicts > 0:
        console.print(f"\n[red]⚠ Found {zone_conflicts} zone conflicts that need attention![/red]")
        console.print("[dim]Zone conflicts occur when all copies of a shard are in the same zone.[/dim]")
        console.print("[dim]This violates CrateDB's zone-awareness and creates availability risks.[/dim]")

    if under_replicated > 0:
        console.print(f"\n[yellow]⚠ Found {under_replicated} under-replicated shards.[/yellow]")
        console.print("[dim]Consider increasing replication for better availability.[/dim]")

    if zone_conflicts == 0 and under_replicated == 0:
        console.print("\n[green]✓ No critical zone distribution issues detected![/green]")


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

    Example: xmover validate-move CUROV.maddoxxFormfactor 4 data-hot-1 data-hot-3
    """
    client = ctx.obj["client"]
    analyzer = ShardAnalyzer(client)

    # Parse schema and table
    if "." not in schema_table:
        console.print("[red]Error: Schema and table must be in format 'schema.table'[/red]")
        return

    schema_name, table_name = schema_table.split(".", 1)

    console.print(Panel.fit("[bold blue]Validating Shard Move[/bold blue]"))
    console.print(f"[dim]Move: {schema_name}.{table_name}[{shard_id}] from {from_node} to {to_node}[/dim]")
    console.print()

    # Find the nodes
    from_node_info = None
    to_node_info = None
    for node in analyzer.nodes:
        if node.name == from_node:
            from_node_info = node
        if node.name == to_node:
            to_node_info = node

    if not from_node_info:
        console.print(f"[red]✗ Source node '{from_node}' not found in cluster[/red]")
        return

    if not to_node_info:
        console.print(f"[red]✗ Target node '{to_node}' not found in cluster[/red]")
        return

    # Find the specific shard
    target_shard = None
    for shard in analyzer.shards:
        if (
            shard.schema_name == schema_name
            and shard.table_name == table_name
            and shard.shard_id == shard_id
            and shard.node_name == from_node
        ):
            target_shard = shard
            break

    if not target_shard:
        console.print(f"[red]✗ Shard {shard_id} not found on node {from_node}[/red]")
        console.print("[dim]Use 'xmover find-candidates' to see available shards[/dim]")
        return

    # Create a move recommendation for validation
    recommendation = MoveRecommendation(
        table_name=table_name,
        schema_name=schema_name,
        shard_id=shard_id,
        from_node=from_node,
        to_node=to_node,
        from_zone=from_node_info.zone,
        to_zone=to_node_info.zone,
        shard_type=target_shard.shard_type,
        size_gb=target_shard.size_gb,
        reason="Manual validation",
    )

    # Display shard details
    details_table = Table(title="Shard Details", box=box.ROUNDED)
    details_table.add_column("Property", style="cyan")
    details_table.add_column("Value", style="magenta")

    details_table.add_row("Table", f"{schema_name}.{table_name}")
    details_table.add_row("Shard ID", str(shard_id))
    details_table.add_row("Type", target_shard.shard_type)
    details_table.add_row("Size", format_size(target_shard.size_gb))
    details_table.add_row("Documents", f"{target_shard.num_docs:,}")
    details_table.add_row("State", target_shard.state)
    details_table.add_row("Routing State", target_shard.routing_state)
    details_table.add_row("From Node", f"{from_node} ({from_node_info.zone})")
    details_table.add_row("To Node", f"{to_node} ({to_node_info.zone})")
    details_table.add_row("Zone Change", "Yes" if from_node_info.zone != to_node_info.zone else "No")

    console.print(details_table)
    console.print()

    # Perform comprehensive validation
    is_safe, safety_msg = analyzer.validate_move_safety(recommendation, max_disk_usage_percent=max_disk_usage)

    if is_safe:
        console.print("[green]✓ VALIDATION PASSED - Move appears safe[/green]")
        console.print(f"[green]✓ {safety_msg}[/green]")
        console.print()

        # Show the SQL command
        console.print(Panel.fit("[bold green]Ready to Execute[/bold green]"))
        console.print("[dim]# Copy and paste this command to execute the move[/dim]")
        console.print()
        console.print(f"{recommendation.to_sql()}")
        console.print()
        console.print("[dim]# Monitor shard health after execution[/dim]")
        console.print(
            "[dim]# Check with: SELECT * FROM sys.shards WHERE table_name = '{table_name}' AND id = {shard_id};[/dim]"
        )
    else:
        console.print("[red]✗ VALIDATION FAILED - Move not safe[/red]")
        console.print(f"[red]✗ {safety_msg}[/red]")
        console.print()

        # Provide troubleshooting guidance
        if "zone conflict" in safety_msg.lower():
            console.print("[yellow]💡 Troubleshooting Zone Conflicts:[/yellow]")
            console.print("  • Check current shard distribution: xmover zone-analysis --show-shards")
            console.print("  • Try moving to a different zone")
            console.print("  • Verify cluster has proper zone-awareness configuration")
        elif "node conflict" in safety_msg.lower():
            console.print("[yellow]💡 Troubleshooting Node Conflicts:[/yellow]")
            console.print("  • The target node already has a copy of this shard")
            console.print("  • Choose a different target node")
            console.print("  • Check shard distribution: xmover analyze")
        elif "space" in safety_msg.lower():
            console.print("[yellow]💡 Troubleshooting Space Issues:[/yellow]")
            console.print("  • Free up space on the target node")
            console.print("  • Choose a node with more available capacity")
            console.print("  • Check node capacity: xmover analyze")
        elif "usage" in safety_msg.lower():
            console.print("[yellow]💡 Troubleshooting High Disk Usage:[/yellow]")
            console.print("  • Wait for target node disk usage to decrease")
            console.print("  • Choose a node with lower disk usage")
            console.print("  • Check cluster health: xmover analyze")
            console.print("  • Consider using --max-disk-usage option for urgent moves")


@main.command()
@click.argument("error_message", required=False)
@click.pass_context
def explain_error(ctx, error_message: Optional[str]):
    """Explain CrateDB allocation error messages and provide solutions

    ERROR_MESSAGE: The CrateDB error message to analyze (optional - can be provided interactively)

    Example: xmover explain-error "NO(a copy of this shard is already allocated to this node)"
    """
    console.print(Panel.fit("[bold blue]CrateDB Error Message Decoder[/bold blue]"))
    console.print("[dim]Helps decode and troubleshoot CrateDB shard allocation errors[/dim]")
    console.print()

    if not error_message:
        console.print("Please paste the CrateDB error message (press Enter twice when done):")
        lines: List[str] = []
        while True:
            try:
                line = input()
                if line.strip() == "" and lines:
                    break
                lines.append(line)
            except (EOFError, KeyboardInterrupt):
                break
        error_message = "\n".join(lines)

    if not error_message.strip():
        console.print("[yellow]No error message provided[/yellow]")
        return

    console.print("[dim]Analyzing error message...[/dim]")
    console.print()

    # Common CrateDB allocation error patterns and solutions
    error_patterns = [
        {
            "pattern": "a copy of this shard is already allocated to this node",
            "title": "Node Already Has Shard Copy",
            "explanation": "The target node already contains a copy (primary or replica) of this shard.",
            "solutions": [
                "Choose a different target node that doesn't have this shard",
                "Use 'xmover zone-analysis --show-shards' to see current distribution",
                "Verify the shard ID and table name are correct",
            ],
            "prevention": "Always check current shard locations before moving",
        },
        {
            "pattern": "there are too many copies of the shard allocated to nodes with attribute",
            "title": "Zone Allocation Limit Exceeded",
            "explanation": "CrateDB's zone awareness prevents too many copies in the same zone.",
            "solutions": [
                "Move the shard to a different availability zone",
                "Check zone balance with 'xmover check-balance'",
                "Ensure target zone doesn't already have copies of this shard",
            ],
            "prevention": "Use 'xmover recommend' which respects zone constraints",
        },
        {
            "pattern": "not enough disk space",
            "title": "Insufficient Disk Space",
            "explanation": "The target node doesn't have enough free disk space for the shard.",
            "solutions": [
                "Free up space on the target node",
                "Choose a node with more available capacity",
                "Check available space with 'xmover analyze'",
            ],
            "prevention": "Use '--min-free-space' parameter in recommendations",
        },
        {
            "pattern": "shard recovery limit",
            "title": "Recovery Limit Exceeded",
            "explanation": "Too many shards are currently being moved/recovered simultaneously.",
            "solutions": [
                "Wait for current recoveries to complete",
                "Check recovery status in CrateDB admin UI",
                "Reduce concurrent recoveries in cluster settings",
            ],
            "prevention": "Move shards gradually, monitor recovery progress",
        },
        {
            "pattern": "allocation is disabled",
            "title": "Allocation Disabled",
            "explanation": "Shard allocation is temporarily disabled in the cluster.",
            "solutions": [
                "Re-enable allocation: PUT /_cluster/settings "
                '{"persistent":{"cluster.routing.allocation.enable":"all"}}',
                "Check if allocation was disabled for maintenance",
                "Verify cluster health before re-enabling",
            ],
            "prevention": "Check allocation status before performing moves",
        },
    ]

    # Find matching patterns
    matches = []
    error_lower = error_message.lower()

    for pattern_info in error_patterns:
        if cast(str, pattern_info["pattern"]).lower() in error_lower:
            matches.append(pattern_info)

    if matches:
        for i, match in enumerate(matches):
            if i > 0:
                console.print("\n" + "─" * 60 + "\n")

            console.print(f"[bold red]🚨 {match['title']}[/bold red]")
            console.print(f"[yellow]📝 Explanation:[/yellow] {match['explanation']}")
            console.print()

            console.print("[green]💡 Solutions:[/green]")
            for j, solution in enumerate(match["solutions"], 1):
                console.print(f"  {j}. {solution}")
            console.print()

            console.print(f"[blue]🛡️ Prevention:[/blue] {match['prevention']}")
    else:
        console.print("[yellow]⚠ No specific pattern match found[/yellow]")
        console.print()
        console.print("[bold]General Troubleshooting Steps:[/bold]")
        console.print("1. Check current shard distribution: [cyan]xmover analyze[/cyan]")
        console.print(
            "2. Validate the specific move: [cyan]xmover validate-move schema.table shard_id from_node to_node[/cyan]"
        )
        console.print("3. Check zone conflicts: [cyan]xmover zone-analysis --show-shards[/cyan]")
        console.print("4. Verify node capacity: [cyan]xmover analyze[/cyan]")
        console.print("5. Review CrateDB documentation on shard allocation")

    console.print()
    console.print("[dim]💡 Tip: Use 'xmover validate-move' to check moves before execution[/dim]")
    console.print(
        "[dim]📚 For more help: https://crate.io/docs/crate/reference/en/latest/admin/system-information.html[/dim]"
    )


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
    try:
        client = ctx.obj["client"]
        recovery_monitor = RecoveryMonitor(client)

        if watch:
            console.print(f"🔄 Monitoring shard recoveries (refreshing every {refresh_interval}s)")
            console.print("Press Ctrl+C to stop")
            console.print()

            try:
                # Show header once
                console.print("📊 Recovery Progress Monitor")
                console.print("=" * 80)

                # Track previous state for change detection
                previous_recoveries: Dict[str, Dict[str, Any]] = {}
                previous_timestamp = None
                first_run = True

                while True:
                    # Get current recovery status
                    recoveries = recovery_monitor.get_cluster_recovery_status(
                        table_name=table,
                        node_name=node,
                        recovery_type_filter=recovery_type,
                        include_transitioning=include_transitioning,
                    )

                    # Display current time
                    from datetime import datetime

                    current_time = datetime.now().strftime("%H:%M:%S")

                    # Check for any changes
                    changes = []
                    active_count = 0
                    completed_count = 0

                    for recovery in recoveries:
                        recovery_key = (
                            f"{recovery.schema_name}.{recovery.table_name}.{recovery.shard_id}.{recovery.node_name}"
                        )

                        # Create complete table name
                        if recovery.schema_name == "doc":
                            table_display = recovery.table_name
                        else:
                            table_display = f"{recovery.schema_name}.{recovery.table_name}"

                        # Count active vs completed
                        if recovery.stage == "DONE" and recovery.overall_progress >= 100.0:
                            completed_count += 1
                        else:
                            active_count += 1

                        # Check for changes since last update
                        if recovery_key in previous_recoveries:
                            prev = previous_recoveries[recovery_key]
                            if prev["progress"] != recovery.overall_progress:
                                diff = recovery.overall_progress - prev["progress"]
                                # Create node route display
                                node_route = ""
                                if recovery.recovery_type == "PEER" and recovery.source_node_name:
                                    node_route = f" {recovery.source_node_name} → {recovery.node_name}"
                                elif recovery.recovery_type == "DISK":
                                    node_route = f" disk → {recovery.node_name}"

                                # Add translog info
                                translog_info = format_translog_info(recovery)

                                if diff > 0:
                                    changes.append(
                                        f"[green]📈[/green] {table_display} S{recovery.shard_id} "
                                        f"{recovery.overall_progress:.1f}% (+{diff:.1f}%) "
                                        f"{recovery.size_gb:.1f}GB{translog_info}{node_route}"
                                    )
                                else:
                                    changes.append(
                                        f"[yellow]📉[/yellow] {table_display} S{recovery.shard_id} "
                                        f"{recovery.overall_progress:.1f}% ({diff:.1f}%) "
                                        f"{recovery.size_gb:.1f}GB{translog_info}{node_route}"
                                    )
                            elif prev["stage"] != recovery.stage:
                                # Create node route display
                                node_route = ""
                                if recovery.recovery_type == "PEER" and recovery.source_node_name:
                                    node_route = f" {recovery.source_node_name} → {recovery.node_name}"
                                elif recovery.recovery_type == "DISK":
                                    node_route = f" disk → {recovery.node_name}"

                                # Add translog info
                                translog_info = format_translog_info(recovery)

                                changes.append(
                                    f"[blue]🔄[/blue] {table_display} S{recovery.shard_id} "
                                    f"{prev['stage']}→{recovery.stage} "
                                    f"{recovery.size_gb:.1f}GB{translog_info}{node_route}"
                                )
                        else:
                            # New recovery - show based on include_transitioning flag or first run
                            if (
                                first_run
                                or include_transitioning
                                or (recovery.overall_progress < 100.0 or recovery.stage != "DONE")
                            ):
                                # Create node route display
                                node_route = ""
                                if recovery.recovery_type == "PEER" and recovery.source_node_name:
                                    node_route = f" {recovery.source_node_name} → {recovery.node_name}"
                                elif recovery.recovery_type == "DISK":
                                    node_route = f" disk → {recovery.node_name}"

                                status_icon = "[cyan]🆕[/cyan]" if not first_run else "[blue]📋[/blue]"
                                # Add translog info
                                translog_info = format_translog_info(recovery)

                                changes.append(
                                    f"{status_icon} {table_display} S{recovery.shard_id} "
                                    f"{recovery.stage} {recovery.overall_progress:.1f}% "
                                    f"{recovery.size_gb:.1f}GB{translog_info}{node_route}"
                                )

                        # Store current state for next comparison
                        previous_recoveries[recovery_key] = {
                            "progress": recovery.overall_progress,
                            "stage": recovery.stage,
                        }

                    # Always show a status line
                    if not recoveries:
                        console.print(f"{current_time} | [green]No recoveries - cluster stable[/green]")
                        previous_recoveries.clear()
                    else:
                        # Build status message
                        status = ""
                        if active_count > 0:
                            status = f"{active_count} active"
                        if completed_count > 0:
                            status += f", {completed_count} done" if status else f"{completed_count} done"

                        # Show status line with changes or periodic update
                        if changes:
                            console.print(f"{current_time} | {status}")
                            for change in changes:
                                console.print(f"         | {change}")
                        else:
                            # Show periodic status even without changes
                            if include_transitioning and completed_count > 0:
                                console.print(f"{current_time} | {status} (transitioning)")
                            elif active_count > 0:
                                console.print(f"{current_time} | {status} (no changes)")

                    previous_timestamp = current_time  # noqa: F841
                    first_run = False
                    time.sleep(refresh_interval)

            except KeyboardInterrupt:
                console.print("\n\n[yellow]⏹  Monitoring stopped by user[/yellow]")

                # Show final summary
                final_recoveries = recovery_monitor.get_cluster_recovery_status(
                    table_name=table,
                    node_name=node,
                    recovery_type_filter=recovery_type,
                    include_transitioning=include_transitioning,
                )

                if final_recoveries:
                    console.print("\n📊 [bold]Final Recovery Summary:[/bold]")
                    summary = recovery_monitor.get_recovery_summary(final_recoveries)

                    # Count active vs completed
                    active_count = len([r for r in final_recoveries if r.overall_progress < 100.0 or r.stage != "DONE"])
                    completed_count = len(final_recoveries) - active_count

                    console.print(f"   Total recoveries: {summary['total_recoveries']}")
                    console.print(f"   Active: {active_count}, Completed: {completed_count}")
                    console.print(f"   Total size: {summary['total_size_gb']:.1f} GB")
                    console.print(f"   Average progress: {summary['avg_progress']:.1f}%")

                    if summary["by_type"]:
                        console.print("   By recovery type:")
                        for rec_type, stats in summary["by_type"].items():
                            console.print(
                                f"     {rec_type}: {stats['count']} recoveries, "
                                f"{stats['avg_progress']:.1f}% avg progress"
                            )
                else:
                    console.print("\n[green]✅ No active recoveries at exit[/green]")

                return

        else:
            # Single status check
            recoveries = recovery_monitor.get_cluster_recovery_status(
                table_name=table,
                node_name=node,
                recovery_type_filter=recovery_type,
                include_transitioning=include_transitioning,
            )

            display_output = recovery_monitor.format_recovery_display(recoveries)
            console.print(display_output)

            if not recoveries:
                if include_transitioning:
                    console.print("\n[green]✅ No recoveries found (active or transitioning)[/green]")
                else:
                    console.print("\n[green]✅ No active recoveries found[/green]")
                    console.print(
                        "[dim]💡 Use --include-transitioning to see completed recoveries still transitioning[/dim]"
                    )
            else:
                # Show summary
                summary = recovery_monitor.get_recovery_summary(recoveries)
                console.print("\n📊 [bold]Recovery Summary:[/bold]")
                console.print(f"   Total recoveries: {summary['total_recoveries']}")
                console.print(f"   Total size: {summary['total_size_gb']:.1f} GB")
                console.print(f"   Average progress: {summary['avg_progress']:.1f}%")

                # Show breakdown by type
                if summary["by_type"]:
                    console.print("\n   By recovery type:")
                    for rec_type, stats in summary["by_type"].items():
                        console.print(
                            f"     {rec_type}: {stats['count']} recoveries, {stats['avg_progress']:.1f}% avg progress"
                        )

                console.print("\n[dim]💡 Use --watch flag for continuous monitoring[/dim]")

    except Exception as e:
        console.print(f"[red]❌ Error monitoring recoveries: {e}[/red]")
        if ctx.obj.get("debug"):
            raise


def _wait_for_recovery_capacity(client, max_concurrent_recoveries: int = 5):
    """Wait until active recovery count is below threshold"""
    from time import sleep

    from .analyzer import RecoveryMonitor

    recovery_monitor = RecoveryMonitor(client)
    wait_time = 0

    while True:
        # Check active recoveries (including transitioning)
        recoveries = recovery_monitor.get_cluster_recovery_status(include_transitioning=True)
        active_count = len([r for r in recoveries if r.overall_progress < 100.0 or r.stage != "DONE"])
        status = f"{active_count}/{max_concurrent_recoveries}"
        if active_count < max_concurrent_recoveries:
            if wait_time > 0:
                console.print(f"    [green]✓ Recovery capacity available ({status} active)[/green]")
            break
        if wait_time == 0:
            console.print(f"    [yellow]⏳ Waiting for recovery capacity... ({status} active)[/yellow]")
        elif wait_time % 30 == 0:  # Update every 30 seconds
            console.print(f"    [yellow]⏳ Still waiting... ({status} active)[/yellow]")

        sleep(10)  # Check every 10 seconds
        wait_time += 10


def _execute_recommendations_safely(client, recommendations, validate: bool):
    """Execute recommendations with extensive safety measures"""

    from .analyzer import ShardAnalyzer

    # Filter to only safe recommendations
    safe_recommendations = []
    if validate:
        analyzer = ShardAnalyzer(client)
        for rec in recommendations:
            is_safe, safety_msg = analyzer.validate_move_safety(rec, max_disk_usage_percent=95.0)
            if is_safe:
                safe_recommendations.append(rec)
    else:
        safe_recommendations = recommendations

    if not safe_recommendations:
        console.print("[yellow]⚠ No safe recommendations to execute[/yellow]")
        return

    console.print("\n[bold red]🚨 AUTO-EXECUTION MODE 🚨[/bold red]")
    console.print(f"About to execute {len(safe_recommendations)} shard moves automatically:")
    console.print()

    # Show what will be executed
    for i, rec in enumerate(safe_recommendations, 1):
        table_display = f"{rec.schema_name}.{rec.table_name}" if rec.schema_name != "doc" else rec.table_name
        console.print(f"  {i}. {table_display} S{rec.shard_id} ({rec.size_gb:.1f}GB) {rec.from_node} → {rec.to_node}")

    console.print()
    console.print("[bold yellow]⚠ SAFETY WARNINGS:[/bold yellow]")
    console.print("  • These commands will immediately start shard movements")
    console.print("  • Each move will temporarily impact cluster performance")
    console.print("  • Recovery time depends on shard size and network speed")
    console.print("  • You should monitor progress with: xmover monitor-recovery --watch")
    console.print()

    # Double confirmation
    try:
        response1 = input("Type 'EXECUTE' to proceed with automatic execution: ").strip()
        if response1 != "EXECUTE":
            console.print("[yellow]❌ Execution cancelled[/yellow]")
            return

        response2 = input(f"Confirm: Execute {len(safe_recommendations)} shard moves? (yes/no): ").strip().lower()
        if response2 not in ["yes", "y"]:
            console.print("[yellow]❌ Execution cancelled[/yellow]")
            return

    except KeyboardInterrupt:
        console.print("\n[yellow]❌ Execution cancelled by user[/yellow]")
        return

    console.print(f"\n🚀 [bold green]Executing {len(safe_recommendations)} shard moves...[/bold green]")
    console.print()

    successful_moves = 0
    failed_moves = 0

    for i, rec in enumerate(safe_recommendations, 1):
        table_display = f"{rec.schema_name}.{rec.table_name}" if rec.schema_name != "doc" else rec.table_name
        sql_command = rec.to_sql()

        console.print(
            f"[{i}/{len(safe_recommendations)}] Executing: {table_display} S{rec.shard_id} ({rec.size_gb:.1f}GB)"
        )
        console.print(f"    {rec.from_node} → {rec.to_node}")

        try:
            # Execute the SQL command
            result = client.execute_query(sql_command)

            if result.get("rowcount", 0) >= 0:  # Success indicator for ALTER statements
                console.print("    [green]✅ SUCCESS[/green] - Move initiated")
                successful_moves += 1

                # Smart delay: check active recoveries before next move
                if i < len(safe_recommendations):
                    _wait_for_recovery_capacity(client, max_concurrent_recoveries=5)
            else:
                console.print(f"    [red]❌ FAILED[/red] - Unexpected result: {result}")
                failed_moves += 1

        except Exception as e:
            console.print(f"    [red]❌ FAILED[/red] - Error: {e}")
            failed_moves += 1

            # Ask whether to continue after a failure
            if i < len(safe_recommendations):
                try:
                    continue_response = (
                        input(f"    Continue with remaining {len(safe_recommendations) - i} moves? (yes/no): ")
                        .strip()
                        .lower()
                    )
                    if continue_response not in ["yes", "y"]:
                        console.print("[yellow]⏹ Execution stopped by user[/yellow]")
                        break
                except KeyboardInterrupt:
                    console.print("\n[yellow]⏹ Execution stopped by user[/yellow]")
                    break

        console.print()

    # Final summary
    console.print("📊 [bold]Execution Summary:[/bold]")
    console.print(f"   Successful moves: [green]{successful_moves}[/green]")
    console.print(f"   Failed moves: [red]{failed_moves}[/red]")
    console.print(f"   Total attempted: {successful_moves + failed_moves}")

    if successful_moves > 0:
        console.print()
        console.print("[green]✅ Shard moves initiated successfully![/green]")
        console.print("[dim]💡 Monitor progress with:[/dim]")
        console.print("[dim]   xmover monitor-recovery --watch[/dim]")
        console.print("[dim]💡 Check cluster status with:[/dim]")
        console.print("[dim]   xmover analyze[/dim]")

    if failed_moves > 0:
        console.print()
        console.print(f"[yellow]⚠ {failed_moves} moves failed - check cluster status and retry if needed[/yellow]")


if __name__ == "__main__":
    main()
