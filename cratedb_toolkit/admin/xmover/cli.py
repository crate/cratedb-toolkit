"""
XMover - CrateDB Shard Analyzer and Movement Tool

Command Line Interface.
"""

import sys
from typing import List, Optional, cast

import click
from rich.console import Console
from rich.panel import Panel

from cratedb_toolkit.admin.xmover.analyze.report import ShardReporter
from cratedb_toolkit.admin.xmover.analyze.shard import ShardAnalyzer
from cratedb_toolkit.admin.xmover.analyze.zone import ZoneReport
from cratedb_toolkit.admin.xmover.model import (
    RecommendationConstraints,
    ShardMoveRequest,
    SizeCriteria,
)
from cratedb_toolkit.admin.xmover.recommender import Recommender

from .database import CrateDBClient
from .recovery import RecoveryMonitor, RecoveryOptions

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
    reporter = ShardReporter(analyzer)
    reporter.movement_candidates(
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
    recommender = Recommender(
        client=ctx.obj["client"],
        constraints=RecommendationConstraints(
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
    )
    recommender.start(auto_execute=auto_execute, validate=validate, dry_run=dry_run)


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

    Example: xmover validate-move CUROV.maddoxxFormfactor 4 data-hot-1 data-hot-3
    """
    client = ctx.obj["client"]
    analyzer = ShardAnalyzer(client)
    reporter = ShardReporter(analyzer)
    reporter.validate_move(
        request=ShardMoveRequest(
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
