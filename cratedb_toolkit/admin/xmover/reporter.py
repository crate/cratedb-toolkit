from typing import Any, Dict

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from cratedb_toolkit.admin.xmover.analyzer import ShardAnalyzer
from cratedb_toolkit.admin.xmover.model import ShardMoveRecommendation, ShardMoveRequest, SizeCriteria
from cratedb_toolkit.admin.xmover.util import format_percentage, format_size

console = Console()


class ShardReporter:
    def __init__(self, analyzer: ShardAnalyzer):
        self.analyzer = analyzer

    def distribution(self, table: str = None):
        """Analyze current shard distribution across nodes and zones"""
        console.print(Panel.fit("[bold blue]CrateDB Cluster Analysis[/bold blue]"))

        # Get cluster overview (includes all shards for complete analysis)
        overview: Dict[str, Any] = self.analyzer.get_cluster_overview()

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

            stats = self.analyzer.analyze_distribution(table)

            table_summary = Table(title=f"Table {table} Distribution", box=box.ROUNDED)
            table_summary.add_column("Metric", style="cyan")
            table_summary.add_column("Value", style="magenta")

            table_summary.add_row("Total Shards", str(stats.total_shards))
            table_summary.add_row("Total Size", format_size(stats.total_size_gb))
            table_summary.add_row("Zone Balance Score", f"{stats.zone_balance_score:.1f}/100")
            table_summary.add_row("Node Balance Score", f"{stats.node_balance_score:.1f}/100")

            console.print(table_summary)

    def movement_candidates(self, criteria: SizeCriteria, limit: int):
        """
        Find shard candidates for movement based on size criteria

        Results are sorted by nodes with least available space first,
        then by shard size (smallest first) for easier moves.
        """

        console.print(
            Panel.fit(f"[bold blue]Finding Moveable Shards ({criteria.min_size}-{criteria.max_size}GB)[/bold blue]")
        )

        if criteria.source_node:
            console.print(f"[dim]Filtering: Only showing candidates from source node '{criteria.source_node}'[/dim]")

        # Find moveable candidates (only healthy shards suitable for operations)
        candidates = self.analyzer.find_moveable_shards(criteria.min_size, criteria.max_size, criteria.table_name)

        # Filter by node if specified
        if criteria.source_node:
            candidates = [c for c in candidates if c.node_name == criteria.source_node]

        if not candidates:
            if criteria.source_node:
                console.print(
                    f"[yellow]No moveable shards found on node '{criteria.source_node}' "
                    f"in the specified size range.[/yellow]"
                )
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
        node_space_map = {node.name: node.available_space_gb for node in self.analyzer.nodes}

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

    def validate_move(self, request: ShardMoveRequest):
        # Parse schema and table
        if "." not in request.schema_table:
            console.print("[red]Error: Schema and table must be in format 'schema.table'[/red]")
            return

        schema_name, table_name = request.schema_table.split(".", 1)

        console.print(Panel.fit("[bold blue]Validating Shard Move[/bold blue]"))
        console.print(
            f"[dim]Move: {schema_name}.{table_name}[{request.shard_id}] "
            f"from {request.from_node} to {request.to_node}[/dim]"
        )
        console.print()

        # Find the nodes
        from_node_info = None
        to_node_info = None
        for node in self.analyzer.nodes:
            if node.name == request.from_node:
                from_node_info = node
            if node.name == request.to_node:
                to_node_info = node

        if not from_node_info:
            console.print(f"[red]✗ Source node '{request.from_node}' not found in cluster[/red]")
            return

        if not to_node_info:
            console.print(f"[red]✗ Target node '{request.to_node}' not found in cluster[/red]")
            return

        # Find the specific shard
        target_shard = None
        for shard in self.analyzer.shards:
            if (
                shard.schema_name == schema_name
                and shard.table_name == table_name
                and shard.shard_id == request.shard_id
                and shard.node_name == request.from_node
            ):
                target_shard = shard
                break

        if not target_shard:
            console.print(f"[red]✗ Shard {request.shard_id} not found on node {request.from_node}[/red]")
            console.print("[dim]Use 'xmover find-candidates' to see available shards[/dim]")
            return

        # Create a move recommendation for validation
        recommendation = ShardMoveRecommendation(
            table_name=table_name,
            schema_name=schema_name,
            shard_id=request.shard_id,
            from_node=request.from_node,
            to_node=request.to_node,
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
        details_table.add_row("Shard ID", str(request.shard_id))
        details_table.add_row("Type", target_shard.shard_type)
        details_table.add_row("Size", format_size(target_shard.size_gb))
        details_table.add_row("Documents", f"{target_shard.num_docs:,}")
        details_table.add_row("State", target_shard.state)
        details_table.add_row("Routing State", target_shard.routing_state)
        details_table.add_row("From Node", f"{request.from_node} ({from_node_info.zone})")
        details_table.add_row("To Node", f"{request.to_node} ({to_node_info.zone})")
        details_table.add_row("Zone Change", "Yes" if from_node_info.zone != to_node_info.zone else "No")

        console.print(details_table)
        console.print()

        # Perform comprehensive validation
        is_safe, safety_msg = self.analyzer.validate_move_safety(
            recommendation, max_disk_usage_percent=request.max_disk_usage
        )

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
                "[dim]# Check with: SELECT * FROM sys.shards "
                "WHERE table_name = '{table_name}' AND id = {shard_id};[/dim]"
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
