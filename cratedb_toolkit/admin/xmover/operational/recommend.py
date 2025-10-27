import time

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from cratedb_toolkit.admin.xmover.analysis.shard import ShardAnalyzer
from cratedb_toolkit.admin.xmover.model import (
    ShardRelocationConstraints,
    ShardRelocationRequest,
    ShardRelocationResponse,
)
from cratedb_toolkit.admin.xmover.operational.monitor import RecoveryMonitor, RecoveryOptions
from cratedb_toolkit.admin.xmover.util.database import CrateDBClient
from cratedb_toolkit.admin.xmover.util.format import format_size

console = Console()


class ShardRelocationRecommender:
    def __init__(self, client: CrateDBClient):
        self.client = client
        self.analyzer = ShardAnalyzer(self.client)

    def validate(self, request: ShardRelocationRequest):
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
        recommendation = ShardRelocationResponse(
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

    def execute(
        self,
        constraints: ShardRelocationConstraints,
        auto_execute: bool,
        validate: bool,
        dry_run: bool,
    ):
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
        if constraints.prioritize_space:
            console.print("[dim]Mode: Prioritizing available space over zone balancing[/dim]")
        else:
            console.print("[dim]Mode: Prioritizing zone balancing over available space[/dim]")

        if constraints.source_node:
            console.print(f"[dim]Filtering: Only showing moves from source node '{constraints.source_node}'[/dim]")

        console.print(
            f"[dim]Safety thresholds: Max disk usage {constraints.max_disk_usage}%, "
            f"Min free space {constraints.min_free_space}GB[/dim]"
        )

        if dry_run:
            console.print("[green]Running in DRY RUN mode - no SQL commands will be generated[/green]")
        else:
            console.print("[red]EXECUTION MODE - SQL commands will be generated for actual moves[/red]")
        console.print()

        recommendations = self.analyzer.generate_rebalancing_recommendations(constraints=constraints)

        if not recommendations:
            if constraints.source_node:
                console.print(f"[yellow]No safe recommendations found for node '{constraints.source_node}'[/yellow]")
                console.print("[dim]This could be due to:[/dim]")
                console.print("[dim]  • Zone conflicts preventing safe moves[/dim]")
                console.print(
                    f"[dim]  • Target nodes exceeding {constraints.max_disk_usage}% disk usage threshold[/dim]"
                )
                console.print(
                    f"[dim]  • Insufficient free space on target nodes (need {constraints.min_free_space}GB)[/dim]"
                )
                console.print(f"[dim]  • No shards in size range {constraints.min_size}-{constraints.max_size}GB[/dim]")
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
        node_space_map = {node.name: node.available_space_gb for node in self.analyzer.nodes}

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
                is_safe, safety_msg = self.analyzer.validate_move_safety(
                    rec, max_disk_usage_percent=constraints.max_disk_usage
                )
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
                    is_safe, safety_msg = self.analyzer.validate_move_safety(
                        rec, max_disk_usage_percent=constraints.max_disk_usage
                    )
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
                    f"\n[green]✓ Ready to execute {safe_moves} safe moves. "
                    f"Use --execute to generate SQL commands.[/green]"
                )
            else:
                console.print(
                    "\n[yellow]⚠ No safe moves identified. Review cluster balance or adjust parameters.[/yellow]"
                )
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
                    is_safe, safety_msg = self.analyzer.validate_move_safety(
                        rec, max_disk_usage_percent=constraints.max_disk_usage
                    )
                    if not is_safe:
                        if "Zone conflict" in safety_msg:
                            zone_conflicts += 1
                            console.print(f"-- Move {i}: SKIPPED - {safety_msg}")
                            console.print(
                                "--   Tip: Try moving to a different zone or check existing shard distribution"
                            )
                        else:
                            console.print(f"-- Move {i}: SKIPPED - {safety_msg}")
                        continue
                    safe_moves += 1

                console.print(f"-- Move {i}: {rec.reason}")
                console.print(f"{rec.to_sql()}")
            console.print()

            # Auto-execution if requested
            if auto_execute:
                self._execute_recommendations_safely(recommendations, validate)

        if validate and safe_moves < len(recommendations):
            if zone_conflicts > 0:
                console.print(f"[yellow]Warning: {zone_conflicts} moves skipped due to zone conflicts[/yellow]")
                console.print(
                    "[yellow]Tip: Use 'find-candidates' to see current shard distribution across zones[/yellow]"
                )
            console.print(
                f"[yellow]Warning: Only {safe_moves} of {len(recommendations)} moves passed safety validation[/yellow]"
            )

    def _execute_recommendations_safely(self, recommendations, validate: bool):
        """Execute recommendations with extensive safety measures"""

        # Filter to only safe recommendations
        safe_recommendations = []
        if validate:
            for rec in recommendations:
                is_safe, safety_msg = self.analyzer.validate_move_safety(rec, max_disk_usage_percent=95.0)
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
            console.print(
                f"  {i}. {table_display} S{rec.shard_id} ({rec.size_gb:.1f}GB) {rec.from_node} → {rec.to_node}"
            )

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
                result = self.client.execute_query(sql_command)

                if result.get("rowcount", 0) >= 0:  # Success indicator for ALTER statements
                    console.print("    [green]✅ SUCCESS[/green] - Move initiated")
                    successful_moves += 1

                    # Smart delay: check active recoveries before next move
                    if i < len(safe_recommendations):
                        self._wait_for_recovery_capacity(max_concurrent_recoveries=5)
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

    def _wait_for_recovery_capacity(self, max_concurrent_recoveries: int = 5):
        """Wait until active recovery count is below threshold"""

        recovery_monitor = RecoveryMonitor(self.client, RecoveryOptions(include_transitioning=True))
        wait_time = 0

        while True:
            # Check active recoveries (including transitioning)
            recoveries = recovery_monitor.get_cluster_recovery_status()
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

            time.sleep(10)  # Check every 10 seconds
            wait_time += 10
