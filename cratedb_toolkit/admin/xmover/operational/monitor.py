import dataclasses
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from rich.console import Console

from cratedb_toolkit.admin.xmover.model import RecoveryInfo
from cratedb_toolkit.admin.xmover.util.database import CrateDBClient
from cratedb_toolkit.admin.xmover.util.format import format_table_display_with_partition, format_translog_info

console = Console()


@dataclasses.dataclass
class RecoveryOptions:
    table: Optional[str] = None
    node: Optional[str] = None
    refresh_interval: int = 10
    include_transitioning: bool = False
    recovery_type: Optional[str] = None


class RecoveryMonitor:
    """Monitor shard recovery operations"""

    def __init__(self, client: CrateDBClient, options: Optional[RecoveryOptions] = None):
        self.client = client
        self.options = options or RecoveryOptions()

    def get_cluster_recovery_status(self) -> List[RecoveryInfo]:
        """Get comprehensive recovery status with minimal cluster impact"""

        # Get all recovering shards using the efficient combined query
        recoveries = self.client.get_all_recovering_shards(
            self.options.table, self.options.node, self.options.include_transitioning
        )

        # Apply recovery type filter
        if self.options.recovery_type is not None and self.options.recovery_type.lower() != "all":
            recoveries = [r for r in recoveries if r.recovery_type.upper() == self.options.recovery_type.upper()]

        return recoveries

    def get_problematic_shards(self) -> List[Dict[str, Any]]:
        """Get shards that need attention but aren't actively recovering"""
        return self.client.get_problematic_shards(self.options.table, self.options.node)

    def get_recovery_summary(self, recoveries: List[RecoveryInfo]) -> Dict[str, Any]:
        """Generate a summary of recovery operations"""

        if not recoveries:
            return {"total_recoveries": 0, "by_type": {}, "by_stage": {}, "avg_progress": 0.0, "total_size_gb": 0.0}

        # Group by recovery type
        by_type = {}
        by_stage = {}
        total_progress = 0.0
        total_size_gb = 0.0

        for recovery in recoveries:
            # By type
            if recovery.recovery_type not in by_type:
                by_type[recovery.recovery_type] = {"count": 0, "total_size_gb": 0.0, "avg_progress": 0.0}
            by_type[recovery.recovery_type]["count"] += 1
            by_type[recovery.recovery_type]["total_size_gb"] += recovery.size_gb

            # By stage
            if recovery.stage not in by_stage:
                by_stage[recovery.stage] = 0
            by_stage[recovery.stage] += 1

            # Totals
            total_progress += recovery.overall_progress
            total_size_gb += recovery.size_gb

        # Calculate averages
        for type_name, rec_type in by_type.items():
            if rec_type["count"] > 0:
                type_recoveries = [r for r in recoveries if r.recovery_type == type_name]
                if type_recoveries:
                    rec_type["avg_progress"] = sum(r.overall_progress for r in type_recoveries) / len(type_recoveries)

        return {
            "total_recoveries": len(recoveries),
            "by_type": by_type,
            "by_stage": by_stage,
            "avg_progress": total_progress / len(recoveries) if recoveries else 0.0,
            "total_size_gb": total_size_gb,
        }

    def format_recovery_display(self, recoveries: List[RecoveryInfo]) -> str:
        """Format recovery information for display"""

        if not recoveries:
            return "✅ No active shard recoveries found"

        # Group by recovery type
        peer_recoveries = [r for r in recoveries if r.recovery_type == "PEER"]
        disk_recoveries = [r for r in recoveries if r.recovery_type == "DISK"]
        other_recoveries = [r for r in recoveries if r.recovery_type not in ["PEER", "DISK"]]

        output = [f"\n🔄 Active Shard Recoveries ({len(recoveries)} total)"]
        output.append("=" * 80)

        if peer_recoveries:
            output.append(f"\n📡 PEER Recoveries ({len(peer_recoveries)})")
            output.append(self._format_recovery_table(peer_recoveries))

        if disk_recoveries:
            output.append(f"\n💾 DISK Recoveries ({len(disk_recoveries)})")
            output.append(self._format_recovery_table(disk_recoveries))

        if other_recoveries:
            output.append(f"\n🔧 Other Recoveries ({len(other_recoveries)})")
            output.append(self._format_recovery_table(other_recoveries))

        # Add summary
        summary = self.get_recovery_summary(recoveries)
        output.append("\n📊 Summary:")
        output.append(f"   Total size: {summary['total_size_gb']:.1f} GB")
        output.append(f"   Average progress: {summary['avg_progress']:.1f}%")

        return "\n".join(output)

    def _format_recovery_table(self, recoveries: List[RecoveryInfo]) -> str:
        """Format a table of recovery information"""

        if not recoveries:
            return "   No recoveries of this type"

        # Table headers
        headers = ["Table", "Shard", "Node", "Recovery", "Stage", "Progress", "Size(GB)", "Time(s)"]

        # Calculate column widths
        col_widths = [len(h) for h in headers]

        rows = []
        for recovery in recoveries:
            # Format table name with partition values if available
            table_display = f"{recovery.schema_name}.{recovery.table_name}"
            if recovery.partition_values:
                table_display = f"{table_display} {recovery.partition_values}"
            row = [
                table_display,
                str(recovery.shard_id),
                recovery.node_name,
                recovery.recovery_type,
                recovery.stage,
                f"{recovery.overall_progress:.1f}%",
                f"{recovery.size_gb:.1f}",
                f"{recovery.total_time_seconds:.1f}",
            ]
            rows.append(row)

            # Update column widths
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(cell))

        # Format table
        output = []

        # Header row
        header_row = "   " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths))
        output.append(header_row)
        output.append("   " + "-" * (len(header_row) - 3))

        # Data rows
        for row in rows:
            data_row = "   " + " | ".join(cell.ljust(w) for cell, w in zip(row, col_widths))
            output.append(data_row)

        return "\n".join(output)

    def start(self, watch: bool, debug: bool = False):
        try:
            if watch:
                console.print(f"🔄 Monitoring shard recoveries (refreshing every {self.options.refresh_interval}s)")
                console.print("Press Ctrl+C to stop")
                console.print()

                try:
                    # Show header once
                    console.print("📊 Recovery Progress Monitor")
                    console.print("=" * 80)

                    # Track previous state for change detection
                    previous_recoveries: Dict[str, Dict[str, Any]] = {}
                    first_run = True

                    while True:
                        # Get current recovery status
                        recoveries = self.get_cluster_recovery_status()

                        # Display current time
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
                            table_display = format_table_display_with_partition(
                                recovery.schema_name, recovery.table_name, recovery.partition_values
                            )

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
                                        table_display = format_table_display_with_partition(
                                            recovery.schema_name, recovery.table_name, recovery.partition_values
                                        )
                                        changes.append(
                                            f"[green]📈[/green] {table_display} S{recovery.shard_id} "
                                            f"{recovery.recovery_type} {recovery.overall_progress:.1f}% "
                                            f"(+{diff:.1f}%) {recovery.size_gb:.1f}GB{translog_info}{node_route}"
                                        )
                                    else:
                                        table_display = format_table_display_with_partition(
                                            recovery.schema_name, recovery.table_name, recovery.partition_values
                                        )
                                        changes.append(
                                            f"[yellow]📉[/yellow] {table_display} S{recovery.shard_id} "
                                            f"{recovery.recovery_type} {recovery.overall_progress:.1f}% "
                                            f"({diff:.1f}%) {recovery.size_gb:.1f}GB{translog_info}{node_route}"
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
                                    table_display = format_table_display_with_partition(
                                        recovery.schema_name, recovery.table_name, recovery.partition_values
                                    )
                                    changes.append(
                                        f"[blue]🔄[/blue] {table_display} S{recovery.shard_id} "
                                        f"{recovery.recovery_type} {prev['stage']}→{recovery.stage} "
                                        f"{recovery.size_gb:.1f}GB{translog_info}{node_route}"
                                    )
                            else:
                                # New recovery - show based on include_transitioning flag or first run
                                if (
                                    first_run
                                    or self.options.include_transitioning
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
                                    table_display = format_table_display_with_partition(
                                        recovery.schema_name, recovery.table_name, recovery.partition_values
                                    )
                                    changes.append(
                                        f"{status_icon} {table_display} S{recovery.shard_id} "
                                        f"{recovery.recovery_type} {recovery.stage} {recovery.overall_progress:.1f}% "
                                        f"{recovery.size_gb:.1f}GB{translog_info}{node_route}"
                                    )

                            # Store current state for next comparison
                            previous_recoveries[recovery_key] = {
                                "progress": recovery.overall_progress,
                                "stage": recovery.stage,
                            }

                        # Get problematic shards for comprehensive status
                        problematic_shards = self.get_problematic_shards()

                        # Filter out shards that are already being recovered
                        non_recovering_shards = []
                        if problematic_shards:
                            for shard in problematic_shards:
                                # Check if this shard is already in our recoveries list
                                is_recovering = any(
                                    r.shard_id == shard["shard_id"]
                                    and r.table_name == shard["table_name"]
                                    and r.schema_name == shard["schema_name"]
                                    for r in recoveries
                                )
                                if not is_recovering:
                                    non_recovering_shards.append(shard)

                        # Always show a comprehensive status line
                        if not recoveries and not non_recovering_shards:
                            console.print(f"{current_time} | [green]No issues - cluster stable[/green]")
                            previous_recoveries.clear()
                        elif not recoveries and non_recovering_shards:
                            console.print(
                                f"{current_time} | [yellow]{len(non_recovering_shards)} shards "
                                f"need attention (not recovering)[/yellow]"
                            )
                            # Show first few problematic shards
                            for shard in non_recovering_shards[:5]:
                                table_display = format_table_display_with_partition(
                                    shard["schema_name"], shard["table_name"], shard.get("partition_values")
                                )
                                primary_indicator = "P" if shard.get("primary") else "R"
                                console.print(
                                    f"         | [red]⚠[/red] {table_display} "
                                    f"S{shard['shard_id']}{primary_indicator} {shard['state']}"
                                )
                            if len(non_recovering_shards) > 5:
                                console.print(f"         | [dim]... and {len(non_recovering_shards) - 5} more[/dim]")
                            previous_recoveries.clear()
                        else:
                            # Build status message for active recoveries
                            status_parts = []
                            if active_count > 0:
                                status_parts.append(f"{active_count} recovering")
                            if completed_count > 0:
                                status_parts.append(f"{completed_count} done")
                            if non_recovering_shards:
                                status_parts.append(f"[yellow]{len(non_recovering_shards)} awaiting recovery[/yellow]")

                            status = " | ".join(status_parts)

                            # Show status line with changes or periodic update
                            if changes:
                                console.print(f"{current_time} | {status}")
                                for change in changes:
                                    console.print(f"         | {change}")
                            # Show some problematic shards if there are any
                            if non_recovering_shards and len(changes) < 3:  # Don't overwhelm the output
                                for shard in non_recovering_shards[:2]:
                                    table_display = format_table_display_with_partition(
                                        shard["schema_name"], shard["table_name"], shard.get("partition_values")
                                    )
                                    primary_indicator = "P" if shard.get("primary") else "R"
                                    console.print(
                                        f"         | [red]⚠[/red] {table_display} "
                                        f"S{shard['shard_id']}{primary_indicator} {shard['state']}"
                                    )
                            else:
                                # Show periodic status even without changes
                                if self.options.include_transitioning and completed_count > 0:
                                    console.print(f"{current_time} | {status} (transitioning)")
                                elif active_count > 0:
                                    console.print(f"{current_time} | {status} (no changes)")
                                elif non_recovering_shards:
                                    console.print(f"{current_time} | {status} (issues persist)")

                        first_run = False
                        time.sleep(self.options.refresh_interval)

                except KeyboardInterrupt:
                    console.print("\n\n[yellow]⏹  Monitoring stopped by user[/yellow]")

                    # Show final summary
                    final_recoveries = self.get_cluster_recovery_status()

                    final_problematic_shards = self.get_problematic_shards()

                    # Filter out shards that are already being recovered
                    final_non_recovering_shards = []
                    if final_problematic_shards:
                        for shard in final_problematic_shards:
                            is_recovering = any(
                                r.shard_id == shard["shard_id"]
                                and r.table_name == shard["table_name"]
                                and r.schema_name == shard["schema_name"]
                                for r in final_recoveries
                            )
                            if not is_recovering:
                                final_non_recovering_shards.append(shard)

                    if final_recoveries or final_non_recovering_shards:
                        console.print("\n📊 [bold]Final Cluster Status Summary:[/bold]")

                        if final_recoveries:
                            summary = self.get_recovery_summary(final_recoveries)
                            # Count active vs completed
                            active_count = len(
                                [r for r in final_recoveries if r.overall_progress < 100.0 or r.stage != "DONE"]
                            )
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

                        if final_non_recovering_shards:
                            console.print(
                                f"   [yellow]Problematic shards needing attention: "
                                f"{len(final_non_recovering_shards)}[/yellow]"
                            )
                            # Group by state for summary
                            by_state = {}
                            for shard in final_non_recovering_shards:
                                state = shard["state"]
                                if state not in by_state:
                                    by_state[state] = 0
                                by_state[state] += 1

                            for state, count in by_state.items():
                                console.print(f"     {state}: {count} shards")
                    else:
                        console.print("\n[green]✅ No active recoveries at exit[/green]")
                        console.print("\n[green]✅ Cluster stable - no issues detected[/green]")

                    return

            else:
                # Single status check
                recoveries = self.get_cluster_recovery_status()

                display_output = self.format_recovery_display(recoveries)
                console.print(display_output)

                # Get problematic shards for comprehensive status
                problematic_shards = self.get_problematic_shards()

                # Filter out shards that are already being recovered
                non_recovering_shards = []
                if problematic_shards:
                    for shard in problematic_shards:
                        is_recovering = any(
                            r.shard_id == shard["shard_id"]
                            and r.table_name == shard["table_name"]
                            and r.schema_name == shard["schema_name"]
                            for r in recoveries
                        )
                        if not is_recovering:
                            non_recovering_shards.append(shard)

                if not recoveries and not non_recovering_shards:
                    if self.options.include_transitioning:
                        console.print("\n[green]✅ No issues found - cluster stable[/green]")
                    else:
                        console.print("\n[green]✅ No active recoveries found[/green]")
                        console.print(
                            "[dim]💡 Use --include-transitioning to see completed recoveries still transitioning[/dim]"
                        )

                elif not recoveries and non_recovering_shards:
                    console.print(
                        f"\n[yellow]⚠️ {len(non_recovering_shards)} shards need attention (not recovering)[/yellow]"
                    )
                    # Group by state for summary
                    by_state = {}
                    for shard in non_recovering_shards:
                        state = shard["state"]
                        if state not in by_state:
                            by_state[state] = 0
                        by_state[state] += 1

                    for state, count in by_state.items():
                        console.print(f"   {state}: {count} shards")

                    # Show first few examples
                    console.print("\nExamples:")
                    for shard in non_recovering_shards[:5]:
                        table_display = format_table_display_with_partition(
                            shard["schema_name"], shard["table_name"], shard.get("partition_values")
                        )
                        primary_indicator = "P" if shard.get("primary") else "R"
                        console.print(
                            f"   [red]⚠[/red] {table_display} S{shard['shard_id']}{primary_indicator} {shard['state']}"
                        )

                    if len(non_recovering_shards) > 5:
                        console.print(f"   [dim]... and {len(non_recovering_shards) - 5} more[/dim]")

                else:
                    # Show recovery summary
                    summary = self.get_recovery_summary(recoveries)
                    console.print("\n📊 [bold]Cluster Status Summary:[/bold]")
                    console.print(f"   Active recoveries: {summary['total_recoveries']}")
                    console.print(f"   Total recovery size: {summary['total_size_gb']:.1f} GB")
                    console.print(f"   Average progress: {summary['avg_progress']:.1f}%")

                    # Show breakdown by type
                    if summary["by_type"]:
                        console.print("\n   By recovery type:")
                        for rec_type, stats in summary["by_type"].items():
                            console.print(
                                f"     {rec_type}: {stats['count']} recoveries, "
                                f"{stats['avg_progress']:.1f}% avg progress"
                            )

                    # Show problematic shards if any
                    if non_recovering_shards:
                        console.print(
                            f"\n   [yellow]Problematic shards needing attention: {len(non_recovering_shards)}[/yellow]"
                        )
                        by_state = {}
                        for shard in non_recovering_shards:
                            state = shard["state"]
                            if state not in by_state:
                                by_state[state] = 0
                            by_state[state] += 1

                        for state, count in by_state.items():
                            console.print(f"     {state}: {count} shards")

                    console.print("\n[dim]💡 Use --watch flag for continuous monitoring[/dim]")

        except Exception as e:
            console.print(f"[red]❌ Error monitoring recoveries: {e}[/red]")
            if debug:
                raise
