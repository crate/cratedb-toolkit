import dataclasses
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from rich.console import Console

from cratedb_toolkit.admin.xmover.model import RecoveryInfo
from cratedb_toolkit.admin.xmover.util.database import CrateDBClient
from cratedb_toolkit.admin.xmover.util.format import format_translog_info

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
        if self.options.recovery_type is not None:
            recoveries = [r for r in recoveries if r.recovery_type.upper() == self.options.recovery_type.upper()]

        return recoveries

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
        headers = ["Table", "Shard", "Node", "Type", "Stage", "Progress", "Size(GB)", "Time(s)"]

        # Calculate column widths
        col_widths = [len(h) for h in headers]

        rows = []
        for recovery in recoveries:
            row = [
                f"{recovery.schema_name}.{recovery.table_name}",
                str(recovery.shard_id),
                recovery.node_name,
                recovery.shard_type,
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
                    previous_timestamp = None
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
                                if self.options.include_transitioning and completed_count > 0:
                                    console.print(f"{current_time} | {status} (transitioning)")
                                elif active_count > 0:
                                    console.print(f"{current_time} | {status} (no changes)")

                        previous_timestamp = current_time  # noqa: F841
                        first_run = False
                        time.sleep(self.options.refresh_interval)

                except KeyboardInterrupt:
                    console.print("\n\n[yellow]⏹  Monitoring stopped by user[/yellow]")

                    # Show final summary
                    final_recoveries = self.get_cluster_recovery_status()

                    if final_recoveries:
                        console.print("\n📊 [bold]Final Recovery Summary:[/bold]")
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
                    else:
                        console.print("\n[green]✅ No active recoveries at exit[/green]")

                    return

            else:
                # Single status check
                recoveries = self.get_cluster_recovery_status()

                display_output = self.format_recovery_display(recoveries)
                console.print(display_output)

                if not recoveries:
                    if self.options.include_transitioning:
                        console.print("\n[green]✅ No recoveries found (active or transitioning)[/green]")
                    else:
                        console.print("\n[green]✅ No active recoveries found[/green]")
                        console.print(
                            "[dim]💡 Use --include-transitioning to see completed recoveries still transitioning[/dim]"
                        )
                else:
                    # Show summary
                    summary = self.get_recovery_summary(recoveries)
                    console.print("\n📊 [bold]Recovery Summary:[/bold]")
                    console.print(f"   Total recoveries: {summary['total_recoveries']}")
                    console.print(f"   Total size: {summary['total_size_gb']:.1f} GB")
                    console.print(f"   Average progress: {summary['avg_progress']:.1f}%")

                    # Show breakdown by type
                    if summary["by_type"]:
                        console.print("\n   By recovery type:")
                        for rec_type, stats in summary["by_type"].items():
                            console.print(
                                f"     {rec_type}: {stats['count']} recoveries, "
                                f"{stats['avg_progress']:.1f}% avg progress"
                            )

                    console.print("\n[dim]💡 Use --watch flag for continuous monitoring[/dim]")

        except Exception as e:
            console.print(f"[red]❌ Error monitoring recoveries: {e}[/red]")
            if debug:
                raise
