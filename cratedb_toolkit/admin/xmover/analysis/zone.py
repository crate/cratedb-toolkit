from typing import Dict, List, Optional

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from cratedb_toolkit.admin.xmover.analysis.shard import ShardAnalyzer
from cratedb_toolkit.admin.xmover.model import ShardInfo
from cratedb_toolkit.admin.xmover.util.database import CrateDBClient

console = Console()


class ZoneReport:
    def __init__(self, client: CrateDBClient):
        self.client = client
        self.analyzer = ShardAnalyzer(self.client)

    def shard_balance(self, tolerance: float, table: Optional[str] = None):
        """Check zone balance for shards"""
        console.print(Panel.fit("[bold blue]Zone Balance Check[/bold blue]"))
        console.print("[dim]Note: Analyzing all shards regardless of state for complete cluster view[/dim]")
        console.print()

        zone_stats = self.analyzer.check_zone_balance(table, tolerance)

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

    def distribution_conflicts(self, shard_details: bool = False, table: Optional[str] = None):
        """Detailed analysis of zone distribution and potential conflicts"""
        console.print(Panel.fit("[bold blue]Detailed Zone Analysis[/bold blue]"))
        console.print("[dim]Comprehensive zone distribution analysis for CrateDB cluster[/dim]")
        console.print()

        # Get all shards for analysis
        shards = self.client.get_shards_info(table_name=table, for_analysis=True)

        if not shards:
            console.print("[yellow]No shards found for analysis[/yellow]")
            return

        # Organize by table and shard
        tables: Dict[str, Dict[int, List[ShardInfo]]] = {}
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
                if shard_details:
                    for shard_copy in shard_copies:
                        health_indicator = "✓" if shard_copy.routing_state == "STARTED" else "⚠"
                        console.print(
                            f"    {health_indicator} {shard_copy.shard_type} "
                            f"on {shard_copy.node_name} ({shard_copy.zone}) - "
                            f"{shard_copy.state}/{shard_copy.routing_state}"
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
