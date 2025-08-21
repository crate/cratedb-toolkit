from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from cratedb_toolkit.admin.xmover.analysis.shard import ShardAnalyzer
from cratedb_toolkit.admin.xmover.model import SizeCriteria
from cratedb_toolkit.admin.xmover.util.format import format_size

console = Console()


class CandidateFinder:
    def __init__(self, analyzer: ShardAnalyzer):
        self.analyzer = analyzer

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
