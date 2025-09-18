from typing import List, Optional

from rich import get_console
from rich.panel import Panel

console = get_console()


def explain_cratedb_error(error_message: Optional[str]):
    """
    Decode and troubleshoot common CrateDB shard allocation errors.

    Parameters
    ----------
    error_message:
        Raw CrateDB error message. If None and interactive=True, the user is prompted
        to paste the message (finish with two blank lines).
    interactive:
        When False, never prompt for input; return early if no message is provided.
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

    if not (error_message or "").strip():
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
        if pattern_info["pattern"].lower() in error_lower:  # type: ignore[attr-defined]
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
