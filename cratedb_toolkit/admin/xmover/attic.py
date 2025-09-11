# ruff: noqa

# @main.command()
# @click.argument('node_name')
# @click.option('--min-free-space', default=100.0, help='Minimum free space required on target nodes in GB (default: 100)')
# @click.option('--dry-run/--execute', default=True, help='Show decommission plan without generating SQL commands (default: True)')
# @click.pass_context
# def decommission(ctx, node_name: str, min_free_space: float, dry_run: bool):
#     """Plan decommissioning of a node by analyzing required shard moves
#
#     NODE_NAME: Name of the node to decommission
#     """
#     client = ctx.obj['client']
#     analyzer = ShardAnalyzer(client)
#
#     mode_text = "PLANNING MODE" if dry_run else "EXECUTION MODE"
#     console.print(Panel.fit(f"[bold blue]Node Decommission Analysis[/bold blue] - [bold {'green' if dry_run else 'red'}]{mode_text}[/bold {'green' if dry_run else 'red'}]"))
#     console.print(f"[dim]Analyzing decommission plan for node: {node_name}[/dim]")
#     console.print()
#
#     # Generate decommission plan
#     plan = analyzer.plan_node_decommission(node_name, min_free_space)
#
#     if 'error' in plan:
#         console.print(f"[red]Error: {plan['error']}[/red]")
#         return
#
#     # Display plan summary
#     summary_table = Table(title=f"Decommission Plan for {node_name}", box=box.ROUNDED)
#     summary_table.add_column("Metric", style="cyan")
#     summary_table.add_column("Value", style="magenta")
#
#     summary_table.add_row("Node", plan['node'])
#     summary_table.add_row("Zone", plan['zone'])
#     summary_table.add_row("Feasible", "[green]✓ Yes[/green]" if plan['feasible'] else "[red]✗ No[/red]")
#     summary_table.add_row("Shards to Move", str(plan['shards_to_move']))
#     summary_table.add_row("Moveable Shards", str(plan['moveable_shards']))
#     summary_table.add_row("Total Data Size", format_size(plan['total_size_gb']))
#     summary_table.add_row("Estimated Time", f"{plan['estimated_time_hours']:.1f} hours")
#
#     console.print(summary_table)
#     console.print()
#
#     # Show warnings if any
#     if plan['warnings']:
#         console.print("[bold yellow]⚠ Warnings:[/bold yellow]")
#         for warning in plan['warnings']:
#             console.print(f"  • [yellow]{warning}[/yellow]")
#         console.print()
#
#     # Show infeasible moves if any
#     if plan['infeasible_moves']:
#         console.print("[bold red]✗ Cannot Move:[/bold red]")
#         infeasible_table = Table(box=box.ROUNDED)
#         infeasible_table.add_column("Shard", style="cyan")
#         infeasible_table.add_column("Size", style="magenta")
#         infeasible_table.add_column("Reason", style="red")
#
#         for move in plan['infeasible_moves']:
#             infeasible_table.add_row(
#                 move['shard'],
#                 format_size(move['size_gb']),
#                 move['reason']
#             )
#         console.print(infeasible_table)
#         console.print()
#
#     # Show move recommendations
#     if plan['recommendations']:
#         move_table = Table(title="Required Shard Moves", box=box.ROUNDED)
#         move_table.add_column("Table", style="cyan")
#         move_table.add_column("Shard", justify="right", style="magenta")
#         move_table.add_column("Type", style="blue")
#         move_table.add_column("Size", style="green")
#         move_table.add_column("From Zone", style="yellow")
#         move_table.add_column("To Node", style="cyan")
#         move_table.add_column("To Zone", style="yellow")
#
#         for rec in plan['recommendations']:
#             move_table.add_row(
#                 f"{rec.schema_name}.{rec.table_name}",
#                 str(rec.shard_id),
#                 rec.shard_type,
#                 format_size(rec.size_gb),
#                 rec.from_zone,
#                 rec.to_node,
#                 rec.to_zone
#             )
#
#         console.print(move_table)
#         console.print()
#
#         # Generate SQL commands if not in dry-run mode
#         if not dry_run and plan['feasible']:
#             console.print(Panel.fit("[bold green]Decommission SQL Commands[/bold green]"))
#             console.print("[dim]# Execute these commands in order to prepare for node decommission[/dim]")
#             console.print("[dim]# ALWAYS test in a non-production environment first![/dim]")
#             console.print("[dim]# Monitor shard health after each move before proceeding[/dim]")
#             console.print()
#
#             for i, rec in enumerate(plan['recommendations'], 1):
#                 console.print(f"-- Move {i}: {rec.reason}")
#                 console.print(f"{rec.to_sql()}")
#                 console.print()
#
#             console.print(f"-- After all moves complete, the node {node_name} can be safely removed")
#             console.print(f"-- Total moves required: {len(plan['recommendations'])}")
#         elif dry_run:
#             console.print("[green]✓ Decommission plan ready. Use --execute to generate SQL commands.[/green]")
#
#     # Final status
#     if not plan['feasible']:
#         console.print(f"[red]⚠ Node {node_name} cannot be safely decommissioned at this time.[/red]")
#         console.print("[dim]Address the issues above before attempting decommission.[/dim]")
#     elif plan['shards_to_move'] == 0:
#         console.print(f"[green]✓ Node {node_name} is ready for immediate decommission (no shards to move).[/green]")
#     else:
#         console.print(f"[green]✓ Node {node_name} can be safely decommissioned after moving {len(plan['recommendations'])} shards.[/green]")
