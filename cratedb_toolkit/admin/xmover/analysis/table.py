"""
Shard Distribution Analysis for CrateDB Clusters

This module analyzes shard distribution across nodes to detect imbalances
and provide recommendations for optimization.
"""

import logging
import statistics
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from rich import print as rprint
from rich.console import Console
from rich.table import Table

from cratedb_toolkit.admin.xmover.model import NodeInfo
from cratedb_toolkit.admin.xmover.util.database import CrateDBClient

logger = logging.getLogger(__name__)


def format_storage_size(size_gb: float) -> str:
    """Format storage size with appropriate units and spacing"""
    if size_gb < 0.001:
        return "0 B"
    elif size_gb < 1.0:
        size_mb = size_gb * 1024
        return f"{size_mb:.0f} MB"
    elif size_gb < 1024:
        return f"{size_gb:.1f} GB"
    else:
        size_tb = size_gb / 1024
        return f"{size_tb:.2f} TB"


@dataclass
class TableDistribution:
    """Represents shard distribution for a single table"""

    schema_name: str
    table_name: str
    total_primary_size_gb: float
    node_distributions: Dict[str, Dict[str, Any]]  # node_name -> metrics

    @property
    def full_table_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}" if self.schema_name != "doc" else self.table_name


@dataclass
class DistributionAnomaly:
    """Represents a detected distribution anomaly"""

    table: TableDistribution
    anomaly_type: str
    severity_score: float
    impact_score: float
    combined_score: float
    description: str
    details: Dict[str, Any]
    recommendations: List[str]


class DistributionAnalyzer:
    """Analyzes shard distribution across cluster nodes"""

    def __init__(self, client: CrateDBClient):
        self.client = client
        self.console = Console()

    def find_table_by_name(self, table_name: str) -> Optional[str]:
        """Find table by name and resolve schema ambiguity"""

        query = """
                SELECT DISTINCT schema_name, table_name
                FROM sys.shards
                WHERE table_name = ?
                  AND schema_name NOT IN ('sys', 'information_schema', 'pg_catalog')
                  AND routing_state = 'STARTED'
                ORDER BY schema_name \
                """

        result = self.client.execute_query(query, [table_name])
        rows = result.get("rows", [])

        if not rows:
            return None
        elif len(rows) == 1:
            schema, table = rows[0]
            return f"{schema}.{table}" if schema != "doc" else table
        else:
            # Multiple schemas have this table - ask user
            rprint(f"[yellow]Multiple schemas contain table '{table_name}':[/yellow]")
            for i, (schema, table) in enumerate(rows, 1):
                full_name = f"{schema}.{table}" if schema != "doc" else table
                rprint(f"  {i}. {full_name}")

            try:
                choice = input("\nSelect table (enter number): ").strip()
                if not choice:
                    rprint("[yellow]No selection made[/yellow]")
                    return None
                idx = int(choice) - 1
                if 0 <= idx < len(rows):
                    schema, table = rows[idx]
                    return f"{schema}.{table}" if schema != "doc" else table
                else:
                    rprint("[red]Invalid selection[/red]")
                    return None
            except (ValueError, KeyboardInterrupt):
                rprint("\n[yellow]Selection cancelled[/yellow]")
                return None

    def get_table_distribution_detailed(self, table_identifier: str) -> Optional[TableDistribution]:
        """Get detailed distribution data for a specific table"""

        # Parse schema and table name
        if "." in table_identifier:
            schema_name, table_name = table_identifier.split(".", 1)
        else:
            schema_name = "doc"
            table_name = table_identifier

        query = """
                SELECT s.schema_name, \
                       s.table_name, \
                       s.node['name']                                                                                 as node_name, \
                       COUNT(CASE WHEN s."primary" = true THEN 1 END)                                                 as primary_shards, \
                       COUNT(CASE WHEN s."primary" = false THEN 1 END)                                                as replica_shards, \
                       COUNT(*)                                                                                       as total_shards, \
                       ROUND(SUM(s.size) / 1024.0 / 1024.0 / 1024.0, 2)                                               as total_size_gb, \
                       ROUND(SUM(CASE WHEN s."primary" = true THEN s.size ELSE 0 END) / 1024.0 / 1024.0 / 1024.0, \
                             2)                                                                                       as primary_size_gb, \
                       ROUND(SUM(CASE WHEN s."primary" = false THEN s.size ELSE 0 END) / 1024.0 / 1024.0 / 1024.0, \
                             2)                                                                                       as replica_size_gb, \
                       SUM(s.num_docs)                                                                                as total_documents
                FROM sys.shards s
                WHERE s.schema_name = ? \
                  AND s.table_name = ?
                  AND s.routing_state = 'STARTED'
                GROUP BY s.schema_name, s.table_name, s.node['name']
                ORDER BY s.node['name'] \
                """  # noqa: E501

        result = self.client.execute_query(query, [schema_name, table_name])
        rows = result.get("rows", [])

        if not rows:
            return None

        # Build node distributions
        node_distributions = {}
        for row in rows:
            node_distributions[row[2]] = {
                "primary_shards": row[3],
                "replica_shards": row[4],
                "total_shards": row[5],
                "total_size_gb": row[6],
                "primary_size_gb": row[7],
                "replica_size_gb": row[8],
                "total_documents": row[9],
            }

        # Calculate total primary size
        total_primary_size = sum(node["primary_size_gb"] for node in node_distributions.values())

        return TableDistribution(
            schema_name=rows[0][0],
            table_name=rows[0][1],
            total_primary_size_gb=total_primary_size,
            node_distributions=node_distributions,
        )

    def format_table_health_report(self, table_dist: TableDistribution) -> None:
        """Format and display comprehensive table health report"""

        rprint(f"\n[bold blue]📋 Table Health Report: {table_dist.full_table_name}[/bold blue]")
        rprint("=" * 80)

        # Calculate overview stats
        all_nodes_info = self.client.get_nodes_info()
        cluster_nodes = {node.name for node in all_nodes_info if node.name}
        table_nodes = set(table_dist.node_distributions.keys())
        missing_nodes = cluster_nodes - table_nodes

        total_shards = sum(node["total_shards"] for node in table_dist.node_distributions.values())
        total_primary_shards = sum(node["primary_shards"] for node in table_dist.node_distributions.values())
        total_replica_shards = sum(node["replica_shards"] for node in table_dist.node_distributions.values())
        total_size_gb = sum(node["total_size_gb"] for node in table_dist.node_distributions.values())
        total_documents = sum(node["total_documents"] for node in table_dist.node_distributions.values())

        # Table Overview
        rprint("\n[bold]🎯 Overview[/bold]")
        rprint(f"• Primary Data Size: {format_storage_size(table_dist.total_primary_size_gb)}")
        rprint(f"• Total Size (with replicas): {format_storage_size(total_size_gb)}")
        rprint(f"• Total Shards: {total_shards} ({total_primary_shards} primary + {total_replica_shards} replica)")
        rprint(f"• Total Documents: {total_documents:,}")
        rprint(
            f"• Node Coverage: {len(table_nodes)}/{len(cluster_nodes)} nodes "
            f"({len(table_nodes) / len(cluster_nodes) * 100:.0f}%)"
        )

        if missing_nodes:
            rprint(f"• [yellow]Missing from nodes: {', '.join(sorted(missing_nodes))}[/yellow]")

        # Shard Distribution Table
        rprint("\n[bold]📊 Shard Distribution by Node[/bold]")

        shard_table = Table(show_header=True)
        shard_table.add_column("Node", width=15)
        shard_table.add_column("Primary", width=8, justify="right")
        shard_table.add_column("Replica", width=8, justify="right")
        shard_table.add_column("Total", width=8, justify="right")
        shard_table.add_column("Primary Size", width=12, justify="right")
        shard_table.add_column("Replica Size", width=12, justify="right")
        shard_table.add_column("Total Size", width=12, justify="right")
        shard_table.add_column("Documents", width=12, justify="right")

        for node_name in sorted(table_dist.node_distributions.keys()):
            node_data = table_dist.node_distributions[node_name]

            # Color coding based on shard count compared to average
            avg_total_shards = total_shards / len(table_dist.node_distributions)
            if node_data["total_shards"] > avg_total_shards * 1.5:
                node_color = "red"
            elif node_data["total_shards"] < avg_total_shards * 0.5:
                node_color = "yellow"
            else:
                node_color = "white"

            shard_table.add_row(
                f"[{node_color}]{node_name}[/{node_color}]",
                str(node_data["primary_shards"]),
                str(node_data["replica_shards"]),
                f"[{node_color}]{node_data['total_shards']}[/{node_color}]",
                format_storage_size(node_data["primary_size_gb"]),
                format_storage_size(node_data["replica_size_gb"]),
                f"[{node_color}]{format_storage_size(node_data['total_size_gb'])}[/{node_color}]",
                f"{node_data['total_documents']:,}",
            )

        self.console.print(shard_table)

        # Distribution Analysis
        rprint("\n[bold]🔍 Distribution Analysis[/bold]")

        # Calculate statistics
        shard_counts = [node["total_shards"] for node in table_dist.node_distributions.values()]
        storage_sizes = [node["total_size_gb"] for node in table_dist.node_distributions.values()]
        doc_counts = [node["total_documents"] for node in table_dist.node_distributions.values()]

        shard_cv = self.calculate_coefficient_of_variation(shard_counts)
        storage_cv = self.calculate_coefficient_of_variation(storage_sizes)
        doc_cv = self.calculate_coefficient_of_variation(doc_counts)

        min_shards, max_shards = min(shard_counts), max(shard_counts)
        min_storage, max_storage = min(storage_sizes), max(storage_sizes)
        min_docs, max_docs = min(doc_counts), max(doc_counts)

        # Shard distribution analysis
        if shard_cv > 0.3:
            rprint(
                f"• [red]⚠  Shard Imbalance:[/red] Range {min_shards}-{max_shards} shards per node (CV: {shard_cv:.2f})"
            )
        else:
            rprint(f"• [green]✓ Shard Balance:[/green] Well distributed (CV: {shard_cv:.2f})")

        # Storage distribution analysis
        if storage_cv > 0.4:
            rprint(
                f"• [red]⚠  Storage Imbalance:[/red] Range "
                f"{format_storage_size(min_storage)}-{format_storage_size(max_storage)} per node (CV: {storage_cv:.2f})"
            )
        else:
            rprint(f"• [green]✓ Storage Balance:[/green] Well distributed (CV: {storage_cv:.2f})")

        # Document distribution analysis
        if doc_cv > 0.5:
            rprint(f"• [red]⚠  Document Skew:[/red] Range {min_docs:,}-{max_docs:,} docs per node (CV: {doc_cv:.2f})")
        else:
            rprint(f"• [green]✓ Document Distribution:[/green] Well balanced (CV: {doc_cv:.2f})")

        # Node coverage analysis
        coverage_ratio = len(table_nodes) / len(cluster_nodes)
        if coverage_ratio < 0.7:
            missing_list = ", ".join(sorted(missing_nodes)[:5])  # Show up to 5 nodes
            if len(missing_nodes) > 5:
                missing_list += f", +{len(missing_nodes) - 5} more"
            rprint(f"• [red]⚠  Limited Coverage:[/red] {coverage_ratio:.0%} cluster coverage, missing: {missing_list}")
        else:
            rprint(f"• [green]✓ Good Coverage:[/green] {coverage_ratio:.0%} of cluster nodes have this table")

        # Zone analysis if available
        try:
            zone_distribution = {}
            for node_name, node_data in table_dist.node_distributions.items():
                # Try to get zone info for each node
                node_info: Optional[NodeInfo] = next((n for n in all_nodes_info if n.name == node_name), None)
                if node_info and node_info.zone:
                    zone = node_info.zone
                    if zone not in zone_distribution:
                        zone_distribution[zone] = {"nodes": 0, "shards": 0, "size": 0}
                    zone_distribution[zone]["nodes"] += 1
                    zone_distribution[zone]["shards"] += node_data["total_shards"]
                    zone_distribution[zone]["size"] += node_data["total_size_gb"]

            if zone_distribution:
                rprint("\n[bold]🌍 Zone Distribution[/bold]")
                for zone in sorted(zone_distribution.keys()):
                    zone_data = zone_distribution[zone]
                    rprint(
                        f"• {zone}: {zone_data['nodes']} nodes, "
                        f"{zone_data['shards']} shards, {format_storage_size(zone_data['size'])}"
                    )

        except Exception:
            # Zone info not available
            logger.exception("Zone info not available")

        # Health Summary
        rprint("\n[bold]💊 Health Summary[/bold]")
        issues = []
        recommendations = []

        if shard_cv > 0.3:
            issues.append("Shard imbalance")
            recommendations.append("Consider moving shards between nodes for better distribution")

        if storage_cv > 0.4:
            issues.append("Storage imbalance")
            recommendations.append("Rebalance shards to distribute storage more evenly")

        if doc_cv > 0.5:
            issues.append("Document skew")
            recommendations.append("Review routing configuration - data may not be evenly distributed")

        if coverage_ratio < 0.7:
            issues.append("Limited node coverage")
            recommendations.append("Consider adding replicas to improve availability and distribution")

        if not issues:
            rprint("• [green]✅ Table appears healthy with good distribution[/green]")
        else:
            rprint(f"• [yellow]⚠  Issues found: {', '.join(issues)}[/yellow]")
            rprint("\n[bold]💡 Recommendations:[/bold]")
            for rec in recommendations:
                rprint(f"  • {rec}")

        rprint()

    def get_largest_tables_distribution(self, top_n: int = 10) -> List[TableDistribution]:
        """Get distribution data for the largest tables using BIGDUDES query"""

        query = """
                WITH largest_tables AS (SELECT schema_name, \
                                               table_name, \
                                               SUM(CASE WHEN "primary" = true THEN size ELSE 0 END) as total_primary_size \
                                        FROM sys.shards \
                                        WHERE schema_name NOT IN ('sys', 'information_schema', 'pg_catalog') \
                                          AND routing_state = 'STARTED' \
                                        GROUP BY schema_name, table_name \
                                        ORDER BY total_primary_size DESC
                    LIMIT ?
                    )
                SELECT s.schema_name, \
                       s.table_name, \
                       s.node['name']                                                                                 as node_name, \
                       COUNT(CASE WHEN s."primary" = true THEN 1 END)                                                 as primary_shards, \
                       COUNT(CASE WHEN s."primary" = false THEN 1 END)                                                as replica_shards, \
                       COUNT(*)                                                                                       as total_shards, \
                       ROUND(SUM(s.size) / 1024.0 / 1024.0 / 1024.0, 2)                                               as total_size_gb, \
                       ROUND(SUM(CASE WHEN s."primary" = true THEN s.size ELSE 0 END) / 1024.0 / 1024.0 / 1024.0, \
                             2)                                                                                       as primary_size_gb, \
                       ROUND(SUM(CASE WHEN s."primary" = false THEN s.size ELSE 0 END) / 1024.0 / 1024.0 / 1024.0, \
                             2)                                                                                       as replica_size_gb, \
                       SUM(s.num_docs)                                                                                as total_documents
                FROM sys.shards s
                         INNER JOIN largest_tables lt \
                                    ON (s.schema_name = lt.schema_name AND s.table_name = lt.table_name)
                WHERE s.routing_state = 'STARTED'
                GROUP BY s.schema_name, s.table_name, s.node['name']
                ORDER BY s.schema_name, s.table_name, s.node['name'] \
                """  # noqa: E501

        result = self.client.execute_query(query, [top_n])

        # Extract rows from the result dictionary
        rows = result.get("rows", [])

        if not rows:
            return []

        # Group results by table
        tables_data = {}
        for row in rows:
            # Ensure we have enough columns
            if len(row) < 10:
                continue

            table_key = f"{row[0]}.{row[1]}"
            if table_key not in tables_data:
                tables_data[table_key] = {"schema_name": row[0], "table_name": row[1], "nodes": {}}

            tables_data[table_key]["nodes"][row[2]] = {
                "primary_shards": row[3],
                "replica_shards": row[4],
                "total_shards": row[5],
                "total_size_gb": row[6],
                "primary_size_gb": row[7],
                "replica_size_gb": row[8],
                "total_documents": row[9],
            }

        # Calculate total primary sizes and create TableDistribution objects
        distributions = []
        for table_data in tables_data.values():
            total_primary_size = sum(node["primary_size_gb"] for node in table_data["nodes"].values())

            distribution = TableDistribution(
                schema_name=table_data["schema_name"],
                table_name=table_data["table_name"],
                total_primary_size_gb=total_primary_size,
                node_distributions=table_data["nodes"],
            )
            distributions.append(distribution)

        # Sort by primary size (descending)
        return sorted(distributions, key=lambda x: x.total_primary_size_gb, reverse=True)

    def calculate_coefficient_of_variation(self, values: List[float]) -> float:
        """Calculate coefficient of variation (std dev / mean)"""
        if not values or len(values) < 2:
            return 0.0

        mean_val = statistics.mean(values)
        if mean_val == 0:
            return 0.0

        try:
            std_dev = statistics.stdev(values)
            return std_dev / mean_val
        except statistics.StatisticsError:
            return 0.0

    def detect_shard_count_imbalance(self, table: TableDistribution) -> Optional[DistributionAnomaly]:
        """Detect imbalances in shard count distribution"""
        if not table.node_distributions:
            return None

        # Get shard counts per node
        total_shards = [node["total_shards"] for node in table.node_distributions.values()]
        primary_shards = [node["primary_shards"] for node in table.node_distributions.values()]
        replica_shards = [node["replica_shards"] for node in table.node_distributions.values()]

        # Calculate coefficient of variation
        total_cv = self.calculate_coefficient_of_variation(total_shards)
        primary_cv = self.calculate_coefficient_of_variation(primary_shards)
        replica_cv = self.calculate_coefficient_of_variation(replica_shards)

        # Severity based on highest CV (higher CV = more imbalanced)
        max_cv = max(total_cv, primary_cv, replica_cv)

        # Consider it an anomaly if CV > 0.3 (30% variation)
        if max_cv < 0.3:
            return None

        # Impact based on table size
        impact_score = min(table.total_primary_size_gb / 100.0, 10.0)  # Cap at 10
        severity_score = min(max_cv * 10, 10.0)  # Scale to 0-10
        combined_score = impact_score * severity_score

        # Generate recommendations
        recommendations = []
        min_shards = min(total_shards)
        max_shards = max(total_shards)

        if max_shards - min_shards > 1:
            overloaded_nodes = [
                node for node, data in table.node_distributions.items() if data["total_shards"] == max_shards
            ]
            underloaded_nodes = [
                node for node, data in table.node_distributions.items() if data["total_shards"] == min_shards
            ]

            if overloaded_nodes and underloaded_nodes:
                recommendations.append(f"Move shards from {overloaded_nodes[0]} to {underloaded_nodes[0]}")

        return DistributionAnomaly(
            table=table,
            anomaly_type="Shard Count Imbalance",
            severity_score=severity_score,
            impact_score=impact_score,
            combined_score=combined_score,
            description=f"Uneven shard distribution (CV: {max_cv:.2f})",
            details={
                "total_cv": total_cv,
                "primary_cv": primary_cv,
                "replica_cv": replica_cv,
                "shard_counts": {node: data["total_shards"] for node, data in table.node_distributions.items()},
            },
            recommendations=recommendations,
        )

    def detect_storage_imbalance(self, table: TableDistribution) -> Optional[DistributionAnomaly]:
        """Detect imbalances in storage distribution"""
        if not table.node_distributions:
            return None

        storage_sizes = [node["total_size_gb"] for node in table.node_distributions.values()]

        # Skip if all sizes are very small (< 1GB total)
        if sum(storage_sizes) < 1.0:
            return None

        cv = self.calculate_coefficient_of_variation(storage_sizes)

        # Consider it an anomaly if CV > 0.4 (40% variation) for storage
        if cv < 0.4:
            return None

        impact_score = min(table.total_primary_size_gb / 50.0, 10.0)
        severity_score = min(cv * 8, 10.0)
        combined_score = impact_score * severity_score

        # Generate recommendations
        recommendations = []
        min_size = min(storage_sizes)
        max_size = max(storage_sizes)

        if max_size > min_size * 2:  # If difference is > 2x
            overloaded_node = None
            underloaded_node = None

            for node, data in table.node_distributions.items():
                if data["total_size_gb"] == max_size:
                    overloaded_node = node
                elif data["total_size_gb"] == min_size:
                    underloaded_node = node

            if overloaded_node and underloaded_node:
                recommendations.append(
                    f"Rebalance storage from {overloaded_node} ({format_storage_size(max_size)}) "
                    f"to {underloaded_node} ({format_storage_size(min_size)})"
                )

        return DistributionAnomaly(
            table=table,
            anomaly_type="Storage Imbalance",
            severity_score=severity_score,
            impact_score=impact_score,
            combined_score=combined_score,
            description=f"Uneven storage distribution (CV: {cv:.2f})",
            details={
                "storage_cv": cv,
                "storage_sizes": {node: data["total_size_gb"] for node, data in table.node_distributions.items()},
            },
            recommendations=recommendations,
        )

    def detect_node_coverage_issues(self, table: TableDistribution) -> Optional[DistributionAnomaly]:
        """Detect nodes with missing shard coverage"""
        if not table.node_distributions:
            return None

        # Get all cluster nodes
        all_nodes = set()
        try:
            nodes_info = self.client.get_nodes_info()
            all_nodes = {node.name for node in nodes_info if node.name}
        except Exception:
            # If we can't get node info, use nodes that have shards
            all_nodes = set(table.node_distributions.keys())

        nodes_with_shards = set(table.node_distributions.keys())
        nodes_without_shards = all_nodes - nodes_with_shards

        # Only flag as an anomaly if we have missing nodes and the table is significant
        if not nodes_without_shards or table.total_primary_size_gb < 10.0:
            return None

        coverage_ratio = len(nodes_with_shards) / len(all_nodes)

        # Consider it an anomaly if coverage < 70%
        if coverage_ratio >= 0.7:
            return None

        impact_score = min(table.total_primary_size_gb / 100.0, 10.0)
        severity_score = (1 - coverage_ratio) * 10  # Higher severity for lower coverage
        combined_score = impact_score * severity_score

        recommendations = [f"Consider adding replicas to nodes: {', '.join(sorted(nodes_without_shards))}"]

        return DistributionAnomaly(
            table=table,
            anomaly_type="Node Coverage Issue",
            severity_score=severity_score,
            impact_score=impact_score,
            combined_score=combined_score,
            description=f"Limited node coverage ({len(nodes_with_shards)}/{len(all_nodes)} nodes)",
            details={
                "coverage_ratio": coverage_ratio,
                "nodes_with_shards": sorted(nodes_with_shards),
                "nodes_without_shards": sorted(nodes_without_shards),
            },
            recommendations=recommendations,
        )

    def detect_document_imbalance(self, table: TableDistribution) -> Optional[DistributionAnomaly]:
        """Detect imbalances in document distribution"""
        if not table.node_distributions:
            return None

        document_counts = [node["total_documents"] for node in table.node_distributions.values()]

        # Skip if total documents is very low
        if sum(document_counts) < 10000:
            return None

        cv = self.calculate_coefficient_of_variation(document_counts)

        # Consider it an anomaly if CV > 0.5 (50% variation) for documents
        if cv < 0.5:
            return None

        impact_score = min(table.total_primary_size_gb / 100.0, 10.0)
        severity_score = min(cv * 6, 10.0)
        combined_score = impact_score * severity_score

        # Generate recommendations
        recommendations = ["Document imbalance may indicate data skew - consider reviewing shard routing"]

        min_docs = min(document_counts)
        max_docs = max(document_counts)

        if max_docs > min_docs * 3:  # If difference is > 3x
            recommendations.append(f"Significant document skew detected ({min_docs:,} to {max_docs:,} docs per node)")

        return DistributionAnomaly(
            table=table,
            anomaly_type="Document Imbalance",
            severity_score=severity_score,
            impact_score=impact_score,
            combined_score=combined_score,
            description=f"Uneven document distribution (CV: {cv:.2f})",
            details={
                "document_cv": cv,
                "document_counts": {node: data["total_documents"] for node, data in table.node_distributions.items()},
            },
            recommendations=recommendations,
        )

    def analyze_distribution(self, top_tables: int = 10) -> Tuple[List[DistributionAnomaly], int]:
        """Analyze shard distribution and return ranked anomalies"""

        # Get table distributions
        distributions = self.get_largest_tables_distribution(top_tables)

        # Detect all anomalies
        anomalies = []

        for table_dist in distributions:
            # Check each type of anomaly
            for detector in [
                self.detect_shard_count_imbalance,
                self.detect_storage_imbalance,
                self.detect_node_coverage_issues,
                self.detect_document_imbalance,
            ]:
                anomaly = detector(table_dist)
                if anomaly:
                    anomalies.append(anomaly)

        # Sort by combined score (highest first)
        return sorted(anomalies, key=lambda x: x.combined_score, reverse=True), len(distributions)

    def format_distribution_report(self, anomalies: List[DistributionAnomaly], tables_analyzed: int) -> None:
        """Format and display the distribution analysis report"""

        if not anomalies:
            rprint(
                f"[green]✓ No significant shard distribution anomalies "
                f"detected in top {tables_analyzed} tables![/green]"
            )
            return

        # Show analysis scope
        unique_tables = {anomaly.table.full_table_name for anomaly in anomalies}
        rprint(
            f"[blue]📋 Analyzed {tables_analyzed} largest tables, found issues in {len(unique_tables)} tables[/blue]"
        )
        rprint()

        # Summary table
        table = Table(title="🎯 Shard Distribution Anomalies", show_header=True)
        table.add_column("Rank", width=4)
        table.add_column("Table", min_width=20)
        table.add_column("Issue Type", min_width=15)
        table.add_column("Score", width=8)
        table.add_column("Primary Size", width=12)
        table.add_column("Description", min_width=25)

        for i, anomaly in enumerate(anomalies[:10], 1):  # Top 10
            # Color coding by severity
            if anomaly.combined_score >= 50:
                rank_color = "red"
            elif anomaly.combined_score >= 25:
                rank_color = "yellow"
            else:
                rank_color = "blue"

            table.add_row(
                f"[{rank_color}]{i}[/{rank_color}]",
                anomaly.table.full_table_name,
                anomaly.anomaly_type,
                f"[{rank_color}]{anomaly.combined_score:.1f}[/{rank_color}]",
                format_storage_size(anomaly.table.total_primary_size_gb),
                anomaly.description,
            )

        self.console.print(table)

        # Detailed recommendations for top issues
        if anomalies:
            rprint("\n[bold]🔧 Top Recommendations:[/bold]")

            for i, anomaly in enumerate(anomalies[:5], 1):  # Top 5 recommendations
                rprint(f"\n[bold]{i}. {anomaly.table.full_table_name}[/bold] - {anomaly.anomaly_type}")

                # Show the problem analysis first
                rprint(f"   [yellow]🔍 Problem:[/yellow] {anomaly.description}")

                # Add specific details about what's wrong
                if anomaly.anomaly_type == "Shard Count Imbalance":
                    if "shard_counts" in anomaly.details:
                        counts = anomaly.details["shard_counts"]
                        min_count = min(counts.values())
                        max_count = max(counts.values())
                        overloaded = [node for node, count in counts.items() if count == max_count]
                        underloaded = [node for node, count in counts.items() if count == min_count]
                        rprint(
                            f"   [red]⚠  Issue:[/red] {overloaded[0]} has {max_count} shards "
                            f"while {underloaded[0]} has only {min_count} shards"
                        )

                elif anomaly.anomaly_type == "Storage Imbalance":
                    if "storage_sizes" in anomaly.details:
                        sizes = anomaly.details["storage_sizes"]
                        min_size = min(sizes.values())
                        max_size = max(sizes.values())
                        overloaded = [node for node, size in sizes.items() if size == max_size][0]
                        underloaded = [node for node, size in sizes.items() if size == min_size][0]
                        rprint(
                            f"   [red]⚠  Issue:[/red] Storage ranges from {format_storage_size(min_size)} ({underloaded}) "  # noqa: E501
                            f"to {format_storage_size(max_size)} ({overloaded}) - {max_size / min_size:.1f}x difference"
                        )

                elif anomaly.anomaly_type == "Node Coverage Issue":
                    if "nodes_without_shards" in anomaly.details:
                        missing_nodes = anomaly.details["nodes_without_shards"]
                        coverage_ratio = anomaly.details["coverage_ratio"]
                        rprint(
                            f"   [red]⚠  Issue:[/red] Table missing from {len(missing_nodes)} nodes "
                            f"({coverage_ratio:.0%} cluster coverage)"
                        )
                        ellipsis = "..." if len(missing_nodes) > 3 else ""
                        rprint(f"   [dim]   Missing from: {', '.join(missing_nodes[:3])}{ellipsis}[/dim]")

                elif anomaly.anomaly_type == "Document Imbalance":
                    if "document_counts" in anomaly.details:
                        doc_counts = anomaly.details["document_counts"]
                        min_docs = min(doc_counts.values())
                        max_docs = max(doc_counts.values())
                        ratio = max_docs / min_docs if min_docs > 0 else float("inf")
                        rprint(
                            f"   [red]⚠  Issue:[/red] Document counts range "
                            f"from {min_docs:,} to {max_docs:,} ({ratio:.1f}x difference)"
                        )

                # Show recommendations
                rprint("   [green]💡 Solutions:[/green]")
                for rec in anomaly.recommendations:
                    rprint(f"     • {rec}")

        # Summary statistics
        unique_tables = {anomaly.table.full_table_name for anomaly in anomalies}
        rprint("\n[dim]📊 Analysis Summary:[/dim]")
        rprint(f"[dim]• Tables analyzed: {tables_analyzed}[/dim]")
        rprint(f"[dim]• Tables with issues: {len(unique_tables)}[/dim]")
        rprint(f"[dim]• Total anomalies found: {len(anomalies)}[/dim]")
        rprint(f"[dim]• Critical issues (score >50): {len([a for a in anomalies if a.combined_score >= 50])}[/dim]")
        rprint(
            f"[dim]• Warning issues (score 25-50): {len([a for a in anomalies if 25 <= a.combined_score < 50])}[/dim]"
        )
