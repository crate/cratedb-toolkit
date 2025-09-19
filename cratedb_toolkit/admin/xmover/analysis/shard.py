"""
Shard analysis and rebalancing logic for CrateDB
"""

import logging
import math
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from cratedb_toolkit.admin.xmover.model import (
    ActiveShardActivity,
    ActiveShardSnapshot,
    DistributionStats,
    NodeInfo,
    ShardInfo,
    ShardRelocationConstraints,
    ShardRelocationResponse,
)
from cratedb_toolkit.admin.xmover.util.database import CrateDBClient
from cratedb_toolkit.admin.xmover.util.format import format_percentage, format_size

logger = logging.getLogger(__name__)

console = Console()


class ShardAnalyzer:
    """Analyzer for CrateDB shard distribution and rebalancing"""

    def __init__(self, client: CrateDBClient):
        self.client = client
        self.nodes: List[NodeInfo] = []
        self.shards: List[ShardInfo] = []

        # Initialize session-based caches for performance.
        self._zone_conflict_cache: Dict[Tuple[str, str, int, str], Union[str, None]] = {}
        self._node_lookup_cache: Dict[str, Union[NodeInfo, None]] = {}
        self._target_nodes_cache: Dict[Tuple[float, frozenset[Any], float, float], List[NodeInfo]] = {}
        self._cache_hits = 0
        self._cache_misses = 0

        self._refresh_data()

    def _refresh_data(self):
        """Refresh node and shard data from the database"""
        self.nodes = self.client.get_nodes_info()
        # For analysis, get all shards regardless of state
        self.shards = self.client.get_shards_info(for_analysis=True)

    def analyze_distribution(self, table_name: Optional[str] = None) -> DistributionStats:
        """Analyze the current shard distribution"""
        # Filter shards by table if specified
        shards = self.shards
        if table_name:
            shards = [s for s in shards if s.table_name == table_name]

        if not shards:
            return DistributionStats(0, 0.0, {}, {}, 100.0, 100.0)

        total_shards = len(shards)
        total_size_gb = sum(s.size_gb for s in shards)

        # Count by zone and node
        zone_counts: Dict[str, int] = defaultdict(int)
        node_counts: Dict[str, int] = defaultdict(int)

        for shard in shards:
            zone_counts[shard.zone] += 1
            node_counts[shard.node_name] += 1

        # Calculate balance scores
        zone_balance_score = self._calculate_balance_score(list(zone_counts.values()))
        node_balance_score = self._calculate_balance_score(list(node_counts.values()))

        return DistributionStats(
            total_shards=total_shards,
            total_size_gb=total_size_gb,
            zones=dict(zone_counts),
            nodes=dict(node_counts),
            zone_balance_score=zone_balance_score,
            node_balance_score=node_balance_score,
        )

    def _calculate_balance_score(self, counts: List[int]) -> float:
        """Calculate a balance score (0-100) for a distribution"""
        if not counts or len(counts) <= 1:
            return 100.0

        mean_count = sum(counts) / len(counts)
        if mean_count == 0:
            return 100.0

        # Calculate coefficient of variation
        variance = sum((count - mean_count) ** 2 for count in counts) / len(counts)
        std_dev = math.sqrt(variance)
        cv = std_dev / mean_count

        # Convert to score (lower CV = higher score)
        # CV of 0 = 100%, CV of 1 = ~37%, CV of 2 = ~14%
        score = max(0, 100 * math.exp(-cv))
        return round(score, 1)

    def find_moveable_shards(
        self, min_size_gb: float = 40.0, max_size_gb: float = 60.0, table_name: Optional[str] = None
    ) -> List[ShardInfo]:
        """Find shards that are candidates for moving based on size

        Only returns healthy shards that are safe to move.
        Prioritizes shards from nodes with less available space.
        """
        # Get only healthy shards (STARTED + 100% recovered) for safe operations
        healthy_shards = self.client.get_shards_info(
            table_name=table_name,
            min_size_gb=min_size_gb,
            max_size_gb=max_size_gb,
            for_analysis=False,  # Only operational shards
        )

        # Create a mapping of node names to available space
        node_space_map = {node.name: node.available_space_gb for node in self.nodes}

        # Sort by node available space (ascending, so low space nodes first), then by shard size
        healthy_shards.sort(key=lambda s: (node_space_map.get(s.node_name, float("inf")), s.size_gb))
        return healthy_shards

    def check_zone_balance(
        self, table_name: Optional[str] = None, tolerance_percent: float = 10.0
    ) -> Dict[str, Dict[str, int]]:
        """Check if zones are balanced within tolerance"""
        # Filter shards by table if specified
        shards = self.shards
        if table_name:
            shards = [s for s in shards if s.table_name == table_name]

        # Count shards by zone and type
        zone_stats: Dict[str, Dict] = defaultdict(lambda: {"PRIMARY": 0, "REPLICA": 0, "TOTAL": 0})

        for shard in shards:
            shard_type = shard.shard_type
            zone_stats[shard.zone][shard_type] += 1
            zone_stats[shard.zone]["TOTAL"] += 1

        return dict(zone_stats)

    def find_nodes_with_capacity(
        self,
        required_space_gb: float,
        exclude_zones: Optional[Set[str]] = None,
        exclude_nodes: Optional[Set[str]] = None,
        min_free_space_gb: float = 100.0,
        max_disk_usage_percent: float = 85.0,
    ) -> List[NodeInfo]:
        """Find nodes that have capacity for additional shards

        Args:
            required_space_gb: Minimum space needed for the shard
            exclude_zones: Zones to exclude from consideration
            exclude_nodes: Specific nodes to exclude
            min_free_space_gb: Additional buffer space required
            max_disk_usage_percent: Maximum disk usage percentage allowed
        """
        available_nodes = []

        for node in self.nodes:
            # Skip zones we want to exclude
            if exclude_zones and node.zone in exclude_zones:
                continue

            # Skip specific nodes we want to exclude
            if exclude_nodes and node.name in exclude_nodes:
                continue

            # Check disk usage threshold
            if node.disk_usage_percent > max_disk_usage_percent:
                continue

            # Check if node has enough free space
            free_space_gb = node.available_space_gb
            if free_space_gb >= (required_space_gb + min_free_space_gb):
                available_nodes.append(node)

        # Sort by available space (most space first) - prioritize nodes with more free space
        available_nodes.sort(key=lambda n: n.available_space_gb, reverse=True)
        return available_nodes

    def generate_rebalancing_recommendations(
        self, constraints: ShardRelocationConstraints
    ) -> List[ShardRelocationResponse]:
        """Generate recommendations for rebalancing shards

        Args:
            prioritize_space: If True, prioritizes moving shards from nodes with less available space
                             regardless of zone balance. If False, prioritizes zone balancing first.
            source_node: If specified, only generate recommendations for shards on this node
            max_disk_usage_percent: Maximum disk usage percentage for target nodes
        """
        recommendations: List[ShardRelocationResponse] = []

        # Get moveable shards (only healthy ones for actual operations)
        moveable_shards = self.find_moveable_shards(constraints.min_size, constraints.max_size, constraints.table_name)

        logger.info(
            f"Analyzing {len(moveable_shards)} candidate shards "
            f"in size range {constraints.min_size}-{constraints.max_size}GB..."
        )

        if not moveable_shards:
            return recommendations

        # Analyze current zone balance
        zone_stats = self.check_zone_balance(constraints.table_name, constraints.zone_tolerance)

        # Calculate target distribution
        total_shards = sum(stats["TOTAL"] for stats in zone_stats.values())
        zones = list(zone_stats.keys())
        target_per_zone = total_shards // len(zones) if zones else 0

        # Find zones that are over/under capacity
        overloaded_zones = []
        underloaded_zones = []

        for zone, stats in zone_stats.items():
            current_count = stats["TOTAL"]
            threshold_high = target_per_zone * (1 + constraints.zone_tolerance / 100)
            threshold_low = target_per_zone * (1 - constraints.zone_tolerance / 100)

            if current_count > threshold_high:
                overloaded_zones.append(zone)
            elif current_count < threshold_low:
                underloaded_zones.append(zone)

        # Optimize processing: if filtering by source node, only process those shards
        if constraints.source_node:
            processing_shards = [s for s in moveable_shards if s.node_name == constraints.source_node]
            logger.info(f"Focusing on {len(processing_shards)} shards from node {constraints.source_node}")
        else:
            processing_shards = moveable_shards

        # Generate move recommendations
        total_evaluated = 0

        for i, shard in enumerate(processing_shards):
            if shard is None:
                logger.info(f"Shard not found: {i}")
                continue

            if len(recommendations) >= constraints.max_recommendations:
                logger.info(f"Found {len(recommendations)} recommendations for shard: {shard.shard_id}")
                break

            # Show progress every 50 shards when processing many
            if len(processing_shards) > 100 and i > 0 and i % 50 == 0:
                print(".", end="", flush=True)

            total_evaluated += 1

            # Skip based on priority mode
            if not constraints.prioritize_space:
                # Zone balancing mode: only move shards from overloaded zones
                if shard.zone not in overloaded_zones:
                    continue
            # In space priority mode, consider all shards regardless of zone balance

            # Find target nodes, excluding the source node and prioritizing by available space (with caching)
            target_nodes = self._find_nodes_with_capacity_cached(
                required_space_gb=shard.size_gb,
                exclude_nodes={shard.node_name},  # Don't move to same node
                min_free_space_gb=constraints.min_free_space,
                max_disk_usage_percent=constraints.max_disk_usage,
            )

            # Quick pre-filter to avoid expensive safety validations
            # Only check nodes in different zones (for zone balancing)
            if not constraints.prioritize_space:
                target_nodes = [node for node in target_nodes if node.zone != shard.zone]

            # Limit to top 3 candidates to reduce validation overhead
            target_nodes = target_nodes[:3]

            # Filter target nodes to find safe candidates
            safe_target_nodes = []
            for candidate_node in target_nodes:
                # Create a temporary recommendation to test safety
                temp_rec = ShardRelocationResponse(
                    table_name=shard.table_name,
                    schema_name=shard.schema_name,
                    shard_id=shard.shard_id,
                    from_node=shard.node_name,
                    to_node=candidate_node.name,
                    from_zone=shard.zone,
                    to_zone=candidate_node.zone,
                    shard_type=shard.shard_type,
                    size_gb=shard.size_gb,
                    reason="Safety validation",
                )

                # Check if this move would be safe
                is_safe, safety_msg = self.validate_move_safety(temp_rec, constraints.max_disk_usage)
                if is_safe:
                    safe_target_nodes.append(candidate_node)

            if not safe_target_nodes:
                continue  # No safe targets found, skip this shard

            target_node: NodeInfo
            if constraints.prioritize_space:
                # Space priority mode: choose node with most available space
                target_node = safe_target_nodes[0]  # Already sorted by available space (desc)
            else:
                # Zone balance mode: prefer underloaded zones, then available space
                target_zones = set(underloaded_zones) - {shard.zone}
                preferred_nodes = [n for n in safe_target_nodes if n.zone in target_zones]
                other_nodes = [n for n in safe_target_nodes if n.zone not in target_zones]

                # Choose target node with intelligent priority:
                # 1. If a node has significantly more space (2x) than zone-preferred nodes, prioritize space
                # 2. Otherwise, prefer zone balancing first, then available space

                if preferred_nodes and other_nodes:
                    best_preferred = preferred_nodes[0]  # Most space in preferred zones
                    best_other = other_nodes[0]  # Most space in other zones

                    # If the best "other" node has significantly more space (2x), choose it
                    if best_other.available_space_gb >= (best_preferred.available_space_gb * 2):
                        target_node = best_other
                    else:
                        target_node = best_preferred
                elif preferred_nodes:
                    target_node = preferred_nodes[0]
                elif other_nodes:
                    target_node = other_nodes[0]
                else:
                    continue  # No suitable target found

            # Determine the reason for the move
            if constraints.prioritize_space:
                if shard.zone == target_node.zone:
                    reason = f"Space optimization within {shard.zone}"
                else:
                    reason = f"Space optimization: {shard.zone} -> {target_node.zone}"
            else:
                reason = f"Zone rebalancing: {shard.zone} -> {target_node.zone}"
                if shard.zone == target_node.zone:
                    reason = f"Node balancing within {shard.zone}"

            recommendation = ShardRelocationResponse(
                table_name=shard.table_name,
                schema_name=shard.schema_name,
                shard_id=shard.shard_id,
                from_node=shard.node_name,
                to_node=target_node.name,
                from_zone=shard.zone,
                to_zone=target_node.zone,
                shard_type=shard.shard_type,
                size_gb=shard.size_gb,
                reason=reason,
            )

            recommendations.append(recommendation)

        if len(processing_shards) > 100:
            print()  # New line after progress dots
        logger.info(f"Generated {len(recommendations)} move recommendations (evaluated {total_evaluated} shards)")
        logger.info(f"Performance: {self.get_cache_stats()}")
        return recommendations

    def validate_move_safety(
        self, recommendation: ShardRelocationResponse, max_disk_usage_percent: float = 90.0, buffer_gb: float = 50.0
    ) -> Tuple[bool, str]:
        """Validate that a move recommendation is safe to execute"""
        # Find target node (with caching)
        target_node = self._get_node_cached(recommendation.to_node)

        if not target_node:
            return False, f"Target node '{recommendation.to_node}' not found"

        # Check for zone conflicts (same shard already exists in target zone) - with caching
        zone_conflict = self._check_zone_conflict_cached(recommendation)
        if zone_conflict:
            return False, zone_conflict

        # Check available space
        required_space_gb = recommendation.size_gb + buffer_gb
        if target_node.available_space_gb < required_space_gb:
            return (
                False,
                f"Insufficient space on target node (need {required_space_gb:.1f}GB, "
                f"have {target_node.available_space_gb:.1f}GB)",
            )

        # Check disk usage
        if target_node.disk_usage_percent > max_disk_usage_percent:
            return False, f"Target node disk usage too high ({target_node.disk_usage_percent:.1f}%)"

        return True, "Move appears safe"

    def _get_node_cached(self, node_name: str):
        """Get node by name with caching"""
        if node_name in self._node_lookup_cache:
            self._cache_hits += 1
            return self._node_lookup_cache[node_name]

        # Find node (cache miss)
        self._cache_misses += 1
        target_node = None
        for node in self.nodes:
            if node.name == node_name:
                target_node = node
                break

        self._node_lookup_cache[node_name] = target_node
        return target_node

    def _check_zone_conflict_cached(self, recommendation: ShardRelocationResponse) -> Optional[str]:
        """Check zone conflicts with caching"""
        # Create cache key: table, shard, target zone
        target_zone = self._get_node_zone(recommendation.to_node)
        cache_key = (recommendation.schema_name, recommendation.table_name, recommendation.shard_id, target_zone)

        if cache_key in self._zone_conflict_cache:
            self._cache_hits += 1
            return self._zone_conflict_cache[cache_key]

        # Cache miss - do expensive check
        self._cache_misses += 1
        result = self._check_zone_conflict(recommendation)
        self._zone_conflict_cache[cache_key] = result
        return result

    def _get_node_zone(self, node_name: str) -> str:
        """Get zone for a node name"""
        node = self._get_node_cached(node_name)
        return node.zone if node else "unknown"

    def get_cache_stats(self) -> str:
        """Get cache performance statistics"""
        total = self._cache_hits + self._cache_misses
        if total == 0:
            return "Cache stats: No operations yet"

        hit_rate = (self._cache_hits / total) * 100
        return f"Cache stats: {hit_rate:.1f}% hit rate ({self._cache_hits} hits, {self._cache_misses} misses)"

    def _find_nodes_with_capacity_cached(
        self, required_space_gb: float, exclude_nodes: set, min_free_space_gb: float, max_disk_usage_percent: float
    ) -> List[NodeInfo]:
        """Find nodes with capacity using caching for repeated queries"""
        # Create cache key based on parameters (rounded to avoid float precision issues)
        cache_key = (
            round(required_space_gb, 1),
            frozenset(exclude_nodes),
            round(min_free_space_gb, 1),
            round(max_disk_usage_percent, 1),
        )

        if cache_key in self._target_nodes_cache:
            self._cache_hits += 1
            return self._target_nodes_cache[cache_key]

        # Cache miss - do expensive calculation
        self._cache_misses += 1
        result = self.find_nodes_with_capacity(
            required_space_gb=required_space_gb,
            exclude_nodes=exclude_nodes,
            min_free_space_gb=min_free_space_gb,
            max_disk_usage_percent=max_disk_usage_percent,
        )

        self._target_nodes_cache[cache_key] = result
        return result

    def _check_zone_conflict(self, recommendation: ShardRelocationResponse) -> Optional[str]:
        """Check if moving this shard would create a zone conflict

        Performs comprehensive zone safety analysis:
        - Checks if target node already has a copy of this shard
        - Checks if target zone already has copies
        - Analyzes zone allocation limits and CrateDB's zone awareness rules
        - Ensures move doesn't violate zone-awareness principles
        """
        try:
            # Query to get all copies of this shard across nodes and zones
            query = """
            SELECT
                s.node['id'] as node_id,
                s.node['name'] as node_name,
                n.attributes['zone'] as zone,
                s."primary" as is_primary,
                s.routing_state,
                s.state
            FROM sys.shards s
            JOIN sys.nodes n ON s.node['id'] = n.id
            WHERE s.table_name = ?
                AND s.schema_name = ?
                AND s.id = ?
            ORDER BY s."primary" DESC, zone, node_name
            """

            result = self.client.execute_query(
                query, [recommendation.table_name, recommendation.schema_name, recommendation.shard_id]
            )

            if not result.get("rows"):
                return (
                    f"Cannot find shard {recommendation.shard_id} "
                    f"for table {recommendation.schema_name}.{recommendation.table_name}"
                )

            # Analyze current distribution
            zones_with_copies = set()
            nodes_with_copies = set()
            current_location = None
            healthy_copies = 0
            total_copies = 0
            target_node_id = None

            # Get target node ID for the recommendation
            for node in self.nodes:
                if node.name == recommendation.to_node:
                    target_node_id = node.id
                    break

            if not target_node_id:
                return f"Target node {recommendation.to_node} not found in cluster"

            for row in result["rows"]:
                node_id, node_name, zone, is_primary, routing_state, state = row
                zone = zone or "unknown"
                total_copies += 1

                # Track the shard we're planning to move
                if node_name == recommendation.from_node:
                    current_location = {
                        "zone": zone,
                        "is_primary": is_primary,
                        "routing_state": routing_state,
                        "state": state,
                    }

                # Track all copies for conflict detection
                nodes_with_copies.add(node_id)
                if routing_state == "STARTED" and state == "STARTED":
                    healthy_copies += 1
                    zones_with_copies.add(zone)

            # Validate the shard we're trying to move exists and is healthy
            if not current_location:
                return f"Shard not found on source node {recommendation.from_node}"

            if current_location["routing_state"] != "STARTED":
                return f"Source shard is not in STARTED state (current: {current_location['routing_state']})"

            # CRITICAL CHECK 1: Target node already has a copy of this shard
            if target_node_id in nodes_with_copies:
                return (
                    f"Node conflict: Target node {recommendation.to_node} "
                    f"already has a copy of shard {recommendation.shard_id}"
                )

            # CRITICAL CHECK 2: Target zone already has a copy (zone allocation limits)
            if recommendation.to_zone in zones_with_copies:
                return f"Zone conflict: {recommendation.to_zone} already has a copy of shard {recommendation.shard_id}"

            # CRITICAL CHECK 3: Ensure we're not creating a single point of failure
            if len(zones_with_copies) == 1 and current_location["zone"] in zones_with_copies:
                # This is the only zone with this shard - moving it is good for zone distribution
                pass
            elif len(zones_with_copies) <= 1 and healthy_copies <= 1:
                return (
                    f"Safety concern: Only {healthy_copies} healthy copy(ies) exist. "
                    f"Moving might risk data availability."
                )

            # ADDITIONAL CHECK: Verify zone allocation constraints for this table
            table_zone_query = """
            SELECT
                n.attributes['zone'] as zone,
                COUNT(*) as shard_count
            FROM sys.shards s
            JOIN sys.nodes n ON s.node['id'] = n.id
            WHERE s.table_name = ?
                AND s.schema_name = ?
                AND s.id = ?
                AND s.routing_state = 'STARTED'
            GROUP BY n.attributes['zone']
            ORDER BY zone
            """

            zone_result = self.client.execute_query(
                table_zone_query, [recommendation.table_name, recommendation.schema_name, recommendation.shard_id]
            )

            current_zone_counts = {}
            for row in zone_result.get("rows", []):
                zone_name, count = row
                current_zone_counts[zone_name or "unknown"] = count

            # Check if adding to target zone would violate balance
            target_zone_count = current_zone_counts.get(recommendation.to_zone, 0)
            if target_zone_count > 0:
                return (
                    f"Zone allocation violation: {recommendation.to_zone} "
                    f"would have {target_zone_count + 1} copies after move."
                )

            return None

        except Exception as e:
            # If we can't check, err on the side of caution
            return f"Cannot verify zone safety: {str(e)}"

    def get_shard_size_overview(self) -> Dict[str, Any]:
        """Get shard size distribution analysis"""
        # Only analyze STARTED shards
        started_shards = [s for s in self.shards if s.state == "STARTED"]

        # Define size buckets (in GB)
        size_buckets = {
            "<1GB": {"count": 0, "total_size": 0.0, "avg_size_gb": 0.0},
            "1GB-5GB": {"count": 0, "total_size": 0.0, "avg_size_gb": 0.0},
            "5GB-10GB": {"count": 0, "total_size": 0.0, "avg_size_gb": 0.0},
            "10GB-50GB": {"count": 0, "total_size": 0.0, "avg_size_gb": 0.0},
            ">=50GB": {"count": 0, "total_size": 0.0, "avg_size_gb": 0.0},
        }

        if not started_shards:
            return {
                "total_shards": 0,
                "total_size_gb": 0.0,
                "avg_shard_size_gb": 0.0,
                "size_buckets": size_buckets,
                "large_shards_count": 0,
                "very_small_shards_percentage": 0.0,
            }

        total_shards = len(started_shards)
        total_size_gb = sum(s.size_gb for s in started_shards)
        avg_size_gb = total_size_gb / total_shards if total_shards > 0 else 0.0

        # Categorize shards by size
        large_shards_count = 0  # >50GB shards
        very_small_shards = 0  # <1GB shards (for percentage calculation)

        for shard in started_shards:
            size_gb = shard.size_gb

            if size_gb >= 50:
                size_buckets[">=50GB"]["count"] += 1
                size_buckets[">=50GB"]["total_size"] += size_gb
                large_shards_count += 1
            elif size_gb >= 10:
                size_buckets["10GB-50GB"]["count"] += 1
                size_buckets["10GB-50GB"]["total_size"] += size_gb
            elif size_gb >= 5:
                size_buckets["5GB-10GB"]["count"] += 1
                size_buckets["5GB-10GB"]["total_size"] += size_gb
            elif size_gb >= 1:
                size_buckets["1GB-5GB"]["count"] += 1
                size_buckets["1GB-5GB"]["total_size"] += size_gb
            else:
                size_buckets["<1GB"]["count"] += 1
                size_buckets["<1GB"]["total_size"] += size_gb
                very_small_shards += 1

        # Calculate the average size for each bucket
        for _, bucket_data in size_buckets.items():
            if bucket_data["count"] > 0:
                bucket_data["avg_size_gb"] = bucket_data["total_size"] / bucket_data["count"]
            else:
                bucket_data["avg_size_gb"] = 0.0

        # Calculate the percentage of very small shards (<1GB)
        very_small_percentage = (very_small_shards / total_shards * 100) if total_shards > 0 else 0.0

        return {
            "total_shards": total_shards,
            "total_size_gb": total_size_gb,
            "avg_shard_size_gb": avg_size_gb,
            "size_buckets": size_buckets,
            "large_shards_count": large_shards_count,
            "very_small_shards_percentage": very_small_percentage,
        }

    def get_cluster_overview(self) -> Dict[str, Any]:
        """Get a comprehensive overview of the cluster"""
        # Get cluster watermark settings
        watermarks = self.client.get_cluster_watermarks()

        overview: Dict[str, Any] = {
            "nodes": len(self.nodes),
            "zones": len({node.zone for node in self.nodes}),
            "total_shards": len(self.shards),
            "primary_shards": len([s for s in self.shards if s.is_primary]),
            "replica_shards": len([s for s in self.shards if not s.is_primary]),
            "total_size_gb": sum(s.size_gb for s in self.shards),
            "zone_distribution": defaultdict(int),
            "node_health": [],
            "watermarks": watermarks,
        }

        # Zone distribution
        for shard in self.shards:
            overview["zone_distribution"][shard.zone] += 1
        overview["zone_distribution"] = dict(overview["zone_distribution"])

        # Node health with watermark calculations
        for node in self.nodes:
            node_shards = [s for s in self.shards if s.node_name == node.name]
            watermark_info = self._calculate_node_watermark_remaining(node, watermarks)

            overview["node_health"].append(
                {
                    "name": node.name,
                    "zone": node.zone,
                    "shards": len(node_shards),
                    "size_gb": sum(s.size_gb for s in node_shards),
                    "disk_usage_percent": node.disk_usage_percent,
                    "heap_usage_percent": node.heap_usage_percent,
                    "available_space_gb": node.available_space_gb,
                    "remaining_to_low_watermark_gb": watermark_info["remaining_to_low_gb"],
                    "remaining_to_high_watermark_gb": watermark_info["remaining_to_high_gb"],
                }
            )

        return overview

    def _calculate_node_watermark_remaining(self, node: "NodeInfo", watermarks: Dict[str, Any]) -> Dict[str, float]:
        """Calculate remaining space until watermarks are reached"""

        # Parse watermark percentages
        low_watermark = self._parse_watermark_percentage(watermarks.get("low", "85%"))
        high_watermark = self._parse_watermark_percentage(watermarks.get("high", "90%"))

        # Calculate remaining space to each watermark
        total_space_bytes = node.fs_total
        current_used_bytes = node.fs_used

        # Space that would be used at each watermark
        low_watermark_used_bytes = total_space_bytes * (low_watermark / 100.0)
        high_watermark_used_bytes = total_space_bytes * (high_watermark / 100.0)

        # Remaining space until each watermark (negative if already exceeded)
        remaining_to_low_gb = max(0, (low_watermark_used_bytes - current_used_bytes) / (1024**3))
        remaining_to_high_gb = max(0, (high_watermark_used_bytes - current_used_bytes) / (1024**3))

        return {"remaining_to_low_gb": remaining_to_low_gb, "remaining_to_high_gb": remaining_to_high_gb}

    def _parse_watermark_percentage(self, watermark_value: str) -> float:
        """Parse watermark percentage from string like '85%' or '0.85'"""
        if isinstance(watermark_value, str):
            if watermark_value.endswith("%"):
                return float(watermark_value[:-1])
            else:
                # Handle decimal format like '0.85'
                decimal_value = float(watermark_value)
                if decimal_value <= 1.0:
                    return decimal_value * 100
                return decimal_value
        elif isinstance(watermark_value, (int, float)):
            if watermark_value <= 1.0:
                return watermark_value * 100
            return watermark_value
        else:
            # Default to common values if parsing fails
            return 85.0  # Default low watermark

    def plan_node_decommission(self, node_name: str, min_free_space_gb: float = 100.0) -> Dict[str, Any]:
        """Plan the decommissioning of a node by analyzing required shard moves

        Args:
            node_name: Name of the node to decommission
            min_free_space_gb: Minimum free space required on target nodes

        Returns:
            Dictionary with decommission plan and analysis
        """
        # Find the node to decommission
        target_node = None
        for node in self.nodes:
            if node.name == node_name:
                target_node = node
                break

        if not target_node:
            return {"error": f"Node {node_name} not found in cluster", "feasible": False}

        # Get all shards on this node (only healthy ones for safety)
        node_shards = [s for s in self.shards if s.node_name == node_name and s.routing_state == "STARTED"]

        if not node_shards:
            return {
                "node": node_name,
                "zone": target_node.zone,
                "feasible": True,
                "shards_to_move": 0,
                "total_size_gb": 0,
                "recommendations": [],
                "warnings": [],
                "message": "Node has no healthy shards - safe to decommission",
            }

        # Calculate space requirements
        total_size_gb = sum(s.size_gb for s in node_shards)

        # Find potential target nodes for each shard
        move_plan = []
        warnings = []
        infeasible_moves = []

        for shard in node_shards:
            # Find nodes that can accommodate this shard
            potential_targets = self.find_nodes_with_capacity(
                shard.size_gb, exclude_nodes={node_name}, min_free_space_gb=min_free_space_gb
            )

            if not potential_targets:
                infeasible_moves.append(
                    {
                        "shard": f"{shard.schema_name}.{shard.table_name}[{shard.shard_id}]",
                        "size_gb": shard.size_gb,
                        "reason": "No nodes with sufficient capacity",
                    }
                )
                continue

            # Check for zone conflicts
            safe_targets = []
            for target in potential_targets:
                # Create a temporary recommendation to test zone safety
                temp_rec = ShardRelocationResponse(
                    table_name=shard.table_name,
                    schema_name=shard.schema_name,
                    shard_id=shard.shard_id,
                    from_node=node_name,
                    to_node=target.name,
                    from_zone=shard.zone,
                    to_zone=target.zone,
                    shard_type=shard.shard_type,
                    size_gb=shard.size_gb,
                    reason=f"Node decommission: {node_name}",
                )

                zone_conflict = self._check_zone_conflict(temp_rec)
                if not zone_conflict:
                    safe_targets.append(target)
                else:
                    warnings.append(
                        f"Zone conflict for {shard.schema_name}.{shard.table_name}[{shard.shard_id}]: {zone_conflict}"
                    )

            if safe_targets:
                # Choose the target with most available space
                best_target = safe_targets[0]
                move_plan.append(
                    ShardRelocationResponse(
                        table_name=shard.table_name,
                        schema_name=shard.schema_name,
                        shard_id=shard.shard_id,
                        from_node=node_name,
                        to_node=best_target.name,
                        from_zone=shard.zone,
                        to_zone=best_target.zone,
                        shard_type=shard.shard_type,
                        size_gb=shard.size_gb,
                        reason=f"Node decommission: {node_name}",
                    )
                )
            else:
                infeasible_moves.append(
                    {
                        "shard": f"{shard.schema_name}.{shard.table_name}[{shard.shard_id}]",
                        "size_gb": shard.size_gb,
                        "reason": "Zone conflicts prevent safe move",
                    }
                )

        # Determine feasibility
        feasible = len(infeasible_moves) == 0

        # Safety margin for cluster capacity after decommission
        capacity_safety_margin = 1.2  # 20 % buffer

        # Add capacity warnings
        if feasible:
            # Check if the remaining cluster capacity is sufficient after decommission
            remaining_capacity = sum(n.available_space_gb for n in self.nodes if n.name != node_name)
            if remaining_capacity < total_size_gb * capacity_safety_margin:
                warnings.append(
                    f"Low remaining capacity after decommission. "
                    f"Only {remaining_capacity:.1f}GB available for {total_size_gb:.1f}GB of data"
                )

        return {
            "node": node_name,
            "zone": target_node.zone,
            "feasible": feasible,
            "shards_to_move": len(node_shards),
            "moveable_shards": len(move_plan),
            "total_size_gb": total_size_gb,
            "recommendations": move_plan,
            "infeasible_moves": infeasible_moves,
            "warnings": warnings,
            "estimated_time_hours": len(move_plan) * 0.1,  # Rough estimate: 0.1 hours (6 minutes) per move
            "message": "Decommission plan generated" if feasible else "Decommission not currently feasible",
        }


class TranslogReporter:
    def __init__(self, client: CrateDBClient):
        self.client = client

    def problematic_translogs(self, size_mb: int) -> List[str]:
        """Find and optionally cancel shards with problematic translog sizes."""
        console.print(Panel.fit("[bold blue]Problematic Translog Analysis[/bold blue]"))
        console.print(f"[dim]Looking for replica shards with translog uncommitted size > {size_mb}MB[/dim]")
        console.print()

        # Query to find problematic replica shards
        query = """
                SELECT sh.schema_name, \
                       sh.table_name, \
                       translate(p.values::text, ':{}', '=()') as partition_values, \
                       sh.id                                   AS shard_id, \
                       node['name']                            as node_name, \
                       sh.translog_stats['uncommitted_size'] / 1024^2 AS translog_uncommitted_mb
                FROM
                    sys.shards AS sh
                    LEFT JOIN information_schema.table_partitions p
                ON sh.table_name = p.table_name
                    AND sh.schema_name = p.table_schema
                    AND sh.partition_ident = p.partition_ident
                WHERE
                    sh.state = 'STARTED'
                  AND sh.translog_stats['uncommitted_size'] \
                    > ? * 1024^2
                  AND primary = FALSE
                ORDER BY
                    6 DESC \
                """

        try:
            result = self.client.execute_query(query, [size_mb])
            rows = result.get("rows", [])

            if not rows:
                console.print(f"[green]✓ No replica shards found with translog uncommitted size > {size_mb}MB[/green]")
                return []

            console.print(f"Found {len(rows)} shards with problematic translogs:")
            console.print()

            # Display query results table
            results_table = Table(title=f"Problematic Replica Shards (translog > {size_mb}MB)", box=box.ROUNDED)
            results_table.add_column("Schema", style="cyan")
            results_table.add_column("Table", style="blue")
            results_table.add_column("Partition", style="magenta")
            results_table.add_column("Shard ID", justify="right", style="yellow")
            results_table.add_column("Node", style="green")
            results_table.add_column("Translog MB", justify="right", style="red")

            for row in rows:
                schema_name, table_name, partition_values, shard_id, node_name, translog_mb = row
                partition_display = (
                    partition_values if partition_values and partition_values != "NULL" else "[dim]none[/dim]"
                )
                results_table.add_row(
                    schema_name, table_name, partition_display, str(shard_id), node_name, f"{translog_mb:.1f}"
                )

            console.print(results_table)
            console.print()
            console.print("[bold]Generated ALTER Commands:[/bold]")
            console.print()

            # Generate ALTER commands
            alter_commands = []
            for row in rows:
                schema_name, table_name, partition_values, shard_id, node_name, translog_mb = row

                # Build the ALTER command based on whether it's partitioned
                if partition_values and partition_values != "NULL":
                    # partition_values already formatted like ("sync_day"=1757376000000) from the translate function
                    alter_cmd = (
                        f'ALTER TABLE "{schema_name}"."{table_name}" partition {partition_values} '
                        f"REROUTE CANCEL SHARD {shard_id} on '{node_name}' WITH (allow_primary=False);"
                    )
                else:
                    alter_cmd = (
                        f'ALTER TABLE "{schema_name}"."{table_name}" '
                        f"REROUTE CANCEL SHARD {shard_id} on '{node_name}' WITH (allow_primary=False);"
                    )

                alter_commands.append(alter_cmd)
                console.print(alter_cmd)

            console.print()
            console.print(f"[bold]Total: {len(alter_commands)} ALTER commands generated[/bold]")
            return alter_commands

        except Exception as e:
            console.print(f"[red]Error analyzing problematic translogs: {e}[/red]")
            import traceback

            console.print(f"[dim]{traceback.format_exc()}[/dim]")
            return []


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

        console.print()

        # Shard Size Overview
        size_overview = self.analyzer.get_shard_size_overview()

        size_table = Table(title="Shard Size Distribution", box=box.ROUNDED)
        size_table.add_column("Size Range", style="cyan")
        size_table.add_column("Count", justify="right", style="magenta")
        size_table.add_column("Percentage", justify="right", style="green")
        size_table.add_column("Avg Size", justify="right", style="blue")
        size_table.add_column("Total Size", justify="right", style="yellow")

        total_shards = size_overview["total_shards"]

        # Define color coding thresholds
        large_shards_threshold = 0  # warn if ANY shards >=50GB (red flag)
        small_shards_percentage_threshold = 40  # warn if >40% of shards are small (<1GB)

        for bucket_name, bucket_data in size_overview["size_buckets"].items():
            count = bucket_data["count"]
            avg_size = bucket_data["avg_size_gb"]
            total_size = bucket_data["total_size"]
            percentage = (count / total_shards * 100) if total_shards > 0 else 0

            # Apply color coding
            count_str = str(count)
            percentage_str = f"{percentage:.1f}%"

            # Color code large shards (>=50GB) - ANY large shard is a red flag
            if bucket_name == ">=50GB" and count > large_shards_threshold:
                count_str = f"[red]{count}[/red]"
                percentage_str = f"[red]{percentage:.1f}%[/red]"

            # Color code if too many very small shards (<1GB)
            if bucket_name == "<1GB" and percentage > small_shards_percentage_threshold:
                count_str = f"[yellow]{count}[/yellow]"
                percentage_str = f"[yellow]{percentage:.1f}%[/yellow]"

            size_table.add_row(
                bucket_name,
                count_str,
                percentage_str,
                f"{avg_size:.2f}GB" if avg_size > 0 else "0GB",
                format_size(total_size),
            )

        console.print(size_table)

        # Add warnings if thresholds are exceeded
        warnings = []
        if size_overview["large_shards_count"] > large_shards_threshold:
            warnings.append(
                f"[red]🔥 CRITICAL: {size_overview['large_shards_count']} "
                f"large shards (>=50GB) detected - IMMEDIATE ACTION REQUIRED![/red]"
            )
            warnings.append("[red]   Large shards cause slow recovery, memory pressure, and performance issues[/red]")

        # Calculate the percentage of very small shards (<1GB)
        very_small_count = size_overview["size_buckets"]["<1GB"]["count"]
        very_small_percentage = (very_small_count / total_shards * 100) if total_shards > 0 else 0

        if very_small_percentage > small_shards_percentage_threshold:
            warnings.append(
                f"[yellow]⚠️  {very_small_percentage:.1f}% of shards are very small (<1GB) - "
                f"consider optimizing shard allocation[/yellow]"
            )
            warnings.append("[yellow]   Too many small shards create metadata overhead and reduce efficiency[/yellow]")

        if warnings:
            console.print()
            for warning in warnings:
                console.print(warning)

        console.print()

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


class ActiveShardMonitor:
    """Monitor active shard checkpoint progression over time"""

    def __init__(self, client: CrateDBClient):
        self.client = client

    def compare_snapshots(
        self,
        snapshot1: List[ActiveShardSnapshot],
        snapshot2: List[ActiveShardSnapshot],
        min_activity_threshold: int = 0,
    ) -> List["ActiveShardActivity"]:
        """Compare two snapshots and return activity data for shards present in both

        Args:
            snapshot1: First snapshot (baseline)
            snapshot2: Second snapshot (comparison)
            min_activity_threshold: Minimum checkpoint delta to consider active (default: 0)
        """

        # Create lookup dict for snapshot1
        snapshot1_dict = {snap.shard_identifier: snap for snap in snapshot1}

        activities = []

        for snap2 in snapshot2:
            snap1 = snapshot1_dict.get(snap2.shard_identifier)
            if snap1:
                # Calculate local checkpoint delta
                local_checkpoint_delta = snap2.local_checkpoint - snap1.local_checkpoint
                time_diff = snap2.timestamp - snap1.timestamp

                # Filter based on actual activity between snapshots
                if local_checkpoint_delta >= min_activity_threshold:
                    activity = ActiveShardActivity(
                        schema_name=snap2.schema_name,
                        table_name=snap2.table_name,
                        shard_id=snap2.shard_id,
                        node_name=snap2.node_name,
                        is_primary=snap2.is_primary,
                        partition_ident=snap2.partition_ident,
                        local_checkpoint_delta=local_checkpoint_delta,
                        snapshot1=snap1,
                        snapshot2=snap2,
                        time_diff_seconds=time_diff,
                    )
                    activities.append(activity)

        # Sort by activity (highest checkpoint delta first)
        activities.sort(key=lambda x: x.local_checkpoint_delta, reverse=True)

        return activities

    def format_activity_display(
        self, activities: List["ActiveShardActivity"], show_count: int = 10, watch_mode: bool = False
    ) -> str:
        """Format activity data for console display"""
        if not activities:
            return "✅ No active shards with significant checkpoint progression found"

        # Limit to requested count
        activities = activities[:show_count]

        # Calculate observation period for context
        if activities:
            observation_period = activities[0].time_diff_seconds
            output = [
                f"\n🔥 Most Active Shards ({len(activities)} shown, {observation_period:.0f}s observation period)"
            ]
        else:
            output = [f"\n🔥 Most Active Shards ({len(activities)} shown, sorted by checkpoint activity)"]

        output.append("")

        # Add activity rate context
        if activities:
            total_activity = sum(a.local_checkpoint_delta for a in activities)
            avg_rate = sum(a.activity_rate for a in activities) / len(activities)
            output.append(
                f"[dim]Total checkpoint activity: {total_activity:,} changes, Average rate: {avg_rate:.1f}/sec[/dim]"
            )
            output.append("")

        # Create table headers
        headers = ["Rank", "Schema.Table", "Shard", "Partition", "Node", "Type", "Checkpoint Δ", "Rate/sec", "Trend"]

        # Calculate column widths
        col_widths = [len(h) for h in headers]

        # Prepare rows
        rows = []
        for i, activity in enumerate(activities, 1):
            # Format values
            rank = str(i)
            table_id = activity.table_identifier
            shard_id = str(activity.shard_id)
            partition = (
                activity.partition_ident[:14] + "..."
                if len(activity.partition_ident) > 14
                else activity.partition_ident or "-"
            )
            node = activity.node_name
            shard_type = "P" if activity.is_primary else "R"
            checkpoint_delta = f"{activity.local_checkpoint_delta:,}"
            rate = f"{activity.activity_rate:.1f}" if activity.activity_rate >= 0.1 else "<0.1"

            # Calculate activity trend indicator
            if activity.activity_rate >= 100:
                trend = "🔥 HOT"
            elif activity.activity_rate >= 50:
                trend = "📈 HIGH"
            elif activity.activity_rate >= 10:
                trend = "📊 MED"
            else:
                trend = "📉 LOW"

            row = [rank, table_id, shard_id, partition, node, shard_type, checkpoint_delta, rate, trend]
            rows.append(row)

            # Update column widths
            for j, cell in enumerate(row):
                col_widths[j] = max(col_widths[j], len(cell))

        # Format table
        header_row = "   " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths))
        output.append(header_row)
        output.append("   " + "-" * (len(header_row) - 3))

        # Data rows
        for row in rows:
            data_row = "   " + " | ".join(cell.ljust(w) for cell, w in zip(row, col_widths))
            output.append(data_row)

        # Only show legend and insights in non-watch mode
        if not watch_mode:
            output.append("")
            output.append("Legend:")
            output.append("  • Checkpoint Δ: Write operations during observation period")
            output.append("  • Rate/sec: Checkpoint changes per second")
            output.append("  • Partition: partition_ident (truncated if >14 chars, '-' if none)")
            output.append("  • Type: P=Primary, R=Replica")
            output.append("  • Trend: 🔥 HOT (≥100/s), 📈 HIGH (≥50/s), 📊 MED (≥10/s), 📉 LOW (<10/s)")

            # Add insights about activity patterns
            if activities:
                output.append("")
                output.append("Insights:")

                # Count by trend
                hot_count = len([a for a in activities if a.activity_rate >= 100])
                high_count = len([a for a in activities if 50 <= a.activity_rate < 100])
                med_count = len([a for a in activities if 10 <= a.activity_rate < 50])
                low_count = len([a for a in activities if a.activity_rate < 10])

                if hot_count > 0:
                    output.append(f"  • {hot_count} HOT shards (≥100 changes/sec) - consider load balancing")
                if high_count > 0:
                    output.append(f"  • {high_count} HIGH activity shards - monitor capacity")
                if med_count > 0:
                    output.append(f"  • {med_count} MEDIUM activity shards - normal operation")
                if low_count > 0:
                    output.append(f"  • {low_count} LOW activity shards - occasional writes")

                # Identify patterns
                primary_activities = [a for a in activities if a.is_primary]
                if len(primary_activities) == len(activities):
                    output.append("  • All active shards are PRIMARY - normal write pattern")
                elif len(primary_activities) < len(activities) * 0.5:
                    output.append("  • Many REPLICA shards active - possible recovery/replication activity")

                # Node concentration
                nodes = {a.node_name for a in activities}
                if len(nodes) <= 2:
                    output.append(f"  • Activity concentrated on {len(nodes)} node(s) - consider redistribution")

        return "\n".join(output)
