"""
Shard analysis and rebalancing logic for CrateDB
"""

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from .database import CrateDBClient, NodeInfo, RecoveryInfo, ShardInfo


@dataclass
class MoveRecommendation:
    """Recommendation for moving a shard"""

    table_name: str
    schema_name: str
    shard_id: int
    from_node: str
    to_node: str
    from_zone: str
    to_zone: str
    shard_type: str
    size_gb: float
    reason: str

    def to_sql(self) -> str:
        """Generate the SQL command for this move"""
        return (
            f'ALTER TABLE "{self.schema_name}"."{self.table_name}" '
            f"REROUTE MOVE SHARD {self.shard_id} "
            f"FROM '{self.from_node}' TO '{self.to_node}';"
        )

    @property
    def safety_score(self) -> float:
        """Calculate a safety score for this move (0-1, higher is safer)"""
        score = 1.0

        # Penalize if moving to same zone (not ideal for zone distribution)
        if self.from_zone == self.to_zone:
            score -= 0.3

        # Bonus for zone balancing moves
        if "rebalancing" in self.reason.lower():
            score += 0.2

        # Ensure score stays in valid range
        return max(0.0, min(1.0, score))


@dataclass
class DistributionStats:
    """Statistics about shard distribution"""

    total_shards: int
    total_size_gb: float
    zones: Dict[str, int]
    nodes: Dict[str, int]
    zone_balance_score: float  # 0-100, higher is better
    node_balance_score: float  # 0-100, higher is better


class ShardAnalyzer:
    """Analyzer for CrateDB shard distribution and rebalancing"""

    def __init__(self, client: CrateDBClient):
        self.client = client
        self.nodes: List[NodeInfo] = []
        self.shards: List[ShardInfo] = []

        # Initialize session-based caches for performance
        self._zone_conflict_cache = {}
        self._node_lookup_cache = {}
        self._target_nodes_cache = {}
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
        zone_counts = defaultdict(int)
        node_counts = defaultdict(int)

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
        zone_stats = defaultdict(lambda: {"PRIMARY": 0, "REPLICA": 0, "TOTAL": 0})

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
            else:
                continue

        # Sort by available space (most space first) - prioritize nodes with more free space
        available_nodes.sort(key=lambda n: n.available_space_gb, reverse=True)
        return available_nodes

    def generate_rebalancing_recommendations(
        self,
        table_name: Optional[str] = None,
        min_size_gb: float = 40.0,
        max_size_gb: float = 60.0,
        zone_tolerance_percent: float = 10.0,
        min_free_space_gb: float = 100.0,
        max_recommendations: int = 10,
        prioritize_space: bool = False,
        source_node: Optional[str] = None,
        max_disk_usage_percent: float = 90.0,
    ) -> List[MoveRecommendation]:
        """Generate recommendations for rebalancing shards

        Args:
            prioritize_space: If True, prioritizes moving shards from nodes with less available space
                             regardless of zone balance. If False, prioritizes zone balancing first.
            source_node: If specified, only generate recommendations for shards on this node
            max_disk_usage_percent: Maximum disk usage percentage for target nodes
        """
        recommendations = []

        # Get moveable shards (only healthy ones for actual operations)
        moveable_shards = self.find_moveable_shards(min_size_gb, max_size_gb, table_name)

        print(f"Analyzing {len(moveable_shards)} candidate shards in size range {min_size_gb}-{max_size_gb}GB...")

        if not moveable_shards:
            return recommendations

        # Analyze current zone balance
        zone_stats = self.check_zone_balance(table_name, zone_tolerance_percent)

        # Calculate target distribution
        total_shards = sum(stats["TOTAL"] for stats in zone_stats.values())
        zones = list(zone_stats.keys())
        target_per_zone = total_shards // len(zones) if zones else 0

        # Find zones that are over/under capacity
        overloaded_zones = []
        underloaded_zones = []

        for zone, stats in zone_stats.items():
            current_count = stats["TOTAL"]
            threshold_high = target_per_zone * (1 + zone_tolerance_percent / 100)
            threshold_low = target_per_zone * (1 - zone_tolerance_percent / 100)

            if current_count > threshold_high:
                overloaded_zones.append(zone)
            elif current_count < threshold_low:
                underloaded_zones.append(zone)

        # Optimize processing: if filtering by source node, only process those shards
        if source_node:
            processing_shards = [s for s in moveable_shards if s.node_name == source_node]
            print(f"Focusing on {len(processing_shards)} shards from node {source_node}")
        else:
            processing_shards = moveable_shards

        # Generate move recommendations
        safe_recommendations = 0  # noqa: F841
        total_evaluated = 0

        for i, shard in enumerate(processing_shards):
            if len(recommendations) >= max_recommendations:
                break

            # Show progress every 50 shards when processing many
            if len(processing_shards) > 100 and i > 0 and i % 50 == 0:
                print(".", end="", flush=True)

            total_evaluated += 1

            # Skip based on priority mode
            if not prioritize_space:
                # Zone balancing mode: only move shards from overloaded zones
                if shard.zone not in overloaded_zones:
                    continue
            # In space priority mode, consider all shards regardless of zone balance

            # Find target nodes, excluding the source node and prioritizing by available space (with caching)
            target_nodes = self._find_nodes_with_capacity_cached(
                required_space_gb=shard.size_gb,
                exclude_nodes={shard.node_name},  # Don't move to same node
                min_free_space_gb=min_free_space_gb,
                max_disk_usage_percent=max_disk_usage_percent,
            )

            # Quick pre-filter to avoid expensive safety validations
            # Only check nodes in different zones (for zone balancing)
            if not prioritize_space:
                target_nodes = [node for node in target_nodes if node.zone != shard.zone]

            # Limit to top 3 candidates to reduce validation overhead
            target_nodes = target_nodes[:3]

            # Filter target nodes to find safe candidates
            safe_target_nodes = []
            for candidate_node in target_nodes:
                # Create a temporary recommendation to test safety
                temp_rec = MoveRecommendation(
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
                is_safe, safety_msg = self.validate_move_safety(temp_rec, max_disk_usage_percent)
                if is_safe:
                    safe_target_nodes.append(candidate_node)

            if not safe_target_nodes:
                continue  # No safe targets found, skip this shard

            if prioritize_space:
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
                target_node = None

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
            if prioritize_space:
                if shard.zone == target_node.zone:
                    reason = f"Space optimization within {shard.zone}"
                else:
                    reason = f"Space optimization: {shard.zone} -> {target_node.zone}"
            else:
                reason = f"Zone rebalancing: {shard.zone} -> {target_node.zone}"
                if shard.zone == target_node.zone:
                    reason = f"Node balancing within {shard.zone}"

            recommendation = MoveRecommendation(
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
        print(f"Generated {len(recommendations)} move recommendations (evaluated {total_evaluated} shards)")
        print(f"Performance: {self.get_cache_stats()}")
        return recommendations

    def validate_move_safety(
        self, recommendation: MoveRecommendation, max_disk_usage_percent: float = 90.0
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
        required_space_gb = recommendation.size_gb + 50  # 50GB buffer
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

    def _check_zone_conflict_cached(self, recommendation: MoveRecommendation) -> Optional[str]:
        """Check zone conflicts with caching"""
        # Create cache key: table, shard, target zone
        target_zone = self._get_node_zone(recommendation.to_node)
        cache_key = (recommendation.table_name, recommendation.shard_id, target_zone)

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

    def _check_zone_conflict(self, recommendation: MoveRecommendation) -> Optional[str]:
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

    def get_cluster_overview(self) -> Dict[str, Any]:
        """Get a comprehensive overview of the cluster"""
        # Get cluster watermark settings
        watermarks = self.client.get_cluster_watermarks()

        overview = {
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
                temp_rec = MoveRecommendation(
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
                    MoveRecommendation(
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

        # Add capacity warnings
        if feasible:
            # Check if remaining cluster capacity is sufficient after decommission
            remaining_capacity = sum(n.available_space_gb for n in self.nodes if n.name != node_name)
            if remaining_capacity < total_size_gb * 1.2:  # 20% safety margin
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
            "estimated_time_hours": len(move_plan) * 0.1,  # Rough estimate: 6 minutes per move
            "message": "Decommission plan generated" if feasible else "Decommission not currently feasible",
        }


class RecoveryMonitor:
    """Monitor shard recovery operations"""

    def __init__(self, client: CrateDBClient):
        self.client = client

    def get_cluster_recovery_status(
        self,
        table_name: Optional[str] = None,
        node_name: Optional[str] = None,
        recovery_type_filter: str = "all",
        include_transitioning: bool = False,
    ) -> List[RecoveryInfo]:
        """Get comprehensive recovery status with minimal cluster impact"""

        # Get all recovering shards using the efficient combined query
        recoveries = self.client.get_all_recovering_shards(table_name, node_name, include_transitioning)

        # Apply recovery type filter
        if recovery_type_filter != "all":
            recoveries = [r for r in recoveries if r.recovery_type.upper() == recovery_type_filter.upper()]

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
