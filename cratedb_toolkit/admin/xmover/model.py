import dataclasses
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class NodeInfo:
    """Information about a CrateDB node"""

    id: str
    name: str
    zone: str
    heap_used: int
    heap_max: int
    fs_total: int
    fs_used: int
    fs_available: int

    @property
    def heap_usage_percent(self) -> float:
        return (self.heap_used / self.heap_max) * 100 if self.heap_max > 0 else 0

    @property
    def disk_usage_percent(self) -> float:
        return (self.fs_used / self.fs_total) * 100 if self.fs_total > 0 else 0

    @property
    def available_space_gb(self) -> float:
        return self.fs_available / (1024**3)


@dataclass
class ShardInfo:
    """Information about a shard"""

    table_name: str
    schema_name: str
    shard_id: int
    node_id: str
    node_name: str
    zone: str
    is_primary: bool
    size_bytes: int
    size_gb: float
    num_docs: int
    state: str
    routing_state: str

    @property
    def shard_type(self) -> str:
        return "PRIMARY" if self.is_primary else "REPLICA"


@dataclass
class RecoveryInfo:
    """Information about an active shard recovery"""

    schema_name: str
    table_name: str
    shard_id: int
    node_name: str
    node_id: str
    recovery_type: str  # PEER, DISK, etc.
    stage: str  # INIT, INDEX, VERIFY_INDEX, TRANSLOG, FINALIZE, DONE
    files_percent: float
    bytes_percent: float
    total_time_ms: int
    routing_state: str  # INITIALIZING, RELOCATING, etc.
    current_state: str  # from allocations
    is_primary: bool
    size_bytes: int
    source_node_name: Optional[str] = None  # Source node for PEER recoveries
    translog_size_bytes: int = 0  # Translog size in bytes

    @property
    def overall_progress(self) -> float:
        """Calculate overall progress percentage"""
        return max(self.files_percent, self.bytes_percent)

    @property
    def size_gb(self) -> float:
        """Size in GB"""
        return self.size_bytes / (1024**3)

    @property
    def shard_type(self) -> str:
        return "PRIMARY" if self.is_primary else "REPLICA"

    @property
    def total_time_seconds(self) -> float:
        """Total time in seconds"""
        return self.total_time_ms / 1000.0

    @property
    def translog_size_gb(self) -> float:
        """Translog size in GB"""
        return self.translog_size_bytes / (1024**3)

    @property
    def translog_percentage(self) -> float:
        """Translog size as percentage of shard size"""
        return (self.translog_size_bytes / self.size_bytes * 100) if self.size_bytes > 0 else 0


@dataclass
class ShardMoveRequest:
    """Request for moving a shard"""

    schema_table: str
    shard_id: int
    from_node: str
    to_node: str
    max_disk_usage: float


@dataclass
class ShardMoveRecommendation:
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


@dataclasses.dataclass
class SizeCriteria:
    min_size: float = 40.0
    max_size: float = 60.0
    table_name: Optional[str] = None
    source_node: Optional[str] = None


@dataclasses.dataclass
class RecommendationConstraints:
    min_size: float = 40.0
    max_size: float = 60.0
    table_name: Optional[str] = None
    source_node: Optional[str] = None
    zone_tolerance: float = 10.0
    min_free_space: float = 100.0
    max_recommendations: int = 10
    max_disk_usage: float = 90.0
    prioritize_space: bool = False
