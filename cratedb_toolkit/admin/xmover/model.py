from dataclasses import dataclass
from typing import Optional


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
