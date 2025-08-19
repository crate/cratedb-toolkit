"""
Database connection and query functions for CrateDB
"""

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


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


class CrateDBClient:
    """Client for connecting to CrateDB and executing queries"""

    def __init__(self, connection_string: Optional[str] = None):
        load_dotenv()

        self.connection_string = connection_string or os.getenv("CRATE_CONNECTION_STRING")
        if not self.connection_string:
            raise ValueError("CRATE_CONNECTION_STRING not found in environment or provided")

        self.username = os.getenv("CRATE_USERNAME")
        self.password = os.getenv("CRATE_PASSWORD")
        self.ssl_verify = os.getenv("CRATE_SSL_VERIFY", "true").lower() == "true"

        # Ensure connection string ends with _sql endpoint
        if not self.connection_string.endswith("/_sql"):
            self.connection_string = self.connection_string.rstrip("/") + "/_sql"

    def execute_query(self, query: str, parameters: Optional[List] = None) -> Dict[str, Any]:
        """Execute a SQL query against CrateDB"""
        payload = {"stmt": query}

        if parameters:
            payload["args"] = parameters

        auth = None
        if self.username and self.password:
            auth = (self.username, self.password)

        try:
            response = requests.post(
                self.connection_string, json=payload, auth=auth, verify=self.ssl_verify, timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to execute query: {e}") from e

    def get_nodes_info(self) -> List[NodeInfo]:
        """Get information about all nodes in the cluster"""
        query = """
        SELECT
            id,
            name,
            attributes['zone'] as zone,
            heap['used'] as heap_used,
            heap['max'] as heap_max,
            fs['total']['size'] as fs_total,
            fs['total']['used'] as fs_used,
            fs['total']['available'] as fs_available
        FROM sys.nodes
        WHERE name IS NOT NULL
        ORDER BY name
        """

        result = self.execute_query(query)
        nodes = []

        for row in result.get("rows", []):
            nodes.append(
                NodeInfo(
                    id=row[0],
                    name=row[1],
                    zone=row[2] or "unknown",
                    heap_used=row[3] or 0,
                    heap_max=row[4] or 0,
                    fs_total=row[5] or 0,
                    fs_used=row[6] or 0,
                    fs_available=row[7] or 0,
                )
            )

        return nodes

    def get_shards_info(
        self,
        table_name: Optional[str] = None,
        min_size_gb: Optional[float] = None,
        max_size_gb: Optional[float] = None,
        for_analysis: bool = False,
    ) -> List[ShardInfo]:
        """Get information about shards, optionally filtered by table and size

        Args:
            table_name: Filter by specific table
            min_size_gb: Minimum shard size in GB
            max_size_gb: Maximum shard size in GB
            for_analysis: If True, includes all shards regardless of state (for cluster analysis)
                         If False, only includes healthy shards suitable for operations
        """

        where_conditions = []
        if not for_analysis:
            # For operations, only include healthy shards
            where_conditions.extend(["s.routing_state = 'STARTED'", "s.recovery['files']['percent'] = 100.0"])
        parameters = []

        if table_name:
            where_conditions.append("s.table_name = ?")
            parameters.append(table_name)

        if min_size_gb is not None:
            where_conditions.append("s.size >= ?")
            parameters.append(int(min_size_gb * 1024**3))  # Convert GB to bytes

        if max_size_gb is not None:
            where_conditions.append("s.size <= ?")
            parameters.append(int(max_size_gb * 1024**3))  # Convert GB to bytes

        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"

        query = f"""
        SELECT
            s.table_name,
            s.schema_name,
            s.id as shard_id,
            s.node['id'] as node_id,
            s.node['name'] as node_name,
            n.attributes['zone'] as zone,
            s."primary" as is_primary,
            s.size as size_bytes,
            s.size / 1024.0^3 as size_gb,
            s.num_docs,
            s.state,
            s.routing_state
        FROM sys.shards s
        JOIN sys.nodes n ON s.node['id'] = n.id
        {where_clause}
        ORDER BY s.table_name, s.schema_name, s.id, s."primary" DESC
        """  # noqa: S608

        result = self.execute_query(query, parameters)
        shards = []

        for row in result.get("rows", []):
            shards.append(
                ShardInfo(
                    table_name=row[0],
                    schema_name=row[1],
                    shard_id=row[2],
                    node_id=row[3],
                    node_name=row[4],
                    zone=row[5] or "unknown",
                    is_primary=row[6],
                    size_bytes=row[7] or 0,
                    size_gb=float(row[8] or 0),
                    num_docs=row[9] or 0,
                    state=row[10],
                    routing_state=row[11],
                )
            )

        return shards

    def get_shard_distribution_summary(self, for_analysis: bool = True) -> Dict[str, Any]:
        """Get a summary of shard distribution across nodes and zones

        Args:
            for_analysis: If True, includes all shards for complete cluster analysis
                         If False, only includes operational shards
        """
        where_clause = ""
        if not for_analysis:
            where_clause = """
        WHERE s.routing_state = 'STARTED'
            AND s.recovery['files']['percent'] = 100.0"""

        query = f"""
        SELECT
            n.attributes['zone'] as zone,
            s.node['name'] as node_name,
            CASE WHEN s."primary" = true THEN 'PRIMARY' ELSE 'REPLICA' END as shard_type,
            COUNT(*) as shard_count,
            SUM(s.size) / 1024.0^3 as total_size_gb,
            AVG(s.size) / 1024.0^3 as avg_size_gb
        FROM sys.shards s
        JOIN sys.nodes n ON s.node['id'] = n.id{where_clause}
        GROUP BY n.attributes['zone'], s.node['name'], s."primary"
        ORDER BY zone, node_name, shard_type DESC
        """  # noqa: S608

        result = self.execute_query(query)

        summary = {"by_zone": {}, "by_node": {}, "totals": {"primary": 0, "replica": 0, "total_size_gb": 0}}

        for row in result.get("rows", []):
            zone = row[0] or "unknown"
            node_name = row[1]
            shard_type = row[2]
            shard_count = row[3]
            total_size_gb = float(row[4] or 0)
            avg_size_gb = float(row[5] or 0)  # noqa: F841

            # By zone summary
            if zone not in summary["by_zone"]:
                summary["by_zone"][zone] = {"PRIMARY": 0, "REPLICA": 0, "total_size_gb": 0}
            summary["by_zone"][zone][shard_type] += shard_count
            summary["by_zone"][zone]["total_size_gb"] += total_size_gb

            # By node summary
            if node_name not in summary["by_node"]:
                summary["by_node"][node_name] = {"zone": zone, "PRIMARY": 0, "REPLICA": 0, "total_size_gb": 0}
            summary["by_node"][node_name][shard_type] += shard_count
            summary["by_node"][node_name]["total_size_gb"] += total_size_gb

            # Overall totals
            if shard_type == "PRIMARY":
                summary["totals"]["primary"] += shard_count
            else:
                summary["totals"]["replica"] += shard_count
            summary["totals"]["total_size_gb"] += total_size_gb

        return summary

    def test_connection(self) -> bool:
        """Test the connection to CrateDB"""
        try:
            result = self.execute_query("SELECT 1")
            return result.get("rowcount", 0) >= 0
        except Exception:
            return False

    def get_cluster_watermarks(self) -> Dict[str, Any]:
        """Get cluster disk watermark settings"""
        query = """
        SELECT settings['cluster']['routing']['allocation']['disk']['watermark']
        FROM sys.cluster
        """

        try:
            result = self.execute_query(query)
            if result.get("rows"):
                watermarks = result["rows"][0][0] or {}
                return {
                    "low": watermarks.get("low", "Not set"),
                    "high": watermarks.get("high", "Not set"),
                    "flood_stage": watermarks.get("flood_stage", "Not set"),
                    "enable_for_single_data_node": watermarks.get("enable_for_single_data_node", "Not set"),
                }
            return {}
        except Exception:
            return {}

    def get_active_recoveries(
        self, table_name: Optional[str] = None, node_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get shards that are currently in recovery states from sys.allocations"""

        where_conditions = ["current_state != 'STARTED'"]
        parameters = []

        if table_name:
            where_conditions.append("table_name = ?")
            parameters.append(table_name)

        if node_name:
            where_conditions.append("node_id = (SELECT id FROM sys.nodes WHERE name = ?)")
            parameters.append(node_name)

        where_clause = f"WHERE {' AND '.join(where_conditions)}"

        query = f"""
        SELECT
            table_name,
            shard_id,
            current_state,
            explanation,
            node_id
        FROM sys.allocations
        {where_clause}
        ORDER BY current_state, table_name, shard_id
        """  # noqa: S608

        result = self.execute_query(query, parameters)

        allocations = []
        for row in result.get("rows", []):
            allocations.append(
                {
                    "schema_name": "doc",  # Default schema since not available in sys.allocations
                    "table_name": row[0],
                    "shard_id": row[1],
                    "current_state": row[2],
                    "explanation": row[3],
                    "node_id": row[4],
                }
            )

        return allocations

    def get_recovery_details(self, schema_name: str, table_name: str, shard_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed recovery information for a specific shard from sys.shards"""

        # Query for shards that are actively recovering (not completed)
        query = """
        SELECT
            s.table_name,
            s.schema_name,
            s.id as shard_id,
            s.node['name'] as node_name,
            s.node['id'] as node_id,
            s.routing_state,
            s.state,
            s.recovery,
            s.size,
            s."primary",
            s.translog_stats['size'] as translog_size
        FROM sys.shards s
        WHERE s.table_name = ? AND s.id = ?
        AND (s.state = 'RECOVERING' OR s.routing_state IN ('INITIALIZING', 'RELOCATING'))
        ORDER BY s.schema_name
        LIMIT 1
        """

        result = self.execute_query(query, [table_name, shard_id])

        if not result.get("rows"):
            return None

        row = result["rows"][0]
        return {
            "table_name": row[0],
            "schema_name": row[1],
            "shard_id": row[2],
            "node_name": row[3],
            "node_id": row[4],
            "routing_state": row[5],
            "state": row[6],
            "recovery": row[7],
            "size": row[8],
            "primary": row[9],
            "translog_size": row[10] or 0,
        }

    def get_all_recovering_shards(
        self, table_name: Optional[str] = None, node_name: Optional[str] = None, include_transitioning: bool = False
    ) -> List[RecoveryInfo]:
        """Get comprehensive recovery information by combining sys.allocations and sys.shards data"""

        # Step 1: Get active recoveries from allocations (efficient)
        active_allocations = self.get_active_recoveries(table_name, node_name)

        if not active_allocations:
            return []

        recoveries = []

        # Step 2: Get detailed recovery info for each active recovery
        for allocation in active_allocations:
            recovery_detail = self.get_recovery_details(
                allocation["schema_name"],  # This will be 'doc' default
                allocation["table_name"],
                allocation["shard_id"],
            )

            if recovery_detail and recovery_detail.get("recovery"):
                # Update allocation with actual schema from sys.shards
                allocation["schema_name"] = recovery_detail["schema_name"]
                recovery_info = self._parse_recovery_info(allocation, recovery_detail)

                # Filter out completed recoveries unless include_transitioning is True
                if include_transitioning or not self._is_recovery_completed(recovery_info):
                    recoveries.append(recovery_info)

        # Sort by recovery type, then by progress
        return sorted(recoveries, key=lambda r: (r.recovery_type, -r.overall_progress))

    def _parse_recovery_info(self, allocation: Dict[str, Any], shard_detail: Dict[str, Any]) -> RecoveryInfo:
        """Parse recovery information from allocation and shard data"""

        recovery = shard_detail.get("recovery", {})

        # Extract recovery progress information
        files_info = recovery.get("files", {})
        size_info = recovery.get("size", {})

        files_percent = float(files_info.get("percent", 0.0))
        bytes_percent = float(size_info.get("percent", 0.0))

        # Calculate actual progress based on recovered vs used
        files_recovered = files_info.get("recovered", 0)
        files_used = files_info.get("used", 1)  # Avoid division by zero
        size_recovered = size_info.get("recovered", 0)
        size_used = size_info.get("used", 1)  # Avoid division by zero

        # Use actual progress if different from reported percent
        actual_files_percent = (files_recovered / files_used * 100.0) if files_used > 0 else files_percent
        actual_size_percent = (size_recovered / size_used * 100.0) if size_used > 0 else bytes_percent

        # Use the more conservative (lower) progress value
        final_files_percent = min(files_percent, actual_files_percent)
        final_bytes_percent = min(bytes_percent, actual_size_percent)

        # Get source node for PEER recoveries
        source_node = None
        if recovery.get("type") == "PEER":
            source_node = self._find_source_node_for_recovery(
                shard_detail["schema_name"],
                shard_detail["table_name"],
                shard_detail["shard_id"],
                shard_detail["node_id"],
            )

        return RecoveryInfo(
            schema_name=shard_detail["schema_name"],
            table_name=shard_detail["table_name"],
            shard_id=shard_detail["shard_id"],
            node_name=shard_detail["node_name"],
            node_id=shard_detail["node_id"],
            recovery_type=recovery.get("type", "UNKNOWN"),
            stage=recovery.get("stage", "UNKNOWN"),
            files_percent=final_files_percent,
            bytes_percent=final_bytes_percent,
            total_time_ms=recovery.get("total_time", 0),
            routing_state=shard_detail["routing_state"],
            current_state=allocation["current_state"],
            is_primary=shard_detail["primary"],
            size_bytes=shard_detail.get("size", 0),
            source_node_name=source_node,
            translog_size_bytes=shard_detail.get("translog_size", 0),
        )

    def _find_source_node_for_recovery(
        self, schema_name: str, table_name: str, shard_id: int, target_node_id: str
    ) -> Optional[str]:
        """Find source node for PEER recovery by looking for primary or other replicas"""
        try:
            # First try to find the primary shard of the same table/shard
            query = """
            SELECT node['name'] as node_name
            FROM sys.shards
            WHERE schema_name = ? AND table_name = ? AND id = ?
            AND state = 'STARTED' AND node['id'] != ?
            AND "primary" = true
            LIMIT 1
            """

            result = self.execute_query(query, [schema_name, table_name, shard_id, target_node_id])

            if result.get("rows"):
                return result["rows"][0][0]

            # If no primary found, look for any started replica
            query_replica = """
            SELECT node['name'] as node_name
            FROM sys.shards
            WHERE schema_name = ? AND table_name = ? AND id = ?
            AND state = 'STARTED' AND node['id'] != ?
            LIMIT 1
            """

            result = self.execute_query(query_replica, [schema_name, table_name, shard_id, target_node_id])

            if result.get("rows"):
                return result["rows"][0][0]

        except Exception:
            # If query fails, just return None
            logger.warning("Failed to find source node for recovery", exc_info=True)

        return None

    def _is_recovery_completed(self, recovery_info: RecoveryInfo) -> bool:
        """Check if a recovery is completed but still transitioning"""
        return (
            recovery_info.stage == "DONE"
            and recovery_info.files_percent >= 100.0
            and recovery_info.bytes_percent >= 100.0
        )
