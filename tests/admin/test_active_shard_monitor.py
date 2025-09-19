"""
Tests for ActiveShardMonitor functionality
"""

import time
from unittest.mock import Mock, patch

from cratedb_toolkit.admin.xmover.analysis.shard import ActiveShardMonitor
from cratedb_toolkit.admin.xmover.model import ActiveShardActivity, ActiveShardSnapshot
from cratedb_toolkit.admin.xmover.util.database import CrateDBClient


class TestActiveShardSnapshot:
    """Test ActiveShardSnapshot dataclass"""

    def test_checkpoint_delta(self):
        """Test checkpoint delta calculation"""
        snapshot = ActiveShardSnapshot(
            schema_name="test_schema",
            table_name="test_table",
            shard_id=1,
            node_name="node1",
            is_primary=True,
            partition_ident="",
            local_checkpoint=1500,
            global_checkpoint=500,
            translog_uncommitted_bytes=10485760,  # 10MB
            timestamp=time.time(),
        )

        assert snapshot.checkpoint_delta == 1000
        assert snapshot.translog_uncommitted_mb == 10.0
        assert snapshot.shard_identifier == "test_schema.test_table:1:node1:P"


class TestActiveShardActivity:
    """Test ActiveShardActivity dataclass"""

    def test_activity_calculations(self):
        """Test activity rate and property calculations"""
        snapshot1 = ActiveShardSnapshot(
            schema_name="test_schema",
            table_name="test_table",
            shard_id=1,
            node_name="node1",
            is_primary=True,
            partition_ident="",
            local_checkpoint=1000,
            global_checkpoint=500,
            translog_uncommitted_bytes=5242880,  # 5MB
            timestamp=100.0,
        )

        snapshot2 = ActiveShardSnapshot(
            schema_name="test_schema",
            table_name="test_table",
            shard_id=1,
            node_name="node1",
            is_primary=True,
            partition_ident="",
            local_checkpoint=1500,
            global_checkpoint=500,
            translog_uncommitted_bytes=10485760,  # 10MB
            timestamp=130.0,  # 30 seconds later
        )

        activity = ActiveShardActivity(
            schema_name="test_schema",
            table_name="test_table",
            shard_id=1,
            node_name="node1",
            is_primary=True,
            partition_ident="",
            local_checkpoint_delta=500,
            snapshot1=snapshot1,
            snapshot2=snapshot2,
            time_diff_seconds=30.0,
        )

        assert activity.activity_rate == 500 / 30.0  # ~16.67 changes/sec
        assert activity.shard_type == "PRIMARY"
        assert activity.table_identifier == "test_schema.test_table"


class TestCrateDBClientActiveShards:
    """Test CrateDB client active shards functionality"""

    @patch.object(CrateDBClient, "execute_query")
    def test_get_active_shards_snapshot_success(self, mock_execute):
        """Test successful snapshot retrieval"""
        mock_execute.return_value = {
            "rows": [
                ["schema1", "table1", 1, True, "node1", "", 10485760, 1500, 500],
                ["schema1", "table2", 2, False, "node2", "part1", 20971520, 2000, 800],
            ]
        }

        client = CrateDBClient("http://test")
        snapshots = client.get_active_shards_snapshot(min_checkpoint_delta=1000)

        assert len(snapshots) == 2

        # Check first snapshot
        snap1 = snapshots[0]
        assert snap1.schema_name == "schema1"
        assert snap1.table_name == "table1"
        assert snap1.shard_id == 1
        assert snap1.is_primary is True
        assert snap1.node_name == "node1"
        assert snap1.local_checkpoint == 1500
        assert snap1.global_checkpoint == 500
        assert snap1.checkpoint_delta == 1000
        assert snap1.translog_uncommitted_mb == 10.0

        # Check second snapshot
        snap2 = snapshots[1]
        assert snap2.schema_name == "schema1"
        assert snap2.table_name == "table2"
        assert snap2.shard_id == 2
        assert snap2.is_primary is False
        assert snap2.node_name == "node2"
        assert snap2.partition_ident == "part1"
        assert snap2.checkpoint_delta == 1200
        assert snap2.translog_uncommitted_mb == 20.0

        # Verify query was called without checkpoint delta filter (new behavior)
        mock_execute.assert_called_once()
        args = mock_execute.call_args[0]
        # No longer passes min_checkpoint_delta parameter
        assert len(args) == 1  # Only the query, no parameters

    @patch.object(CrateDBClient, "execute_query")
    def test_get_active_shards_snapshot_empty(self, mock_execute):
        """Test snapshot retrieval with no results"""
        mock_execute.return_value = {"rows": []}

        client = CrateDBClient("http://test")
        snapshots = client.get_active_shards_snapshot(min_checkpoint_delta=1000)

        assert snapshots == []

    @patch.object(CrateDBClient, "execute_query")
    def test_get_active_shards_snapshot_error(self, mock_execute):
        """Test snapshot retrieval with database error"""
        mock_execute.side_effect = Exception("Database connection failed")

        client = CrateDBClient("http://test")
        snapshots = client.get_active_shards_snapshot(min_checkpoint_delta=1000)

        assert snapshots == []


class TestActiveShardMonitor:
    """Test ActiveShardMonitor class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_client = Mock(spec=CrateDBClient)
        self.monitor = ActiveShardMonitor(self.mock_client)

    def create_test_snapshot(
        self,
        schema: str,
        table: str,
        shard_id: int,
        node: str,
        is_primary: bool,
        local_checkpoint: int,
        timestamp: float,
    ):
        """Helper to create test snapshots"""
        return ActiveShardSnapshot(
            schema_name=schema,
            table_name=table,
            shard_id=shard_id,
            node_name=node,
            is_primary=is_primary,
            partition_ident="",
            local_checkpoint=local_checkpoint,
            global_checkpoint=500,  # Fixed for simplicity
            translog_uncommitted_bytes=10485760,  # 10MB
            timestamp=timestamp,
        )

    def test_compare_snapshots_with_activity(self):
        """Test comparing snapshots with active shards"""
        # Create first snapshot
        snapshot1 = [
            self.create_test_snapshot("schema1", "table1", 1, "node1", True, 1000, 100.0),
            self.create_test_snapshot("schema1", "table2", 1, "node2", False, 2000, 100.0),
            self.create_test_snapshot("schema1", "table3", 1, "node1", True, 3000, 100.0),
        ]

        # Create second snapshot (30 seconds later with activity)
        snapshot2 = [
            self.create_test_snapshot("schema1", "table1", 1, "node1", True, 1500, 130.0),  # +500
            self.create_test_snapshot("schema1", "table2", 1, "node2", False, 2200, 130.0),  # +200
            self.create_test_snapshot("schema1", "table3", 1, "node1", True, 3000, 130.0),  # No change
            self.create_test_snapshot("schema1", "table4", 1, "node3", True, 1000, 130.0),  # New shard
        ]

        activities = self.monitor.compare_snapshots(snapshot1, snapshot2, min_activity_threshold=1)

        # Should have 2 activities (table3 had no change, table4 is new)
        assert len(activities) == 2

        # Check activities are sorted by checkpoint delta (highest first)
        assert activities[0].local_checkpoint_delta == 500  # table1
        assert activities[0].schema_name == "schema1"
        assert activities[0].table_name == "table1"

        assert activities[1].local_checkpoint_delta == 200  # table2
        assert activities[1].schema_name == "schema1"
        assert activities[1].table_name == "table2"

        # Check activity rate calculation
        assert activities[0].activity_rate == 500 / 30.0  # ~16.67/sec
        assert activities[1].activity_rate == 200 / 30.0  # ~6.67/sec

    def test_compare_snapshots_no_activity(self):
        """Test comparing snapshots with no activity"""
        # Create identical snapshots
        snapshot1 = [
            self.create_test_snapshot("schema1", "table1", 1, "node1", True, 1000, 100.0),
        ]

        snapshot2 = [
            self.create_test_snapshot("schema1", "table1", 1, "node1", True, 1000, 130.0),  # No change
        ]

        activities = self.monitor.compare_snapshots(snapshot1, snapshot2, min_activity_threshold=1)

        assert activities == []

    def test_compare_snapshots_no_overlap(self):
        """Test comparing snapshots with no overlapping shards"""
        snapshot1 = [
            self.create_test_snapshot("schema1", "table1", 1, "node1", True, 1000, 100.0),
        ]

        snapshot2 = [
            self.create_test_snapshot("schema1", "table2", 1, "node2", True, 1500, 130.0),  # Different shard
        ]

        activities = self.monitor.compare_snapshots(snapshot1, snapshot2, min_activity_threshold=1)

        assert activities == []

    def test_format_activity_display_with_activities(self):
        """Test formatting activity display with data"""
        # Create test activities
        snapshot1 = self.create_test_snapshot("schema1", "table1", 1, "node1", True, 1000, 100.0)
        snapshot2 = self.create_test_snapshot("schema1", "table1", 1, "node1", True, 1500, 130.0)

        activity = ActiveShardActivity(
            schema_name="schema1",
            table_name="table1",
            shard_id=1,
            node_name="node1",
            is_primary=True,
            partition_ident="",
            local_checkpoint_delta=500,
            snapshot1=snapshot1,
            snapshot2=snapshot2,
            time_diff_seconds=30.0,
        )

        display = self.monitor.format_activity_display([activity], show_count=10, watch_mode=False)

        # Check that output contains expected elements
        assert "Most Active Shards" in display
        assert "schema1.table1" in display
        assert "500" in display  # checkpoint delta
        assert "16.7" in display  # activity rate
        assert "P" in display  # primary indicator
        assert "Legend:" in display
        assert "Trend:" in display  # new trend column explanation
        assert "Partition:" in display  # new partition column explanation

    def test_format_activity_display_empty(self):
        """Test formatting activity display with no data"""
        display = self.monitor.format_activity_display([], show_count=10, watch_mode=False)

        assert "No active shards with significant checkpoint progression found" in display

    def test_format_activity_display_count_limit(self):
        """Test that display respects show_count limit"""
        # Create multiple activities
        activities = []
        for i in range(15):
            snapshot1 = self.create_test_snapshot("schema1", f"table{i}", 1, "node1", True, 1000, 100.0)
            snapshot2 = self.create_test_snapshot("schema1", f"table{i}", 1, "node1", True, 1000 + (i + 1) * 100, 130.0)

            activity = ActiveShardActivity(
                schema_name="schema1",
                table_name=f"table{i}",
                shard_id=1,
                node_name="node1",
                is_primary=True,
                partition_ident="",
                local_checkpoint_delta=(i + 1) * 100,
                snapshot1=snapshot1,
                snapshot2=snapshot2,
                time_diff_seconds=30.0,
            )
            activities.append(activity)

        # Sort activities by checkpoint delta (highest first) - same as compare_snapshots does
        activities.sort(key=lambda x: x.local_checkpoint_delta, reverse=True)

        # Should only show top 5
        display = self.monitor.format_activity_display(activities, show_count=5, watch_mode=False)

        # Count number of table entries in display
        table_count = display.count("schema1.table")
        assert table_count == 5  # Should only show 5 entries

        # Should show highest activity first (table14 has highest checkpoint delta)
        assert "schema1.table14" in display

    def test_compare_snapshots_with_activity_threshold(self):
        """Test filtering activities by minimum threshold"""
        # Create snapshots with various activity levels
        snapshot1 = [
            self.create_test_snapshot("schema1", "table1", 1, "node1", True, 1000, 100.0),  # Will have +2000 delta
            self.create_test_snapshot("schema1", "table2", 1, "node2", False, 2000, 100.0),  # Will have +500 delta
            self.create_test_snapshot("schema1", "table3", 1, "node1", True, 3000, 100.0),  # Will have +100 delta
        ]

        snapshot2 = [
            self.create_test_snapshot("schema1", "table1", 1, "node1", True, 3000, 130.0),  # +2000 delta
            self.create_test_snapshot("schema1", "table2", 1, "node2", False, 2500, 130.0),  # +500 delta
            self.create_test_snapshot("schema1", "table3", 1, "node1", True, 3100, 130.0),  # +100 delta
        ]

        # Test with threshold of 1000 - should only show table1 (2000 delta)
        activities_high_threshold = self.monitor.compare_snapshots(snapshot1, snapshot2, min_activity_threshold=1000)
        assert len(activities_high_threshold) == 1
        assert activities_high_threshold[0].table_name == "table1"
        assert activities_high_threshold[0].local_checkpoint_delta == 2000

        # Test with threshold of 200 - should show table1 and table2
        activities_medium_threshold = self.monitor.compare_snapshots(snapshot1, snapshot2, min_activity_threshold=200)
        assert len(activities_medium_threshold) == 2
        assert activities_medium_threshold[0].local_checkpoint_delta == 2000  # table1 first (highest)
        assert activities_medium_threshold[1].local_checkpoint_delta == 500  # table2 second

        # Test with threshold of 0 - should show all three
        activities_low_threshold = self.monitor.compare_snapshots(snapshot1, snapshot2, min_activity_threshold=0)
        assert len(activities_low_threshold) == 3
        assert activities_low_threshold[0].local_checkpoint_delta == 2000  # Sorted by activity
        assert activities_low_threshold[1].local_checkpoint_delta == 500
        assert activities_low_threshold[2].local_checkpoint_delta == 100

    def test_primary_replica_separation(self):
        """Test that primary and replica shards are tracked separately"""
        # Create snapshots with same table/shard but different primary/replica
        snapshot1 = [
            # Primary shard
            self.create_test_snapshot("gc", "scheduled_jobs_log", 0, "data-hot-8", True, 15876, 100.0),
            # Replica shard (same table/shard/node but different type)
            self.create_test_snapshot("gc", "scheduled_jobs_log", 0, "data-hot-8", False, 129434, 100.0),
        ]

        snapshot2 = [
            # Primary shard progresses normally
            self.create_test_snapshot("gc", "scheduled_jobs_log", 0, "data-hot-8", True, 16000, 130.0),  # +124 delta
            # Replica shard progresses normally
            self.create_test_snapshot("gc", "scheduled_jobs_log", 0, "data-hot-8", False, 129500, 130.0),  # +66 delta
        ]

        activities = self.monitor.compare_snapshots(snapshot1, snapshot2, min_activity_threshold=1)

        # Should have 2 separate activities (primary and replica tracked separately)
        assert len(activities) == 2

        # Find primary and replica activities
        primary_activity = next(a for a in activities if a.is_primary)
        replica_activity = next(a for a in activities if not a.is_primary)

        # Verify deltas are calculated correctly for each type
        assert primary_activity.local_checkpoint_delta == 124  # 16000 - 15876
        assert replica_activity.local_checkpoint_delta == 66  # 129500 - 129434

        # Verify they have different shard identifiers
        assert primary_activity.snapshot1.shard_identifier != replica_activity.snapshot1.shard_identifier
        assert "data-hot-8:P" in primary_activity.snapshot1.shard_identifier
        assert "data-hot-8:R" in replica_activity.snapshot1.shard_identifier

        # This test prevents the bug where we mixed primary CP End with replica CP Start
        # which created fake deltas like 129434 - 15876 = 113558

    def test_partition_separation(self):
        """Test that partitions within the same table/shard are tracked separately"""
        # Create snapshots with same table/shard but different partitions
        snapshot1 = [
            # Partition 1
            self.create_test_snapshot("TURVO", "appointmentFormFieldData_events", 0, "data-hot-8", True, 32684, 100.0),
            # Partition 2 (same table/shard/node/type but different partition)
            self.create_test_snapshot("TURVO", "appointmentFormFieldData_events", 0, "data-hot-8", True, 54289, 100.0),
        ]

        # Modify partition_ident for the snapshots to simulate different partitions
        snapshot1[0].partition_ident = "04732dpl6osj8d1g60o30c1g"
        snapshot1[1].partition_ident = "04732dpl6os3adpm60o30c1g"

        snapshot2 = [
            # Partition 1 progresses
            self.create_test_snapshot("TURVO", "appointmentFormFieldData_events", 0, "data-hot-8", True, 32800, 130.0),
            # +116 delta
            # Partition 2 progresses
            self.create_test_snapshot("TURVO", "appointmentFormFieldData_events", 0, "data-hot-8", True, 54400, 130.0),
            # +111 delta
        ]

        # Set partition_ident for second snapshot
        snapshot2[0].partition_ident = "04732dpl6osj8d1g60o30c1g"
        snapshot2[1].partition_ident = "04732dpl6os3adpm60o30c1g"

        activities = self.monitor.compare_snapshots(snapshot1, snapshot2, min_activity_threshold=1)

        # Should have 2 separate activities (partitions tracked separately)
        assert len(activities) == 2

        # Verify deltas are calculated correctly for each partition
        partition1_activity = next(a for a in activities if "04732dpl6osj8d1g60o30c1g" in a.snapshot1.shard_identifier)
        partition2_activity = next(a for a in activities if "04732dpl6os3adpm60o30c1g" in a.snapshot1.shard_identifier)

        assert partition1_activity.local_checkpoint_delta == 116  # 32800 - 32684
        assert partition2_activity.local_checkpoint_delta == 111  # 54400 - 54289

        # Verify they have different shard identifiers due to partition
        assert partition1_activity.snapshot1.shard_identifier != partition2_activity.snapshot1.shard_identifier
        assert ":04732dpl6osj8d1g60o30c1g" in partition1_activity.snapshot1.shard_identifier
        assert ":04732dpl6os3adpm60o30c1g" in partition2_activity.snapshot1.shard_identifier

        # This test prevents mixing partitions which would create fake activity measurements

    def test_format_activity_display_watch_mode(self):
        """Test that watch mode excludes legend and insights"""
        snapshot1 = self.create_test_snapshot("schema1", "table1", 1, "node1", True, 1000, 100.0)
        snapshot2 = self.create_test_snapshot("schema1", "table1", 1, "node1", True, 1500, 130.0)

        activity = ActiveShardActivity(
            schema_name="schema1",
            table_name="table1",
            shard_id=1,
            node_name="node1",
            is_primary=True,
            partition_ident="",
            local_checkpoint_delta=500,
            snapshot1=snapshot1,
            snapshot2=snapshot2,
            time_diff_seconds=30.0,
        )

        # Test non-watch mode (should include legend and insights)
        normal_display = self.monitor.format_activity_display([activity], show_count=10, watch_mode=False)
        assert "Legend:" in normal_display
        assert "Insights:" in normal_display
        assert "Checkpoint Δ:" in normal_display

        # Test watch mode (should exclude legend and insights)
        watch_display = self.monitor.format_activity_display([activity], show_count=10, watch_mode=True)
        assert "Legend:" not in watch_display
        assert "Insights:" not in watch_display
        assert "Checkpoint Δ" in watch_display  # Core data should still be present

        # But should still contain the core data
        assert "Most Active Shards" in watch_display
        assert "schema1.table1" in watch_display
        assert "500" in watch_display  # checkpoint delta
