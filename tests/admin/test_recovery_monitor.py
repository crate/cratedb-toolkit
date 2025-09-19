"""
Test script for XMover recovery monitoring functionality

This script tests the recovery monitoring features by creating mock recovery scenarios
and verifying the output formatting and data parsing.
"""

import sys
from typing import Any, Dict
from unittest.mock import Mock

from cratedb_toolkit.admin.xmover.model import RecoveryInfo
from cratedb_toolkit.admin.xmover.operational.monitor import RecoveryMonitor, RecoveryOptions
from cratedb_toolkit.admin.xmover.util.database import CrateDBClient
from cratedb_toolkit.model import DatabaseAddress


def create_mock_allocation(
    schema_name: str, table_name: str, shard_id: int, current_state: str, node_id: str
) -> Dict[str, Any]:
    """Create a mock allocation response"""
    return {
        "schema_name": schema_name,
        "table_name": table_name,
        "shard_id": shard_id,
        "current_state": current_state,
        "node_id": node_id,
        "explanation": None,
    }


def create_mock_shard_detail(
    schema_name: str,
    table_name: str,
    shard_id: int,
    node_name: str,
    node_id: str,
    recovery_type: str,
    stage: str,
    files_percent: float,
    bytes_percent: float,
    total_time: int,
    size: int,
    is_primary: bool,
) -> Dict[str, Any]:
    """Create a mock shard detail response"""
    return {
        "schema_name": schema_name,
        "table_name": table_name,
        "shard_id": shard_id,
        "node_name": node_name,
        "node_id": node_id,
        "routing_state": "RELOCATING",
        "state": "RECOVERING",
        "recovery": {
            "type": recovery_type,
            "stage": stage,
            "files": {"percent": files_percent},
            "size": {"percent": bytes_percent},
            "total_time": total_time,
        },
        "size": size,
        "primary": is_primary,
    }


def test_recovery_info_parsing():
    """Test RecoveryInfo dataclass and its properties"""
    print("Testing RecoveryInfo parsing...")

    recovery = RecoveryInfo(
        schema_name="CURVO",
        table_name="PartioffD",
        partition_values="NULL",
        shard_id=19,
        node_name="data-hot-1",
        node_id="ZH6fBanGSjanGqeSh-sw0A",
        recovery_type="PEER",
        stage="DONE",
        files_percent=100.0,
        bytes_percent=100.0,
        total_time_ms=1555907,
        routing_state="RELOCATING",
        current_state="RELOCATING",
        is_primary=False,
        size_bytes=56565284209,
    )

    # Test properties
    assert recovery.overall_progress == 100.0, f"Expected 100.0, got {recovery.overall_progress}"
    assert abs(recovery.size_gb - 52.681) < 0.01, f"Expected ~52.681, got {recovery.size_gb:.3f}"
    assert recovery.shard_type == "REPLICA", f"Expected REPLICA, got {recovery.shard_type}"
    assert recovery.total_time_seconds == 1555.907, f"Expected 1555.907, got {recovery.total_time_seconds}"

    print("✅ RecoveryInfo parsing tests passed")


def test_database_client_parsing(cratedb):
    """Test database client recovery parsing logic"""
    print("Testing database client recovery parsing...")

    # Create a real client instance to test the parsing method
    client = CrateDBClient.__new__(CrateDBClient)  # Create without calling __init__
    client.username = None
    client.password = None
    client.connection_string = DatabaseAddress.from_string(cratedb.database.dburi).httpuri
    client.ssl_verify = False

    # Create test data
    allocation = create_mock_allocation("CURVO", "PartioffD", 19, "RELOCATING", "node1")
    shard_detail = create_mock_shard_detail(
        "CURVO", "PartioffD", 19, "data-hot-1", "node1", "PEER", "DONE", 100.0, 100.0, 1555907, 56565284209, False
    )

    # Test the parsing method directly
    recovery_info = client._parse_recovery_info(allocation, shard_detail)

    assert recovery_info.recovery_type == "PEER"
    assert recovery_info.stage == "DONE"
    assert recovery_info.overall_progress == 0.0

    print("✅ Database client parsing tests passed")


def test_recovery_monitor_formatting():
    """Test recovery monitor display formatting"""
    print("Testing recovery monitor formatting...")

    # Create mock client
    mock_client = Mock(spec=CrateDBClient)
    monitor = RecoveryMonitor(mock_client)

    # Create test recovery data
    recoveries = [
        RecoveryInfo(
            schema_name="CURVO",
            table_name="PartioffD",
            partition_values="NULL",
            shard_id=19,
            node_name="data-hot-1",
            node_id="node1",
            recovery_type="PEER",
            stage="DONE",
            files_percent=100.0,
            bytes_percent=100.0,
            total_time_ms=1555907,
            routing_state="RELOCATING",
            current_state="RELOCATING",
            is_primary=False,
            size_bytes=56565284209,
        ),
        RecoveryInfo(
            schema_name="CURVO",
            table_name="orderTracking",
            partition_values="NULL",
            shard_id=7,
            node_name="data-hot-2",
            node_id="node2",
            recovery_type="DISK",
            stage="INDEX",
            files_percent=75.5,
            bytes_percent=67.8,
            total_time_ms=890234,
            routing_state="INITIALIZING",
            current_state="INITIALIZING",
            is_primary=True,
            size_bytes=25120456789,
        ),
    ]

    # Test summary generation
    summary = monitor.get_recovery_summary(recoveries)

    assert summary["total_recoveries"] == 2
    assert "PEER" in summary["by_type"]
    assert "DISK" in summary["by_type"]
    assert summary["by_type"]["PEER"]["count"] == 1
    assert summary["by_type"]["DISK"]["count"] == 1

    # Test display formatting
    display_output = monitor.format_recovery_display(recoveries)

    assert "Active Shard Recoveries (2 total)" in display_output
    assert "PEER Recoveries (1)" in display_output
    assert "DISK Recoveries (1)" in display_output
    assert "PartioffD" in display_output
    assert "orderTracking" in display_output

    print("✅ Recovery monitor formatting tests passed")


def test_empty_recovery_handling():
    """Test handling of no active recoveries"""
    print("Testing empty recovery handling...")

    mock_client = Mock(spec=CrateDBClient)
    monitor = RecoveryMonitor(mock_client)

    # Test empty list
    empty_recoveries = []

    summary = monitor.get_recovery_summary(empty_recoveries)
    assert summary["total_recoveries"] == 0
    assert summary["by_type"] == {}

    display_output = monitor.format_recovery_display(empty_recoveries)
    assert "No active shard recoveries found" in display_output

    print("✅ Empty recovery handling tests passed")


def test_recovery_type_filtering():
    """Test filtering by recovery type"""
    print("Testing recovery type filtering...")

    mock_client = Mock(spec=CrateDBClient)

    # Mock the get_all_recovering_shards method
    mock_recoveries = [
        RecoveryInfo(
            schema_name="test",
            table_name="table1",
            partition_values="NULL",
            shard_id=1,
            node_name="node1",
            node_id="n1",
            recovery_type="PEER",
            stage="DONE",
            files_percent=100.0,
            bytes_percent=100.0,
            total_time_ms=1000,
            routing_state="RELOCATING",
            current_state="RELOCATING",
            is_primary=True,
            size_bytes=1000000,
        ),
        RecoveryInfo(
            schema_name="test",
            table_name="table2",
            partition_values="NULL",
            shard_id=2,
            node_name="node2",
            node_id="n2",
            recovery_type="DISK",
            stage="INDEX",
            files_percent=50.0,
            bytes_percent=45.0,
            total_time_ms=2000,
            routing_state="INITIALIZING",
            current_state="INITIALIZING",
            is_primary=False,
            size_bytes=2000000,
        ),
    ]

    mock_client.get_all_recovering_shards.return_value = mock_recoveries

    # Test filtering
    monitor = RecoveryMonitor(mock_client, options=RecoveryOptions(recovery_type="PEER"))
    peer_only = monitor.get_cluster_recovery_status()
    assert len(peer_only) == 1
    assert peer_only[0].recovery_type == "PEER"

    monitor = RecoveryMonitor(mock_client, options=RecoveryOptions(recovery_type="DISK"))
    disk_only = monitor.get_cluster_recovery_status()
    assert len(disk_only) == 1
    assert disk_only[0].recovery_type == "DISK"

    monitor = RecoveryMonitor(mock_client, options=RecoveryOptions(recovery_type="all"))
    all_recoveries = monitor.get_cluster_recovery_status()
    assert len(all_recoveries) == 2

    print("✅ Recovery type filtering tests passed")


def main():
    """Run all tests"""
    print("🧪 Running XMover Recovery Monitor Tests")
    print("=" * 50)

    try:
        test_recovery_info_parsing()
        test_database_client_parsing()
        test_recovery_monitor_formatting()
        test_empty_recovery_handling()
        test_recovery_type_filtering()

        print("\n🎉 All tests passed successfully!")
        print("\n📋 Test Summary:")
        print("   ✅ RecoveryInfo data class and properties")
        print("   ✅ Database client parsing logic")
        print("   ✅ Recovery monitor display formatting")
        print("   ✅ Empty recovery state handling")
        print("   ✅ Recovery type filtering")

        print("\n🚀 Recovery monitoring feature is ready for use!")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
