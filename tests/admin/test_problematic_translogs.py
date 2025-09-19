"""
Tests for problematic translogs functionality.
"""

from unittest.mock import Mock, patch

from click.testing import CliRunner

from cratedb_toolkit.admin.xmover.cli import main as cli
from cratedb_toolkit.admin.xmover.util.database import CrateDBClient


class TestXMoverProblematicTranslogs:
    def setup_method(self):
        """Set up test fixtures"""
        self.runner = CliRunner()
        self.mock_client = Mock(spec=CrateDBClient)

    def test_no_problematic_shards(self):
        """Test when no shards meet the criteria"""
        self.mock_client.execute_query.return_value = {"rows": []}
        self.mock_client.test_connection.return_value = True

        with patch("cratedb_toolkit.admin.xmover.cli.CrateDBClient", return_value=self.mock_client):
            result = self.runner.invoke(cli, ["problematic-translogs", "--size-mb", "300"], catch_exceptions=False)

        assert result.exit_code == 0, result.output
        assert "No replica shards found" in result.output
        assert "300MB" in result.output

    def test_non_partitioned_table_command_generation(self):
        """Test ALTER command generation for non-partitioned tables"""
        mock_rows = [
            ["TURVO", "shipmentFormFieldData", None, 14, "data-hot-6", 7011.8],
            ["TURVO", "orderFormFieldData", "NULL", 5, "data-hot-1", 469.5],
        ]
        self.mock_client.execute_query.return_value = {"rows": mock_rows}
        self.mock_client.test_connection.return_value = True

        with patch("cratedb_toolkit.admin.xmover.cli.CrateDBClient", return_value=self.mock_client):
            result = self.runner.invoke(cli, ["problematic-translogs", "--size-mb", "300"])

        assert result.exit_code == 0, result.output
        assert "Found 2 shards with problematic translogs" in result.output
        # Check that the query results table is shown
        assert "Problematic Replica Shards" in result.output
        assert "Generated ALTER Commands:" in result.output
        # Check that key parts of the ALTER commands are present (Rich may wrap lines)
        assert 'ALTER TABLE "TURVO"."shipmentFormFieldData"' in result.output
        assert "REROUTE CANCEL SHARD 14" in result.output
        assert "data-hot-6" in result.output
        assert 'ALTER TABLE "TURVO"."orderFormFieldData"' in result.output
        assert "REROUTE CANCEL SHARD 5" in result.output
        assert "data-hot-1" in result.output
        assert "Total: 2 ALTER commands generated" in result.output

    def test_partitioned_table_command_generation(self):
        """Test ALTER command generation for partitioned tables"""
        mock_rows = [
            ["TURVO", "shipmentFormFieldData_events", '("sync_day"=1757376000000)', 3, "data-hot-2", 481.2],
        ]
        self.mock_client.execute_query.return_value = {"rows": mock_rows}
        self.mock_client.test_connection.return_value = True

        with patch("cratedb_toolkit.admin.xmover.cli.CrateDBClient", return_value=self.mock_client):
            result = self.runner.invoke(cli, ["problematic-translogs", "--size-mb", "400"])

        assert result.exit_code == 0, result.output
        assert "Found 1 shards with problematic translogs" in result.output
        # Check that the query results table is shown
        assert "Problematic Replica Shards" in result.output
        assert "Generated ALTER Commands:" in result.output
        # Check that key parts of the partitioned ALTER command are present
        assert 'ALTER TABLE "TURVO"."shipmentFormFieldData_events"' in result.output
        assert '("sync_day"=1757376000000)' in result.output
        assert "REROUTE CANCEL SHARD 3" in result.output
        assert "data-hot-2" in result.output

    def test_mixed_partitioned_non_partitioned(self):
        """Test handling of both partitioned and non-partitioned tables"""
        mock_rows = [
            ["TURVO", "shipmentFormFieldData", None, 14, "data-hot-6", 7011.8],
            ["TURVO", "shipmentFormFieldData_events", '("sync_day"=1757376000000)', 3, "data-hot-2", 481.2],
            ["TURVO", "orderFormFieldData", "NULL", 5, "data-hot-1", 469.5],
        ]
        self.mock_client.execute_query.return_value = {"rows": mock_rows}
        self.mock_client.test_connection.return_value = True

        with patch("cratedb_toolkit.admin.xmover.cli.CrateDBClient", return_value=self.mock_client):
            result = self.runner.invoke(cli, ["problematic-translogs", "--size-mb", "200"])

        assert result.exit_code == 0, result.output
        assert "Found 3 shards with problematic translogs" in result.output
        # Check that the query results table is shown
        assert "Problematic Replica Shards" in result.output
        assert "Generated ALTER Commands:" in result.output

        # Check non-partitioned command
        assert 'ALTER TABLE "TURVO"."shipmentFormFieldData"' in result.output
        assert "REROUTE CANCEL SHARD 14" in result.output
        assert "data-hot-6" in result.output

        # Check partitioned command
        assert 'ALTER TABLE "TURVO"."shipmentFormFieldData_events"' in result.output
        assert '("sync_day"=1757376000000)' in result.output
        assert "REROUTE CANCEL SHARD 3" in result.output
        assert "data-hot-2" in result.output

        # Check NULL partition handled as non-partitioned
        assert 'ALTER TABLE "TURVO"."orderFormFieldData"' in result.output
        assert "REROUTE CANCEL SHARD 5" in result.output
        assert "data-hot-1" in result.output

    def test_query_parameters(self):
        """Test that the query is called with correct parameters"""
        self.mock_client.execute_query.return_value = {"rows": []}
        self.mock_client.test_connection.return_value = True

        with patch("cratedb_toolkit.admin.xmover.cli.CrateDBClient", return_value=self.mock_client):
            self.runner.invoke(cli, ["problematic-translogs", "--size-mb", "500"])

        # Verify the query was called with the correct threshold
        self.mock_client.execute_query.assert_called_once()
        call_args = self.mock_client.execute_query.call_args
        query = call_args[0][0]
        parameters = call_args[0][1]

        assert "sh.translog_stats['uncommitted_size']" in query
        assert "1024^2" in query
        assert "primary = FALSE" in query
        assert "6 DESC" in query  # More flexible whitespace matching
        assert parameters == [500]

    def test_cancel_flag_user_confirmation_no(self):
        """Test --cancel flag with user declining confirmation"""
        mock_rows = [["TURVO", "shipmentFormFieldData", None, 14, "data-hot-6", 7011.8]]
        self.mock_client.execute_query.return_value = {"rows": mock_rows}
        self.mock_client.test_connection.return_value = True

        with patch("cratedb_toolkit.admin.xmover.cli.CrateDBClient", return_value=self.mock_client), patch(
            "click.confirm", return_value=False
        ):
            result = self.runner.invoke(cli, ["problematic-translogs", "--cancel"])

        assert result.exit_code == 0, result.output
        assert "Operation cancelled by user" in result.output
        # Should only be called once for the initial query, not for execution
        assert self.mock_client.execute_query.call_count == 1

    def test_cancel_flag_user_confirmation_yes(self):
        """Test --cancel flag with user confirming execution"""
        mock_rows = [["TURVO", "shipmentFormFieldData", None, 14, "data-hot-6", 7011.8]]
        self.mock_client.execute_query.return_value = {"rows": mock_rows}
        self.mock_client.test_connection.return_value = True

        with patch("cratedb_toolkit.admin.xmover.cli.CrateDBClient", return_value=self.mock_client), patch(
            "click.confirm", return_value=True
        ), patch("time.sleep"):  # Mock sleep to speed up test
            result = self.runner.invoke(cli, ["problematic-translogs", "--cancel"])

        assert result.exit_code == 0, result.output
        assert "Executing ALTER commands" in result.output
        assert "Command 1 executed successfully" in result.output
        assert "Successful: 1" in result.output

        # Should be called twice: once for query, once for execution
        assert self.mock_client.execute_query.call_count == 2

    def test_execution_failure_handling(self):
        """Test handling of failed command execution"""
        mock_rows = [["TURVO", "shipmentFormFieldData", None, 14, "data-hot-6", 7011.8]]

        # First call returns rows, second call (execution) raises exception
        self.mock_client.execute_query.side_effect = [{"rows": mock_rows}, Exception("Shard not found")]
        self.mock_client.test_connection.return_value = True

        with patch("cratedb_toolkit.admin.xmover.cli.CrateDBClient", return_value=self.mock_client), patch(
            "click.confirm", return_value=True
        ), patch("time.sleep"):
            result = self.runner.invoke(cli, ["problematic-translogs", "--cancel"])

        assert result.exit_code == 0, result.output
        assert "Command 1 failed: Shard not found" in result.output
        assert "Failed: 1" in result.output
        assert "Successful: 0" in result.output

    def test_database_error_handling(self):
        """Test handling of database connection errors"""
        self.mock_client.execute_query.side_effect = Exception("Connection failed")
        self.mock_client.test_connection.return_value = True

        with patch("cratedb_toolkit.admin.xmover.cli.CrateDBClient", return_value=self.mock_client):
            result = self.runner.invoke(cli, ["problematic-translogs"])

        assert result.exit_code == 0, result.output
        assert "Error analyzing problematic translogs" in result.output
        assert "Connection failed" in result.output

    def test_default_size_mb(self):
        """Test that default sizeMB is 300"""
        self.mock_client.execute_query.return_value = {"rows": []}
        self.mock_client.test_connection.return_value = True

        with patch("cratedb_toolkit.admin.xmover.cli.CrateDBClient", return_value=self.mock_client):
            result = self.runner.invoke(cli, ["problematic-translogs"])

        assert result.exit_code == 0, result.output
        assert "300MB" in result.output

        # Verify query was called with default value
        call_args = self.mock_client.execute_query.call_args
        parameters = call_args[0][1]
        assert parameters == [300]
