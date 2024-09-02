import pytest
from click.testing import CliRunner

from cratedb_toolkit.cli import cli

pytestmark = pytest.mark.dynamodb


def test_dynamodb_load_table(caplog, cratedb, dynamodb, dynamodb_test_manager):
    """
    CLI test: Invoke `ctk load table` for DynamoDB.
    """
    dynamodb_url = f"{dynamodb.get_connection_url()}/ProductCatalog?region=us-east-1"
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"

    # Populate source database with sample dataset.
    dynamodb_test_manager.load_product_catalog()

    # Run transfer command.
    runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb_url})
    result = runner.invoke(
        cli,
        args=f"load table {dynamodb_url}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 8
