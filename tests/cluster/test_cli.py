import os

import pytest
from click.testing import CliRunner

from cratedb_toolkit.cluster.cli import cli
from cratedb_toolkit.exception import CroudException


def test_cloud_cluster_info_unknown():
    """
    Verify `ctk cluster info --cluster-name=unknown` raises an exception.
    """

    settings = {
        "CRATEDB_CLOUD_API_KEY": os.environ.get("TEST_CRATEDB_CLOUD_API_KEY"),
        "CRATEDB_CLOUD_API_SECRET": os.environ.get("TEST_CRATEDB_CLOUD_API_SECRET"),
        "CRATEDB_CLOUD_ORGANIZATION_ID": os.environ.get("TEST_CRATEDB_CLOUD_ORGANIZATION_ID"),
    }

    if any(setting is None for setting in settings.values()):
        raise pytest.skip("Missing environment variables for headless mode with croud")

    # Synthesize a valid environment.
    runner = CliRunner(env=settings)

    # Execute the command.
    with pytest.raises(CroudException) as ex:
        runner.invoke(
            cli,
            args="info --cluster-name=unknown",
            catch_exceptions=False,
        )
    assert ex.match("Cluster not found: unknown")
