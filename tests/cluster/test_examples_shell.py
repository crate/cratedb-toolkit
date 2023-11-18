import subprocess
from pathlib import Path

import pytest

import cratedb_toolkit

ROOT = Path(__file__).parent.parent.parent

pytestmark = pytest.mark.shell


def test_example_cloud_cluster_exists_shell(mocker, mock_cloud_cluster_exists, capfd):
    """
    Verify that the program `examples/shell/cloud_cluster.sh` works.
    In this case, the `ctk` command is mocked completely, so this is nothing serious.
    """

    cratedb_toolkit.configure(
        settings_accept_env=True,
    )

    mocker.patch.dict(
        "os.environ",
        {
            "CRATEDB_CLOUD_SUBSCRIPTION_ID": "f33a2f55-17d1-4f21-8130-b6595d7c52db",
            "CRATEDB_CLOUD_CLUSTER_NAME": "testcluster",
            "CRATEDB_USERNAME": "crate",
        },
    )

    script = ROOT / "tests" / "cluster" / "test_examples.sh"
    subprocess.check_call(["bash", script])  # noqa: S603, S607

    out, err = capfd.readouterr()

    assert "cluster start" in out
    assert "shell --command" in out

    """
    assert "Mont Blanc massif" in out
    assert "Monte Rosa Alps" in out
    assert "Deploying/starting/resuming CrateDB Cloud Cluster: id=None, name=plotz" in err
    assert "Cluster information: name=plotz, url=https://plotz.aks1.westeurope.azure.cratedb.net:4200" in err
    assert "Successfully acquired cluster" in err
    assert "CONNECT OK" in err
    assert "SELECT 2 rows in set (0.000 sec)" in err
    """
