# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import subprocess
from pathlib import Path

import pytest
import responses
from pytest_notebook.nb_regression import NBRegressionFixture

import cratedb_toolkit

ROOT = Path(__file__).parent.parent.parent


@responses.activate
def test_example_cloud_cluster_exists_python(mocker, mock_cloud_cluster_exists):
    """
    Verify that the program `examples/cloud_cluster.py` works.
    In this case, there is a mock which pretends the cluster already exists.
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
    from examples.python.cloud_cluster import main

    main()


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


@responses.activate
def test_example_cloud_cluster_with_deploy(mocker, mock_cloud_cluster_deploy):
    """
    Verify that the program `examples/cloud_cluster.py` works.
    In this case, mocking-wise, there is no cluster, but the test exercises a full cluster deployment.
    """

    cratedb_toolkit.configure(
        settings_accept_env=True,
    )

    mocker.patch.dict(
        "os.environ",
        {
            "CRATEDB_CLOUD_ORGANIZATION_ID": "4148156d-b842-4a86-8024-ecb380be1fc2",
            "CRATEDB_CLOUD_SUBSCRIPTION_ID": "f33a2f55-17d1-4f21-8130-b6595d7c52db",
            "CRATEDB_CLOUD_CLUSTER_NAME": "testcluster",
            "CRATEDB_USERNAME": "crate",
        },
    )
    from examples.python.cloud_cluster import main

    main()


@responses.activate
def test_example_cloud_import_python(mocker, mock_cloud_import):
    """
    Verify that the program `examples/cloud_import.py` works.
    """

    cratedb_toolkit.configure(
        settings_accept_env=True,
    )

    mocker.patch.dict(
        "os.environ",
        {
            "CRATEDB_CLOUD_CLUSTER_ID": "e1e38d92-a650-48f1-8a70-8133f2d5c400",
        },
    )
    from examples.python.cloud_import import main

    main()


@pytest.mark.skip(
    "Does not work: Apparently, the 'responses' mockery is not properly activated when evaluating the notebook"
)
@responses.activate
def test_example_cloud_import_notebook(mocker, mock_cloud_cluster_exists):
    """
    Verify the Jupyter Notebook example works.
    """

    # Synthesize a valid environment.
    mocker.patch.dict(
        "os.environ",
        {
            "CRATEDB_CLOUD_SUBSCRIPTION_ID": "f33a2f55-17d1-4f21-8130-b6595d7c52db",
            # "CRATEDB_CLOUD_CLUSTER_ID": "e1e38d92-a650-48f1-8a70-8133f2d5c400",  # noqa: ERA001
            "CRATEDB_CLOUD_CLUSTER_NAME": "testcluster",
            "CRATEDB_USERNAME": "crate",
        },
    )

    # Exercise Notebook.
    fixture = NBRegressionFixture(
        diff_ignore=("/metadata/language_info", "/metadata/widgets", "/cells/*/execution_count"),
    )
    notebook = ROOT / "examples" / "notebook" / "cloud_import.ipynb"
    fixture.check(str(notebook))
