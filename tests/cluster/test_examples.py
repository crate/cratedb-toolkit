# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
from pathlib import Path

import pytest
import responses
from pytest_notebook.nb_regression import NBRegressionFixture

import cratedb_toolkit


@responses.activate
def test_example_cloud_cluster_exists(mocker, mock_cloud_cluster_exists):
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
    from examples.cloud_cluster import main

    main()


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
    from examples.cloud_cluster import main

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
    from examples.cloud_import import main

    main()


@pytest.mark.skip(
    "Does not work: Apparently, the 'responses' mockery " "is not properly activated when evaluating the notebook"
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
    here = Path(__file__).parent.parent.parent
    notebook = here / "examples" / "cloud_import.ipynb"
    fixture.check(str(notebook))
