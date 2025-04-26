# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import os
from pathlib import Path

import pytest
from testbook import testbook

import cratedb_toolkit

pytestmark = pytest.mark.python


ROOT = Path(__file__).parent.parent.parent


@pytest.fixture()
def real_settings():
    settings = {
        "CRATEDB_CLOUD_API_KEY": os.environ.get("TEST_CRATEDB_CLOUD_API_KEY"),
        "CRATEDB_CLOUD_API_SECRET": os.environ.get("TEST_CRATEDB_CLOUD_API_SECRET"),
        "CRATEDB_CLOUD_ORGANIZATION_ID": os.environ.get("TEST_CRATEDB_CLOUD_ORGANIZATION_ID"),
        "CRATEDB_CLOUD_CLUSTER_NAME": os.environ.get("TEST_CRATEDB_CLOUD_CLUSTER_NAME", "testcluster"),
        "CRATEDB_USERNAME": os.environ.get("TEST_CRATEDB_USERNAME"),
        "CRATEDB_PASSWORD": os.environ.get("TEST_CRATEDB_PASSWORD"),
    }

    if any(setting is None for setting in settings.values()):
        raise pytest.skip("Missing environment variables for headless mode with croud")

    return settings


def test_example_cloud_cluster_app(mocker, real_settings):
    """
    Verify that the program `examples/python/cloud_cluster.py` works.
    """

    mocker.patch.dict("os.environ", real_settings)

    cratedb_toolkit.configure(
        settings_accept_env=True,
    )

    from examples.python.cloud_cluster import main

    main()


def test_example_cloud_import_app(mocker, real_settings):
    """
    Verify that the program `examples/python/cloud_import.py` works.
    """

    mocker.patch.dict("os.environ", real_settings)

    cratedb_toolkit.configure(
        settings_accept_env=True,
    )

    from examples.python.cloud_import import main

    main()


def test_example_cloud_import_notebook(mocker, real_settings):
    """
    Verify the Jupyter notebook `examples/notebook/cloud_import.py` works.
    """

    # Synthesize a valid environment.
    mocker.patch.dict("os.environ", real_settings)

    # Execute the notebook.
    notebook = Path("examples") / "notebook" / "cloud_import.ipynb"
    with testbook(notebook, timeout=180) as tb:
        tb.execute()
