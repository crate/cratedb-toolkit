# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
from unittest.mock import patch

import responses


def test_example_edit(store, needs_sqlalchemy2):
    """
    Verify that the program `examples/retention_edit.py` works.
    """
    from examples.retention_edit import main

    main(dburi=store.database.dburi)


def test_example_retire_cutoff(store):
    """
    Verify that the program `examples/retention_retire_cutoff.py` works.
    """
    from examples.retention_retire_cutoff import main

    main(dburi=store.database.dburi)


@responses.activate
def test_example_cloud_import(store, cloud_cluster_mock):
    """
    Verify that the program `examples/cloud_import.py` works.
    """

    from examples.cloud_import import main

    cluster_id = "e1e38d92-a650-48f1-8a70-8133f2d5c400"
    with patch("examples.cloud_import.obtain_cluster_id", return_value=cluster_id):
        main()
