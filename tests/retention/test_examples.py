# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import responses

import cratedb_toolkit


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
def test_example_cloud_import(mocker, store, cloud_cluster_mock):
    """
    Verify that the program `examples/cloud_import.py` works.
    """

    cratedb_toolkit.configure(
        settings_accept_cli=True,
        settings_accept_env=True,
    )

    cluster_id = "e1e38d92-a650-48f1-8a70-8133f2d5c400"
    mocker.patch.dict("os.environ", {"CRATEDB_CLOUD_CLUSTER_ID": cluster_id})
    from examples.cloud_import import main

    main()
