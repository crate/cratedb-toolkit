# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.


def test_example_edit(store):
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
