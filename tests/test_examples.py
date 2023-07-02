# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.


def test_example_basic(store):
    """
    Verify that the program `examples/basic.py` works.
    """
    from examples.basic import main

    main(dburi=store.database.dburi)


def test_example_edit(store):
    """
    Verify that the program `examples/edit.py` works.
    """
    from examples.edit import main

    main(dburi=store.database.dburi)
