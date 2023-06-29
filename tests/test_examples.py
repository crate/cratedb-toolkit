# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.


def test_example_basic(cratedb):
    """
    Verify that the program `examples/basic.py` works.
    """
    from examples.basic import main

    main(dburi=cratedb.get_connection_url())
