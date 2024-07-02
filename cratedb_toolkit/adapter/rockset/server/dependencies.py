# Copyright (c) 2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import os
from functools import lru_cache

from cratedb_toolkit.util import DatabaseAdapter


@lru_cache
def database_adapter() -> DatabaseAdapter:
    # TODO: return config.Settings()
    cratedb_sqlalchemy_url = os.environ["CRATEDB_SQLALCHEMY_URL"]
    return DatabaseAdapter(dburi=cratedb_sqlalchemy_url)
