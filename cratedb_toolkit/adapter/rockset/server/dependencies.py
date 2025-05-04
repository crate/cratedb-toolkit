# Copyright (c) 2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import os
from functools import lru_cache

from cratedb_toolkit.util.database import DatabaseAdapter


@lru_cache
def database_adapter() -> DatabaseAdapter:
    # TODO: return config.Settings()
    cluster_url = os.environ["CRATEDB_CLUSTER_URL"]
    return DatabaseAdapter(dburi=cluster_url)
