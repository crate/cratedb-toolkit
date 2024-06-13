# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import os
from functools import lru_cache

import typing_extensions as t
from fastapi import Depends, FastAPI, HTTPException

from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.util.service import start_service
from cratedb_toolkit.wtf.core import InfoContainer
from cratedb_toolkit.wtf.util import get_baseinfo

logger = logging.getLogger(__name__)

app = FastAPI()


@lru_cache
def database_adapter() -> DatabaseAdapter:
    # TODO: return config.Settings()
    cratedb_sqlalchemy_url = os.environ["CRATEDB_SQLALCHEMY_URL"]
    return DatabaseAdapter(dburi=cratedb_sqlalchemy_url)


@app.get("/")
def read_root():
    return get_baseinfo()


@app.get("/info/{category}")
def info(category: str, adapter: t.Annotated[DatabaseAdapter, Depends(database_adapter)], scrub: bool = False):  # type: ignore[name-defined]
    if category != "all":
        raise HTTPException(status_code=404, detail="Info category not found")
    sample = InfoContainer(adapter=adapter, scrub=scrub)
    return sample.to_dict()


def start(listen_address: t.Union[str, None] = None, reload: bool = False):  # pragma: no cover
    start_service(app="cratedb_toolkit.wtf.http:app", listen_address=listen_address, reload=reload)
