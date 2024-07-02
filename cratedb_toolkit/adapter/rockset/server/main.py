# Copyright (c) 2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import typing as t

from fastapi import FastAPI

from cratedb_toolkit.util.service import start_service
from cratedb_toolkit.wtf.util import get_baseinfo

from .api import document, query

logger = logging.getLogger(__name__)

app = FastAPI()
app.include_router(document.router)
app.include_router(query.router)


@app.get("/")
def read_root():
    return get_baseinfo()


def start(listen_address: t.Union[str, None] = None, reload: bool = False):  # pragma: no cover
    if listen_address is None:
        listen_address = "127.0.0.1:4243"
    start_service(app="cratedb_toolkit.adapter.rockset.server.main:app", listen_address=listen_address, reload=reload)
