# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import typing as t

from cratedb_toolkit.util.common import setup_logging

logger = logging.getLogger(__name__)


def start_service(app: str, listen_address: t.Union[str, None] = None, reload: bool = False):  # pragma: no cover
    setup_logging()
    from uvicorn import run

    if listen_address is None:
        listen_address = "127.0.0.1:4242"

    host, _, port = listen_address.partition(":")
    port = port or "0"
    port_int = int(port)

    logger.info(f"Starting HTTP web service on http://{listen_address}")

    run(app=app, host=host, port=port_int, reload=reload)
