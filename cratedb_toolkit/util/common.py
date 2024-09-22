# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import os

import colorlog
from colorlog.escape_codes import escape_codes

from cratedb_toolkit.util.data import asbool


def setup_logging(level=logging.INFO, verbose: bool = False, debug: bool = False, width: int = 36):
    reset = escape_codes["reset"]
    log_format = f"%(asctime)-15s [%(name)-{width}s] %(log_color)s%(levelname)-8s:{reset} %(message)s"

    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(log_format))

    logging.basicConfig(format=log_format, level=level, handlers=[handler])

    logging.getLogger("crate.client").setLevel(level)
    logging.getLogger("sqlalchemy_cratedb").setLevel(level)
    logging.getLogger("urllib3.connectionpool").setLevel(level)

    # Enable SQLAlchemy logging.
    if verbose:
        logging.getLogger("cratedb_toolkit").setLevel(logging.DEBUG)

    if debug:
        # Optionally tame SQLAlchemy and PyMongo.
        if asbool(os.environ.get("DEBUG_SQLALCHEMY")):
            logging.getLogger("sqlalchemy").setLevel(level)
        else:
            logging.getLogger("sqlalchemy").setLevel(logging.INFO)
        if asbool(os.environ.get("DEBUG_PYMONGO")):
            logging.getLogger("pymongo").setLevel(level)
        else:
            logging.getLogger("pymongo").setLevel(logging.INFO)

    # logging.getLogger("docker.auth").setLevel(logging.INFO)  # noqa: ERA001

    # Tame Faker spamming the logs.
    # https://github.com/joke2k/faker/issues/753#issuecomment-491402018
    logging.getLogger("faker").setLevel(logging.ERROR)
