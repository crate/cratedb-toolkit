# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging

import colorlog
from colorlog.escape_codes import escape_codes


def setup_logging(level=logging.INFO, verbose: bool = False, width: int = 36):
    reset = escape_codes["reset"]
    log_format = f"%(asctime)-15s [%(name)-{width}s] %(log_color)s%(levelname)-8s:{reset} %(message)s"

    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(log_format))

    logging.basicConfig(format=log_format, level=level, handlers=[handler])

    # Enable SQLAlchemy logging.
    if verbose:
        logging.getLogger("sqlalchemy").setLevel(level)

    logging.getLogger("crate.client").setLevel(level)
    logging.getLogger("sqlalchemy_cratedb").setLevel(level)
    logging.getLogger("urllib3.connectionpool").setLevel(level)

    # logging.getLogger("docker.auth").setLevel(logging.INFO)  # noqa: ERA001

    # Tame Faker spamming the logs.
    # https://github.com/joke2k/faker/issues/753#issuecomment-491402018
    logging.getLogger("faker").setLevel(logging.ERROR)
