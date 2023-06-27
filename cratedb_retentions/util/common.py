# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.

import logging

import colorlog
from colorlog.escape_codes import escape_codes


def setup_logging(level=logging.INFO):
    reset = escape_codes["reset"]
    log_format = f"%(asctime)-15s [%(name)-28s] %(log_color)s%(levelname)-8s:{reset} %(message)s"

    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(log_format))

    logging.basicConfig(format=log_format, level=level, handlers=[handler])
