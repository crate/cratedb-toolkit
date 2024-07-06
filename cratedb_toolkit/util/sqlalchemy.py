"""
Patches and polyfills, mostly for SQLAlchemy.

TODO: Refactor to `crate` or `sqlalchemy-cratedb` packages.
"""

import calendar
import datetime as dt
import json
from decimal import Decimal
from uuid import UUID

from sqlalchemy_cratedb.dialect import TYPES_MAP

try:
    import numpy as np

    has_numpy = True
except ImportError:
    has_numpy = False

from sqlalchemy import types as sqltypes


def patch_encoder():
    import crate.client.http

    crate.client.http.CrateJsonEncoder = CrateJsonEncoderWithNumPy


class CrateJsonEncoderWithNumPy(json.JSONEncoder):
    epoch_aware = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    epoch_naive = dt.datetime(1970, 1, 1)

    def default(self, o):
        # Vanilla CrateDB Python.
        if isinstance(o, (Decimal, UUID)):
            return str(o)
        if isinstance(o, dt.datetime):
            if o.tzinfo is not None:
                delta = o - self.epoch_aware
            else:
                delta = o - self.epoch_naive
            return int(delta.microseconds / 1000.0 + (delta.seconds + delta.days * 24 * 3600) * 1000.0)
        if isinstance(o, dt.date):
            return calendar.timegm(o.timetuple()) * 1000

        # NumPy ndarray and friends.
        # https://stackoverflow.com/a/49677241
        if has_numpy:
            if isinstance(o, np.integer):
                return int(o)
            elif isinstance(o, np.floating):
                return float(o)
            elif isinstance(o, np.ndarray):
                return o.tolist()
        return json.JSONEncoder.default(self, o)


def patch_types_map():
    """
    Register missing timestamp data type.
    """
    # TODO: Submit patch to `crate-python`.
    TYPES_MAP["timestamp without time zone"] = sqltypes.TIMESTAMP
