"""
Patches and polyfills, mostly for SQLAlchemy.

TODO: Refactor to `crate` or `sqlalchemy-cratedb` packages.
"""

from sqlalchemy import types as sqltypes
from sqlalchemy_cratedb.dialect import TYPES_MAP


def patch_types_map():
    """
    Register missing timestamp data type.
    """
    # TODO: Submit patch to `crate-python`.
    TYPES_MAP["timestamp without time zone"] = sqltypes.TIMESTAMP
