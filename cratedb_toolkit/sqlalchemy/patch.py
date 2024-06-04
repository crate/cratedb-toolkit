import calendar
import datetime as dt
import json
import typing as t
from decimal import Decimal
from uuid import UUID

import sqlalchemy as sa

try:
    import numpy as np

    has_numpy = True
except ImportError:
    has_numpy = False


def patch_inspector():
    """
    When using `get_table_names()`, make sure the correct schema name gets used.

    Apparently, SQLAlchemy does not honor the `search_path` of the engine, when
    using the inspector?

    FIXME: Bug in CrateDB SQLAlchemy dialect?
    """

    def get_effective_schema(engine: sa.Engine):
        schema_name_raw = engine.url.query.get("schema")
        schema_name = None
        if isinstance(schema_name_raw, str):
            schema_name = schema_name_raw
        elif isinstance(schema_name_raw, tuple):
            schema_name = schema_name_raw[0]
        return schema_name

    try:
        from sqlalchemy_cratedb import dialect
    except ImportError:  # pragma: nocover
        from crate.client.sqlalchemy.dialect import CrateDialect as dialect

    get_table_names_dist = dialect.get_table_names

    def get_table_names(self, connection: sa.Connection, schema: t.Optional[str] = None, **kw: t.Any) -> t.List[str]:
        if schema is None:
            schema = get_effective_schema(connection.engine)
        return get_table_names_dist(self, connection=connection, schema=schema, **kw)

    dialect.get_table_names = get_table_names  # type: ignore


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
