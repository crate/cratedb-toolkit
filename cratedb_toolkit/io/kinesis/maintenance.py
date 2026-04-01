# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
Standalone maintenance operations for the Kinesis checkpoint table.

Decoupled from ``CrateDBCheckPointer`` so it can be imported without
the ``kinesis`` (async-kinesis) dependency.
"""

import logging
import re
import typing as t
from datetime import datetime, timedelta, timezone

import sqlalchemy as sa

from cratedb_toolkit.exception import CheckpointTableNotFound

logger = logging.getLogger(__name__)

# SQL identifier pattern: prevents injection via dynamic DDL/DML.
# Hyphens are allowed because all identifiers are double-quoted in generated SQL.
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")

# Duration pattern: e.g. "7d", "24h", "30m", "90s", "2w", or combinations like "1d12h".
_DURATION_RE = re.compile(r"^(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")

TABLE_NAME = "kinesis_checkpoints"


def _validate_identifier(value: str) -> None:
    """Validate a SQL identifier to prevent injection."""
    if not value or not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")


def _qualified_table(schema: str) -> str:
    """Return a fully-qualified, double-quoted table reference."""
    _validate_identifier(schema)
    return f'"{schema}"."{TABLE_NAME}"'


def _check_table_exists(conn: sa.Connection, schema: str) -> None:
    """Raise ``CheckpointTableNotFound`` if the checkpoint table does not exist."""
    result = conn.execute(
        sa.text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table"),
        {"schema": schema, "table": TABLE_NAME},
    ).scalar()
    if not result:
        raise CheckpointTableNotFound(f"Checkpoint table {_qualified_table(schema)} does not exist")


def parse_duration(value: str) -> timedelta:
    """
    Parse a human-friendly duration string into a ``timedelta``.

    Accepted formats: ``2w``, ``7d``, ``24h``, ``30m``, ``90s``, or
    combinations like ``1d12h``. At least one component is required
    and the total duration must be positive.

    Raises ``ValueError`` for invalid or zero-length durations.
    """
    value = value.strip()
    if not value:
        raise ValueError(
            "Invalid duration: ''. Use a combination of Nw, Nd, Nh, Nm, Ns (e.g. '7d', '24h', '2w', '1d12h')."
        )
    match = _DURATION_RE.match(value)
    if not match:
        raise ValueError(
            f"Invalid duration: {value!r}. Use a combination of Nw, Nd, Nh, Nm, Ns (e.g. '7d', '24h', '2w', '1d12h')."
        )

    weeks = int(match.group(1) or 0)
    days = int(match.group(2) or 0)
    hours = int(match.group(3) or 0)
    minutes = int(match.group(4) or 0)
    seconds = int(match.group(5) or 0)

    td = timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
    if td.total_seconds() <= 0:
        raise ValueError(f"Duration must be positive, got: {value!r}")
    return td


def list_checkpoints(
    engine: sa.Engine,
    schema: str = "ext",
    namespace: t.Optional[str] = None,
) -> t.List[t.Dict[str, t.Any]]:
    """
    List checkpoint records, optionally filtered by namespace.

    Returns a list of dicts with keys: ``namespace``, ``shard_id``,
    ``sequence``, ``active``, ``updated_at``.
    """
    table = _qualified_table(schema)
    with engine.connect() as conn:
        _check_table_exists(conn, schema)
        conn.execute(sa.text(f"REFRESH TABLE {table}"))

        where = ' WHERE "namespace" = :ns' if namespace else ""
        params = {"ns": namespace} if namespace else {}
        sql = sa.text(
            f'SELECT "namespace", "shard_id", "sequence", "active", "updated_at" '  # noqa: S608
            f"FROM {table}{where} "
            f'ORDER BY "namespace", "shard_id"'
        )
        rows = conn.execute(sql, params).fetchall()

    return [dict(row._mapping) for row in rows]


def prune_checkpoints(
    engine: sa.Engine,
    schema: str = "ext",
    older_than: t.Optional[str] = None,
    namespace: t.Optional[str] = None,
    include_active: bool = False,
    dry_run: bool = False,
) -> t.Dict[str, t.Any]:
    """
    Delete checkpoint rows matching the given filters.

    By default only rows with ``active = FALSE`` are eligible. Pass
    ``include_active=True`` to also delete active rows (use with care,
    stop consumers first).

    At least one of ``older_than`` or ``namespace`` is required.

    Returns a dict with ``deleted`` count (or ``would_delete`` in dry-run
    mode) and the ``cutoff`` timestamp used (if applicable).
    """
    if not older_than and not namespace:
        raise ValueError("At least one of older_than or namespace is required")

    table = _qualified_table(schema)
    conditions: t.List[str] = []
    params: t.Dict[str, t.Any] = {}
    result_meta: t.Dict[str, t.Any] = {}

    if not include_active:
        conditions.append('"active" = FALSE')

    if older_than:
        age = parse_duration(older_than)
        cutoff = datetime.now(tz=timezone.utc) - age
        conditions.append('"updated_at" < :cutoff')
        params["cutoff"] = cutoff
        result_meta["cutoff"] = cutoff.isoformat()

    if namespace:
        conditions.append('"namespace" = :ns')
        params["ns"] = namespace

    where = " AND ".join(conditions)

    with engine.connect() as conn:
        _check_table_exists(conn, schema)
        conn.execute(sa.text(f"REFRESH TABLE {table}"))

        if dry_run:
            count_sql = sa.text(f"SELECT COUNT(*) FROM {table} WHERE {where}")  # noqa: S608
            count = conn.execute(count_sql, params).scalar()
            return {"would_delete": count, **result_meta}

        delete_sql = sa.text(f"DELETE FROM {table} WHERE {where}")  # noqa: S608
        result = conn.execute(delete_sql, params)
        conn.commit()
        return {"deleted": result.rowcount, **result_meta}
