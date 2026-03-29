import asyncio
import logging
import re
import typing as t

import sqlalchemy as sa
from kinesis.checkpointers import BaseCheckPointer

# SQL identifier pattern: prevents injection via dynamic DDL/DML.
# Hyphens are allowed because all identifiers are double-quoted in generated SQL.
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")

logger = logging.getLogger(__name__)


class CrateDBCheckPointer(BaseCheckPointer):
    """
    Kinesis checkpointer that stores shard progress in a CrateDB table.

    Designed for single-consumer use: ``allocate()`` always claims the shard
    (stale ``active=True`` from a crashed consumer is safe to reclaim).

    Table layout::

        <schema>.kinesis_checkpoints
          namespace   TEXT
          shard_id    TEXT
          sequence    TEXT
          active      BOOLEAN
          updated_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
          PRIMARY KEY (namespace, shard_id)

    All sync CrateDB driver calls are wrapped with ``run_in_executor()``
    to avoid blocking the consumer's event loop.
    """

    def __init__(
        self,
        engine: sa.Engine,
        name: str,
        schema: str = "ext",
        consumer_id: t.Union[str, int, None] = None,
    ):
        super().__init__(name=name, id=consumer_id)
        _validate_identifier(schema)
        self.engine = engine
        self.schema = schema
        self.table_name = f'"{schema}"."kinesis_checkpoints"'
        self._owned_shards: t.Set[str] = set()
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the checkpoint table if it does not already exist."""
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                "namespace" TEXT NOT NULL,
                "shard_id"  TEXT NOT NULL,
                "sequence"  TEXT,
                "active"    BOOLEAN DEFAULT TRUE,
                "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY ("namespace", "shard_id")
            )
        """
        with self.engine.connect() as conn:
            conn.execute(sa.text(ddl))
            conn.commit()

    async def allocate(self, shard_id: str) -> t.Tuple[bool, t.Optional[str]]:
        """
        Allocate a shard for processing.

        Single-consumer model: always claims the shard. If a row exists with
        ``active=True``, the previous owner crashed and it is safe to reclaim.
        """

        def _allocate() -> t.Tuple[bool, t.Optional[str]]:
            with self.engine.connect() as conn:
                # Check for existing checkpoint.
                row = conn.execute(
                    sa.text(
                        f'SELECT "sequence" FROM {self.table_name} '  # noqa: S608 -- table_name is built from validated identifiers
                        f'WHERE "namespace" = :ns AND "shard_id" = :sid'
                    ),
                    {"ns": self._name, "sid": shard_id},
                ).fetchone()

                if row is not None:
                    sequence = row[0]
                    conn.execute(
                        sa.text(
                            f"UPDATE {self.table_name} "  # noqa: S608 -- table_name is built from validated identifiers
                            f'SET "active" = TRUE, "updated_at" = CURRENT_TIMESTAMP '
                            f'WHERE "namespace" = :ns AND "shard_id" = :sid'
                        ),
                        {"ns": self._name, "sid": shard_id},
                    )
                    conn.commit()
                    conn.execute(sa.text(f"REFRESH TABLE {self.table_name}"))
                    logger.info(f"{self.get_ref()} allocated {shard_id} (resuming from sequence {sequence})")
                    return True, sequence

                # Fresh allocation: no prior state.
                conn.execute(
                    sa.text(
                        f"INSERT INTO {self.table_name} "  # noqa: S608 -- table_name is built from validated identifiers
                        f'("namespace", "shard_id", "sequence", "active") '
                        f"VALUES (:ns, :sid, NULL, TRUE)"
                    ),
                    {"ns": self._name, "sid": shard_id},
                )
                conn.commit()
                conn.execute(sa.text(f"REFRESH TABLE {self.table_name}"))
                logger.info(f"{self.get_ref()} allocated {shard_id} (fresh start)")
                return True, None

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, _allocate)
        self._items[shard_id] = result[1]
        self._owned_shards.add(shard_id)
        return result

    async def checkpoint(self, shard_id: str, sequence: str) -> None:
        """
        Record processing progress for a shard.

        Enforces monotonic ordering: ignores a sequence older than the current
        value (numeric comparison). The guard uses the in-memory ``_items`` dict
        rather than a DB read, avoiding CrateDB eventual-consistency issues.
        """
        current = self._items.get(shard_id)
        if current is not None and int(current) >= int(sequence):
            logger.debug(
                f"{self.get_ref()} skipping checkpoint {shard_id}@{sequence} (current={current} is not behind)"
            )
            return

        def _checkpoint() -> None:
            with self.engine.connect() as conn:
                conn.execute(
                    sa.text(
                        f"UPDATE {self.table_name} "  # noqa: S608 -- table_name is built from validated identifiers
                        f'SET "sequence" = :seq, "updated_at" = CURRENT_TIMESTAMP '
                        f'WHERE "namespace" = :ns AND "shard_id" = :sid'
                    ),
                    {"ns": self._name, "sid": shard_id, "seq": sequence},
                )
                conn.commit()
                conn.execute(sa.text(f"REFRESH TABLE {self.table_name}"))

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _checkpoint)
        self._items[shard_id] = sequence
        logger.debug(f"{self.get_ref()} checkpointed {shard_id}@{sequence}")

    async def deallocate(self, shard_id: str) -> None:
        """
        Release a shard, preserving the last checkpoint for future consumers.
        """

        def _deallocate() -> None:
            with self.engine.connect() as conn:
                conn.execute(
                    sa.text(
                        f"UPDATE {self.table_name} "  # noqa: S608 -- table_name is built from validated identifiers
                        f'SET "active" = FALSE, "updated_at" = CURRENT_TIMESTAMP '
                        f'WHERE "namespace" = :ns AND "shard_id" = :sid'
                    ),
                    {"ns": self._name, "sid": shard_id},
                )
                conn.commit()
                conn.execute(sa.text(f"REFRESH TABLE {self.table_name}"))

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _deallocate)
        final_seq = self._items.pop(shard_id, None)
        logger.info(f"{self.get_ref()} deallocated {shard_id}@{final_seq}")
        self._owned_shards.discard(shard_id)

    def get_all_checkpoints(self) -> t.Dict[str, str]:
        """
        Return all checkpoints for this namespace.

        Uses ``REFRESH TABLE`` to force a Lucene segment flush before reading,
        ensuring consistent results. Avoid calling this in the hot path.
        """
        with self.engine.connect() as conn:
            conn.execute(sa.text(f"REFRESH TABLE {self.table_name}"))
            rows = conn.execute(
                sa.text(
                    f'SELECT "shard_id", "sequence" FROM {self.table_name} '  # noqa: S608 -- table_name is built from validated identifiers
                    f'WHERE "namespace" = :ns AND "sequence" IS NOT NULL'
                ),
                {"ns": self._name},
            ).fetchall()
            conn.commit()
        return {row[0]: row[1] for row in rows}

    async def close(self) -> None:
        """
        Deallocate all owned shards and clean up.
        """
        logger.info(f"{self.get_ref()} closing")
        for shard_id in list(self._owned_shards):
            await self.deallocate(shard_id)


def _validate_identifier(name: str) -> None:
    """Reject identifiers that could cause SQL injection."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
