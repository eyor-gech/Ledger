"""
ledger/event_store.py — PostgreSQL-backed EventStore
=====================================================
COMPLETION CHECKLIST (implement in order):
  [ ] Phase 1, Day 1: append() + stream_version()
  [ ] Phase 1, Day 1: load_stream()
  [ ] Phase 1, Day 2: load_all()  (needed for projection daemon)
  [ ] Phase 1, Day 2: get_event() (needed for causation chain)
  [ ] Phase 4:        UpcasterRegistry.upcast() integration in load_stream/load_all
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator
from uuid import UUID, uuid4

import asyncpg

from ledger.schema.events import StreamMetadata, EVENT_REGISTRY
from ledger.domain.errors import DomainSemanticError

# ─────────────────────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────────────────────

class OptimisticConcurrencyError(Exception):
    """Raised when expected_version doesn't match current stream version."""

    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"OCC on '{stream_id}': expected v{expected}, actual v{actual}"
        )


class StreamArchivedError(Exception):
    """Raised when attempting to append to an archived stream."""

    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(f"Stream archived: {stream_id}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_json(obj: Any) -> str:
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def _compute_event_hash(
    *,
    stream_id: str,
    stream_position: int,
    event_type: str,
    event_version: int,
    payload: dict[str, Any],
    metadata: dict[str, Any],
    recorded_at: datetime,
    prev_hash: bytes | None,
) -> bytes:
    body = {
        "stream_id": stream_id,
        "stream_position": stream_position,
        "event_type": event_type,
        "event_version": event_version,
        "payload": payload,
        "metadata": metadata,
        "recorded_at": recorded_at.isoformat(),
        "prev_hash": prev_hash.hex() if prev_hash else None,
    }
    return hashlib.sha256(_canonical_json(body).encode("utf-8")).digest()


# ─────────────────────────────────────────────────────────────────────────────
# EventStore
# ─────────────────────────────────────────────────────────────────────────────

class EventStore:
    """PostgreSQL-backed append-only event store."""

    def __init__(self, db_url: str, upcaster_registry=None):
        self.db_url = db_url
        self.upcasters = upcaster_registry
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            self.db_url, min_size=2, max_size=10
        )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    # ─────────────────────────────────────────────────────────────────────────
    # Stream Version
    # ─────────────────────────────────────────────────────────────────────────

    async def stream_version(self, stream_id: str) -> int:
        if self._pool is None:
            raise RuntimeError("EventStore not connected")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id=$1",
                stream_id,
            )

        return int(row["current_version"]) if row else -1

    # ─────────────────────────────────────────────────────────────────────────
    # Append (CORE)
    # ─────────────────────────────────────────────────────────────────────────

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:

        if self._pool is None:
            raise RuntimeError("EventStore not connected")

        if not events:
            return []
        for event in events:
            et = event.get("event_type")

            if not et:
                raise DomainSemanticError(event_type="MISSING")

        aggregate_type = stream_id.split("-", 1)[0]

        meta: dict[str, Any] = dict(metadata or {})
        if correlation_id:
            meta["correlation_id"] = correlation_id
        if causation_id:
            meta["causation_id"] = causation_id

        last_exc: BaseException | None = None

        for attempt in range(5):
            try:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():

                        # Ensure stream exists
                        await conn.execute(
                            """
                            INSERT INTO event_streams(stream_id, aggregate_type, current_version)
                            VALUES ($1, $2, -1)
                            ON CONFLICT DO NOTHING
                            """,
                            stream_id,
                            aggregate_type,
                        )

                        row = await conn.fetchrow(
                            """
                            SELECT current_version, last_event_hash, archived_at, aggregate_type, created_at, updated_at
                            FROM event_streams
                            WHERE stream_id=$1
                            FOR UPDATE
                            """,
                            stream_id,
                        )

                        if row["archived_at"] is not None:
                            raise StreamArchivedError(stream_id)

                        current = int(row["current_version"])

                        if current != expected_version:
                            raise OptimisticConcurrencyError(
                                stream_id, expected_version, current
                            )

                        prev_hash = row["last_event_hash"]
                        # Use database clock to satisfy audit requirements (clock_timestamp()).
                        recorded_at = await conn.fetchval("SELECT clock_timestamp()")

                        positions: list[int] = []
                        global_positions: list[int] = []

                        for i, event in enumerate(events):
                            stream_position = expected_version + 1 + i

                            event_type = event["event_type"]
                            event_version = event.get("event_version", 1)
                            payload = dict(event.get("payload", {}))

                            event_hash = _compute_event_hash(
                                stream_id=stream_id,
                                stream_position=stream_position,
                                event_type=event_type,
                                event_version=event_version,
                                payload=payload,
                                metadata=meta,
                                recorded_at=recorded_at,
                                prev_hash=prev_hash,
                            )

                            inserted = await conn.fetchrow(
                                """
                                INSERT INTO events(
                                    stream_id, stream_position,
                                    event_type, event_version,
                                    payload, metadata,
                                    recorded_at, prev_hash, event_hash
                                )
                                VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9)
                                RETURNING global_position, event_id
                                """,
                                stream_id,
                                stream_position,
                                event_type,
                                event_version,
                                _canonical_json(payload),
                                _canonical_json(meta),
                                recorded_at,
                                prev_hash,
                                event_hash,
                            )

                            global_position = int(inserted["global_position"])
                            event_id = inserted["event_id"]

                            # OUTBOX (full envelope)
                            await conn.execute(
                                """
                                INSERT INTO outbox(event_id, stream_id, stream_position, topic, payload)
                                VALUES ($1,$2,$3,$4,$5::jsonb)
                                """,
                                event_id,
                                stream_id,
                                stream_position,
                                event_type,
                                _canonical_json({
                                    "event_id": str(event_id),
                                    "global_position": global_position,
                                    "stream_id": stream_id,
                                    "stream_position": stream_position,
                                    "event_type": event_type,
                                    "event_version": event_version,
                                    "payload": payload,
                                    "metadata": meta,
                                    "recorded_at": recorded_at.isoformat(),
                                }),
                            )

                            prev_hash = event_hash
                            positions.append(stream_position)
                            global_positions.append(global_position)

                        # Update stream
                        await conn.execute(
                            """
                            UPDATE event_streams
                            SET current_version=$2,
                                last_event_hash=$3,
                                updated_at=clock_timestamp()
                            WHERE stream_id=$1
                            """,
                            stream_id,
                            expected_version + len(events),
                            prev_hash,
                        )

                        # Realtime projections: notify last global_position (single notification per tx).
                        if global_positions:
                            await conn.execute(
                                "SELECT pg_notify('ledger_events', $1)",
                                _canonical_json(
                                    {
                                        "stream_id": stream_id,
                                        "count": len(global_positions),
                                        "last_global_position": max(global_positions),
                                    }
                                ),
                            )

                        return positions

            except OptimisticConcurrencyError:
                raise

            except (
                asyncpg.exceptions.SerializationError,
                asyncpg.exceptions.DeadlockDetectedError,
                asyncpg.UniqueViolationError,
            ) as exc:
                last_exc = exc
                await asyncio.sleep(
                    (0.01 * (2 ** attempt)) + random.uniform(0, 0.02)
                )

        raise RuntimeError("append failed after retries") from last_exc

    # ─────────────────────────────────────────────────────────────────────────
    # Load Stream
    # ─────────────────────────────────────────────────────────────────────────

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:

        if self._pool is None:
            raise RuntimeError("EventStore not connected")

        async with self._pool.acquire() as conn:
            query = """
                SELECT *
                FROM events
                WHERE stream_id=$1 AND stream_position >= $2
            """
            params: list[Any] = [stream_id, from_position]

            if to_position is not None:
                query += " AND stream_position <= $3"
                params.append(to_position)

            query += " ORDER BY stream_position ASC"

            rows = await conn.fetch(query, *params)

        events: list[dict] = []

        for row in rows:
            e = dict(row)
            #e["payload"] = dict(e["payload"])
            #e["metadata"] = dict(e["metadata"])

            if self.upcasters:
                e = self.upcasters.upcast(e)

            events.append(e)

        return events

    # ─────────────────────────────────────────────────────────────────────────
    # Load All
    # ─────────────────────────────────────────────────────────────────────────

    async def load_all(
        self,
        from_position: int = 0,
        batch_size: int = 500,
    ) -> AsyncGenerator[dict, None]:

        if self._pool is None:
            raise RuntimeError("EventStore not connected")

        pos = int(from_position)

        while True:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT *
                    FROM events
                    WHERE global_position >= $1
                    ORDER BY global_position ASC
                    LIMIT $2
                    """,
                    pos,
                    int(batch_size),
                )

            if not rows:
                break

            for row in rows:
                e = dict(row)
                #e["payload"] = dict(e["payload"])
                #e["metadata"] = dict(e["metadata"])

                if self.upcasters:
                    e = self.upcasters.upcast(e)

                yield e
                pos = int(e["global_position"]) + 1

            if len(rows) < batch_size:
                break

    # ─────────────────────────────────────────────────────────────────────────
    # Get Event
    # ─────────────────────────────────────────────────────────────────────────

    async def get_event(self, event_id: UUID) -> dict | None:

        if self._pool is None:
            raise RuntimeError("EventStore not connected")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM events WHERE event_id=$1",
                event_id,
            )

        if not row:
            return None

        e = dict(row)
        e["payload"] = dict(e["payload"])
        e["metadata"] = dict(e["metadata"])

        if self.upcasters:
            e = self.upcasters.upcast(e)

        return e

    # ─────────────────────────────────────────────────────────────────────────
    # Metadata / Lifecycle
    # ─────────────────────────────────────────────────────────────────────────

    async def archive_stream(self, stream_id: str) -> None:
        if self._pool is None:
            raise RuntimeError("EventStore not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE event_streams
                SET archived_at = clock_timestamp()
                WHERE stream_id=$1
                """,
                stream_id,
            )

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        if self._pool is None:
            raise RuntimeError("EventStore not connected")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT stream_id, aggregate_type, current_version,
                       created_at, updated_at, archived_at, last_event_hash
                FROM event_streams
                WHERE stream_id=$1
                """,
                stream_id,
            )

        if row is None:
            return None
        d = dict(row)
        if d.get("last_event_hash") is not None and isinstance(d["last_event_hash"], (bytes, bytearray)):
            d["last_event_hash"] = d["last_event_hash"].hex()
        return StreamMetadata(**d)


# ─────────────────────────────────────────────────────────────────────────────
# In-memory EventStore (tests / fast replay)
# ─────────────────────────────────────────────────────────────────────────────


class InMemoryEventStore:
    """
    Asyncio-safe in-memory store with the same interface as EventStore.
    Used by unit tests; never for production.
    """

    def __init__(self, upcaster_registry=None):
        self.upcasters = upcaster_registry
        self._streams: dict[str, list[dict[str, Any]]] = {}
        self._versions: dict[str, int] = {}
        self._global: list[dict[str, Any]] = []
        self._locks: dict[str, asyncio.Lock] = {}
        self._checkpoints: dict[str, int] = {}
        self._stream_meta: dict[str, dict[str, Any]] = {}
        self._last_recorded_at: datetime | None = None

    def _lock_for(self, stream_id: str) -> asyncio.Lock:
        lock = self._locks.get(stream_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[stream_id] = lock
        return lock

    async def stream_version(self, stream_id: str) -> int:
        return int(self._versions.get(stream_id, -1))

    async def append(
        self,
        stream_id: str,
        events: list[dict[str, Any]],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> list[int]:
        if not events:
            return []
        for event in events:
            et = event.get("event_type")

            if et not in EVENT_REGISTRY:
                raise DomainSemanticError(event_type=et)

        meta = dict(metadata or {})
        if correlation_id is not None:
            meta["correlation_id"] = correlation_id
        if causation_id is not None:
            meta["causation_id"] = causation_id

        async with self._lock_for(stream_id):
            sm = self._stream_meta.get(stream_id)
            if sm and sm.get("archived_at") is not None:
                raise StreamArchivedError(stream_id)

            current = int(self._versions.get(stream_id, -1))
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)

            aggregate_type = stream_id.split("-", 1)[0] if "-" in stream_id else stream_id
            # Ensure recorded_at is monotonic (millisecond+ precision temporal queries).
            # This mirrors PostgreSQL's `clock_timestamp()` behavior in practice.
            now = datetime.now(timezone.utc)
            if stream_id not in self._stream_meta:
                self._stream_meta[stream_id] = {
                    "stream_id": stream_id,
                    "aggregate_type": aggregate_type,
                    "current_version": -1,
                    "created_at": now,
                    "updated_at": now,
                    "archived_at": None,
                    "last_event_hash": None,
                }

            prev_hash_hex = self._stream_meta[stream_id]["last_event_hash"]
            prev_hash = bytes.fromhex(prev_hash_hex) if isinstance(prev_hash_hex, str) else None

            positions: list[int] = []
            for i, event in enumerate(events):
                ev_now = datetime.now(timezone.utc)
                if self._last_recorded_at is not None and ev_now <= self._last_recorded_at:
                    ev_now = self._last_recorded_at + timedelta(microseconds=1)
                self._last_recorded_at = ev_now
                stream_position = expected_version + 1 + i
                event_type = str(event["event_type"])
                event_version = int(event.get("event_version") or 1)
                payload = dict(event.get("payload") or {})
                event_hash = _compute_event_hash(
                    stream_id=stream_id,
                    stream_position=stream_position,
                    event_type=event_type,
                    event_version=event_version,
                    payload=payload,
                    metadata=meta,
                    recorded_at=ev_now,
                    prev_hash=prev_hash,
                )
                stored = {
                    "global_position": len(self._global),
                    "event_id": uuid4(),
                    "stream_id": stream_id,
                    "stream_position": stream_position,
                    "event_type": event_type,
                    "event_version": event_version,
                    "payload": payload,
                    "metadata": dict(meta),
                    "recorded_at": ev_now,
                    "prev_hash": prev_hash.hex() if prev_hash else None,
                    "event_hash": event_hash.hex(),
                }
                self._streams.setdefault(stream_id, []).append(stored)
                self._global.append(stored)
                positions.append(stream_position)
                prev_hash = event_hash

            self._versions[stream_id] = expected_version + len(events)
            self._stream_meta[stream_id]["current_version"] = self._versions[stream_id]
            self._stream_meta[stream_id]["updated_at"] = now
            self._stream_meta[stream_id]["last_event_hash"] = prev_hash.hex() if prev_hash else None
            return positions

    async def load_stream(
        self, stream_id: str, from_position: int = 0, to_position: int | None = None
    ) -> list[dict[str, Any]]:
        events = [
            dict(e)
            for e in self._streams.get(stream_id, [])
            if int(e["stream_position"]) >= int(from_position)
            and (to_position is None or int(e["stream_position"]) <= int(to_position))
        ]
        events.sort(key=lambda e: int(e["stream_position"]))
        if self.upcasters is not None:
            events = [self.upcasters.upcast(e) for e in events]
        return events

    async def load_all(self, from_position: int = 0, batch_size: int = 500) -> AsyncGenerator[dict, None]:
        pos = int(from_position)
        while pos < len(self._global):
            batch = self._global[pos : pos + int(batch_size)]
            for e in batch:
                ev = dict(e)
                if self.upcasters is not None:
                    ev = self.upcasters.upcast(ev)
                yield ev
            pos += len(batch)

    async def get_event(self, event_id: UUID) -> dict[str, Any] | None:
        for e in self._global:
            if str(e["event_id"]) == str(event_id):
                ev = dict(e)
                if self.upcasters is not None:
                    ev = self.upcasters.upcast(ev)
                return ev
        return None

    async def archive_stream(self, stream_id: str) -> None:
        now = datetime.now(timezone.utc)
        meta = self._stream_meta.get(stream_id)
        if meta is None:
            aggregate_type = stream_id.split("-", 1)[0] if "-" in stream_id else stream_id
            meta = {
                "stream_id": stream_id,
                "aggregate_type": aggregate_type,
                "current_version": self._versions.get(stream_id, -1),
                "created_at": now,
                "updated_at": now,
                "archived_at": now,
                "last_event_hash": None,
            }
            self._stream_meta[stream_id] = meta
        else:
            meta["archived_at"] = now
            meta["updated_at"] = now

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        meta = self._stream_meta.get(stream_id)
        if meta is None:
            return None
        return StreamMetadata(**meta)

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = int(position)

    async def load_checkpoint(self, projection_name: str) -> int:
        return int(self._checkpoints.get(projection_name, 0))
