from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import asyncpg


@dataclass(slots=True)
class ProjectionSpec:
    name: str
    apply: Callable[[asyncpg.Connection, dict[str, Any]], Awaitable[None]]


async def _ensure_checkpoint(conn: asyncpg.Connection, projection_name: str) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO projection_checkpoints(projection_name, global_position)
        VALUES($1, 0)
        ON CONFLICT (projection_name) DO NOTHING
        RETURNING global_position
        """,
        projection_name,
    )
    if row is not None:
        return int(row["global_position"])
    existing = await conn.fetchval(
        "SELECT global_position FROM projection_checkpoints WHERE projection_name=$1",
        projection_name,
    )
    return int(existing or 0)


async def _save_checkpoint(conn: asyncpg.Connection, projection_name: str, global_position: int) -> None:
    await conn.execute(
        """
        UPDATE projection_checkpoints
        SET global_position=$2, updated_at=clock_timestamp()
        WHERE projection_name=$1
        """,
        projection_name,
        int(global_position),
    )


async def _record_failure(
    conn: asyncpg.Connection,
    *,
    projection_name: str,
    global_position: int,
    error: BaseException,
) -> None:
    await conn.execute(
        """
        INSERT INTO projection_failures(projection_name, global_position, attempts, last_error, first_failed_at, last_failed_at)
        VALUES($1, $2, 1, $3, clock_timestamp(), clock_timestamp())
        ON CONFLICT (projection_name, global_position) DO UPDATE
          SET attempts = projection_failures.attempts + 1,
              last_error = EXCLUDED.last_error,
              last_failed_at = clock_timestamp()
        """,
        projection_name,
        int(global_position),
        str(error),
    )


class ProjectionDaemon:
    """
    Fault-tolerant projection daemon:
    - Applies events in global_position order.
    - Starts from the **lowest** checkpoint (sequential consistency).
    - Retries per (projection,event) and skips after budget to avoid crashing.
    - Exposes per-projection lag in milliseconds.
    """

    def __init__(
        self,
        *,
        store: Any,
        pool: asyncpg.Pool,
        projections: list[ProjectionSpec],
        batch_size: int = 500,
        poll_interval_s: float = 0.5,
        max_retries: int = 3,
        retry_backoff_s: float = 0.05,
        channel: str = "ledger_events",
    ) -> None:
        self.store = store
        self.pool = pool
        self.projections = projections
        self.batch_size = int(batch_size)
        self.poll_interval_s = float(poll_interval_s)
        self.max_retries = int(max_retries)
        self.retry_backoff_s = float(retry_backoff_s)
        self.channel = str(channel)

        self._stop = asyncio.Event()
        self._poke = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> asyncio.Task[None]:
        if self._task is not None:
            return self._task
        self._task = asyncio.create_task(self.run(), name="projection-daemon")
        return self._task

    async def stop(self) -> None:
        self._stop.set()
        self._poke.set()
        if self._task is not None:
            await self._task

    async def _load_checkpoints(self) -> dict[str, int]:
        async with self.pool.acquire() as conn:
            cps = {p.name: await _ensure_checkpoint(conn, p.name) for p in self.projections}
        return cps

    async def catch_up_once(self) -> None:
        checkpoints = await self._load_checkpoints()
        start = min(checkpoints.values()) if checkpoints else 0

        async for event in self.store.load_all(from_global_position=start, batch_size=self.batch_size):
            gp = int(event["global_position"])
            for p in self.projections:
                if gp <= int(checkpoints.get(p.name, 0)):
                    continue

                attempt = 0
                while True:
                    try:
                        async with self.pool.acquire() as conn:
                            async with conn.transaction():
                                # Lock checkpoint row for the projection to keep idempotency stable.
                                current = await conn.fetchval(
                                    "SELECT global_position FROM projection_checkpoints WHERE projection_name=$1 FOR UPDATE",
                                    p.name,
                                )
                                current_pos = int(current or 0)
                                if gp <= current_pos:
                                    checkpoints[p.name] = current_pos
                                    break

                                await p.apply(conn, event)
                                await _save_checkpoint(conn, p.name, gp)
                                checkpoints[p.name] = gp
                        break
                    except Exception as exc:
                        attempt += 1
                        async with self.pool.acquire() as conn:
                            async with conn.transaction():
                                await _record_failure(
                                    conn,
                                    projection_name=p.name,
                                    global_position=gp,
                                    error=exc,
                                )
                        if attempt <= self.max_retries:
                            await asyncio.sleep(self.retry_backoff_s * (2 ** (attempt - 1)))
                            continue
                        # Skip bad event for this projection after exhausting retries.
                        async with self.pool.acquire() as conn:
                            async with conn.transaction():
                                await _save_checkpoint(conn, p.name, gp)
                                checkpoints[p.name] = gp
                        break

    async def get_lag_ms(self) -> dict[str, int]:
        """
        Lag is measured as:
            latest_event.recorded_at - checkpoint_event.recorded_at
        in milliseconds, per projection.
        """
        async with self.pool.acquire() as conn:
            latest = await conn.fetchrow(
                "SELECT global_position, recorded_at FROM events ORDER BY global_position DESC LIMIT 1"
            )
            if latest is None:
                return {p.name: 0 for p in self.projections}
            latest_gp = int(latest["global_position"])
            latest_ts = latest["recorded_at"]

            lags: dict[str, int] = {}
            for p in self.projections:
                cp = await conn.fetchval(
                    "SELECT global_position FROM projection_checkpoints WHERE projection_name=$1",
                    p.name,
                )
                cp_gp = int(cp or 0)
                if cp_gp >= latest_gp:
                    lags[p.name] = 0
                    continue
                cp_ts = await conn.fetchval("SELECT recorded_at FROM events WHERE global_position=$1", cp_gp)
                if cp_ts is None:
                    lags[p.name] = 0
                    continue
                delta = latest_ts - cp_ts
                lags[p.name] = max(0, int(delta.total_seconds() * 1000))
            return lags

    async def run(self) -> None:
        # Initial catch-up.
        await self.catch_up_once()

        async def _handle_notify(_: asyncpg.Connection, __: int, ___: str, payload: str) -> None:
            try:
                _ = json.loads(payload)
            except Exception:
                return
            self._poke.set()

        conn = await self.pool.acquire()
        try:
            await conn.add_listener(self.channel, _handle_notify)
            await conn.execute(f"LISTEN {self.channel}")

            while not self._stop.is_set():
                try:
                    # Wait for either a notification poke or the poll interval.
                    try:
                        await asyncio.wait_for(self._poke.wait(), timeout=self.poll_interval_s)
                    except TimeoutError:
                        pass
                    self._poke.clear()
                    await self.catch_up_once()
                except Exception:
                    # Never crash the daemon; back off briefly.
                    await asyncio.sleep(self.retry_backoff_s)
        finally:
            try:
                await conn.execute(f"UNLISTEN {self.channel}")
            finally:
                await self.pool.release(conn)

