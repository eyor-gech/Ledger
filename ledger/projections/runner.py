from __future__ import annotations

import asyncio
import json
import time
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


async def run_once(
    *,
    store: Any,
    pool: asyncpg.Pool,
    projections: list[ProjectionSpec],
    batch_size: int = 500,
) -> None:
    """
    Catch up all projections to the end of the event log.
    Applies events in global_position order and stores per-projection checkpoints.
    """
    async with pool.acquire() as conn:
        checkpoints = {p.name: await _ensure_checkpoint(conn, p.name) for p in projections}

    # For sequential consistency, we take the minimum checkpoint as the starting point.
    start = min(checkpoints.values()) if checkpoints else 0

    async for event in store.load_all(from_position=start, batch_size=batch_size):
        gp = int(event["global_position"])
        # Apply each projection in its own transaction to keep checkpoints consistent.
        for p in projections:
            if gp <= int(checkpoints[p.name]):
                continue
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Re-check checkpoint inside the transaction.
                    current = await conn.fetchval(
                        "SELECT global_position FROM projection_checkpoints WHERE projection_name=$1 FOR UPDATE",
                        p.name,
                    )
                    current_pos = int(current or 0)
                    if gp <= current_pos:
                        checkpoints[p.name] = current_pos
                        continue

                    await p.apply(conn, event)
                    await _save_checkpoint(conn, p.name, gp)
                    checkpoints[p.name] = gp

    latest_gp = None
        
    async for ev in store.load_all(from_position=0, batch_size=1):
        latest_gp = int(ev["global_position"])

    if latest_gp is not None:
        for p in projections:
            current_cp = checkpoints.get(p.name, 0)
            lag = latest_gp - current_cp
            print(f"[LAG] Projection={p.name} lag={lag}")

async def listen_and_project(
    *,
    store: Any,
    pool: asyncpg.Pool,
    projections: list[ProjectionSpec],
    channel: str = "ledger_events",
    poll_interval_s: float = 0.5,
) -> None:
    """
    Best-effort realtime projector:
    - LISTEN on `ledger_events` for low-latency triggers
    - Fallback to periodic catch-up if notifications are missed
    """
    # Initial catch-up
    await run_once(store=store, pool=pool, projections=projections)

    async def _handle_notify(_: asyncpg.Connection, __: int, ___: str, payload: str) -> None:
        try:
            _ = json.loads(payload)
        except Exception:
            return
        await run_once(store=store, pool=pool, projections=projections)

    conn = await pool.acquire()
    try:
        await conn.add_listener(channel, _handle_notify)
        await conn.execute(f"LISTEN {channel}")
        while True:
            await asyncio.sleep(poll_interval_s)
            await run_once(store=store, pool=pool, projections=projections)
    finally:
        try:
            await conn.execute(f"UNLISTEN {channel}")
        finally:
            await pool.release(conn)

