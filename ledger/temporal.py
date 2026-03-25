from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator


def parse_as_of(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


async def load_stream_as_of(store: Any, stream_id: str, *, as_of: datetime) -> list[dict[str, Any]]:
    events = await store.load_stream(stream_id, from_position=0)
    cutoff = as_of
    out: list[dict[str, Any]] = []
    for e in events:
        ra = e.get("recorded_at")
        if ra is None:
            out.append(e)
            continue
        if isinstance(ra, str):
            ra_dt = datetime.fromisoformat(ra)
        else:
            ra_dt = ra
        if ra_dt.tzinfo is None:
            ra_dt = ra_dt.replace(tzinfo=timezone.utc)
        if ra_dt <= cutoff:
            out.append(e)
    return out


async def iter_by_correlation_id(store: Any, correlation_id: str, *, from_position: int = 0) -> AsyncIterator[dict[str, Any]]:
    """
    Cross-stream reconstruction helper: yields all events whose metadata correlation_id matches.
    Sequentially consistent (global_position order).
    """
    async for e in store.load_all(from_position=from_position, batch_size=500):
        meta = e.get("metadata") or {}
        if str(meta.get("correlation_id") or "") == str(correlation_id):
            yield e


@dataclass(frozen=True, slots=True)
class CausalLink:
    event_id: str
    causation_id: str
    correlation_id: str


async def causal_links_for_event(store: Any, event_id: str) -> CausalLink | None:
    # Postgres store expects UUID; InMemory store accepts str.
    try:
        from uuid import UUID

        eid: Any = UUID(str(event_id))
    except Exception:
        eid = event_id
    e = await store.get_event(eid)
    if e is None:
        return None
    meta = e.get("metadata") or {}
    return CausalLink(
        event_id=str(e.get("event_id") or event_id),
        causation_id=str(meta.get("causation_id") or ""),
        correlation_id=str(meta.get("correlation_id") or ""),
    )
