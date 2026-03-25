from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ledger.event_store import InMemoryEventStore


@dataclass(frozen=True, slots=True)
class WhatIfBranch:
    branch_id: str
    store: InMemoryEventStore
    base_global_position: int


async def branch_in_memory(
    *,
    base_store: InMemoryEventStore,
    branch_id: str,
    up_to_global_position: int,
) -> WhatIfBranch:
    """
    Creates an in-memory branch by replaying events up to a global_position (inclusive),
    then allowing hypothetical appends against the forked store.
    """
    fork = InMemoryEventStore(upcaster_registry=base_store.upcasters)
    max_gp = int(up_to_global_position)

    async for e in base_store.load_all(from_position=0, batch_size=500):
        gp = int(e["global_position"])
        if gp > max_gp:
            break
        stream_id = str(e["stream_id"])
        expected = await fork.stream_version(stream_id)
        meta = dict(e.get("metadata") or {})
        correlation_id = meta.get("correlation_id")
        causation_id = meta.get("causation_id")
        # Keep user metadata (excluding causal keys that are passed explicitly).
        meta.pop("correlation_id", None)
        meta.pop("causation_id", None)
        await fork.append(
            stream_id=stream_id,
            events=[{"event_type": e["event_type"], "event_version": e.get("event_version", 1), "payload": dict(e.get("payload") or {})}],
            expected_version=int(expected),
            correlation_id=str(correlation_id) if correlation_id is not None else None,
            causation_id=str(causation_id) if causation_id is not None else None,
            metadata=meta,
        )

    return WhatIfBranch(branch_id=str(branch_id), store=fork, base_global_position=max_gp)

