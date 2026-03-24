from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ledger.event_store import _compute_event_hash  # intentionally reuse canonical hashing logic


@dataclass(frozen=True, slots=True)
class AuditChainError:
    stream_id: str
    stream_position: int
    reason: str


def verify_stream_hash_chain(events: list[dict[str, Any]]) -> tuple[bool, list[AuditChainError]]:
    """
    Verifies the per-stream hash chain (prev_hash/event_hash) across ordered events.
    The event store computes `event_hash` using canonical JSON of event envelope + prev_hash.
    """
    errors: list[AuditChainError] = []
    if not events:
        return True, errors

    # Ensure ordered by stream_position
    events_sorted = sorted(events, key=lambda e: int(e["stream_position"]))
    prev_hash: bytes | None = None

    for e in events_sorted:
        try:
            recorded_at = e["recorded_at"]
            if isinstance(recorded_at, str):
                recorded_at = datetime.fromisoformat(recorded_at)
            computed = _compute_event_hash(
                stream_id=str(e["stream_id"]),
                stream_position=int(e["stream_position"]),
                event_type=str(e["event_type"]),
                event_version=int(e.get("event_version") or 1),
                payload=dict(e.get("payload") or {}),
                metadata=dict(e.get("metadata") or {}),
                recorded_at=recorded_at,
                prev_hash=prev_hash,
            )
            stored_hash = e.get("event_hash")
            stored_bytes = bytes.fromhex(stored_hash) if isinstance(stored_hash, str) else stored_hash
            if stored_bytes != computed:
                errors.append(
                    AuditChainError(
                        stream_id=str(e["stream_id"]),
                        stream_position=int(e["stream_position"]),
                        reason="event_hash mismatch",
                    )
                )

            stored_prev = e.get("prev_hash")
            stored_prev_bytes = bytes.fromhex(stored_prev) if isinstance(stored_prev, str) else stored_prev
            if stored_prev_bytes != prev_hash:
                errors.append(
                    AuditChainError(
                        stream_id=str(e["stream_id"]),
                        stream_position=int(e["stream_position"]),
                        reason="prev_hash mismatch",
                    )
                )
            prev_hash = computed
        except Exception as exc:
            errors.append(
                AuditChainError(
                    stream_id=str(e.get("stream_id") or ""),
                    stream_position=int(e.get("stream_position") or -1),
                    reason=f"verification error: {exc}",
                )
            )

    return len(errors) == 0, errors


async def verify_store_stream(store: Any, stream_id: str) -> tuple[bool, list[AuditChainError]]:
    events = await store.load_stream(stream_id, from_position=0)
    return verify_stream_hash_chain(events)

