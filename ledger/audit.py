from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
from typing import Any

from ledger.event_store import _compute_event_hash  # intentionally reuse canonical hashing logic
from ledger.schema.events import AuditIntegrityCheckRun
from ledger.utils import ensure_dict


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


@dataclass(frozen=True, slots=True)
class IntegrityCheckResult:
    entity_type: str
    entity_id: str
    stream_id: str
    events_verified_count: int
    integrity_hash: str
    previous_hash: str | None
    chain_valid: bool
    tamper_detected: bool
    errors: list[AuditChainError]


def _rolling_chain_hash(*, previous_hash: bytes | None, event_hash: bytes) -> bytes:
    """
    Rolling audit chain hash:
        new_hash = sha256(previous_hash + event_hash)
    """
    prev = previous_hash or b""
    return hashlib.sha256(prev + event_hash).digest()


def compute_integrity_hash(
    *,
    previous_hash_hex: str | None,
    computed_event_hashes: list[bytes],
) -> str:
    prev: bytes | None = bytes.fromhex(previous_hash_hex) if previous_hash_hex else None
    head = prev
    for eh in computed_event_hashes:
        head = _rolling_chain_hash(previous_hash=head, event_hash=eh)
    return head.hex() if head is not None else hashlib.sha256(b"").hexdigest()


async def run_integrity_check(
    store: Any,
    *,
    entity_type: str,
    entity_id: str,
    stream_id: str,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> IntegrityCheckResult:
    """
    Verifies per-stream hash chain (prev_hash/event_hash), computes rolling integrity hash,
    and appends an AuditIntegrityCheckRun event to `audit-{entity_id}`.
    """
    events = await store.load_stream(stream_id, from_position=0)
    chain_valid, errors = verify_stream_hash_chain(events)
    tamper_detected = not chain_valid

    computed_hashes: list[bytes] = []
    prev_hash: bytes | None = None
    for e in sorted(events, key=lambda x: int(x["stream_position"])):
        recorded_at = e["recorded_at"]
        if isinstance(recorded_at, str):
            recorded_at = datetime.fromisoformat(recorded_at)
        computed_hashes.append(
            _compute_event_hash(
                stream_id=str(e["stream_id"]),
                stream_position=int(e["stream_position"]),
                event_type=str(e["event_type"]),
                event_version=int(e.get("event_version") or 1),
                #payload=dict(e.get("payload") or {}),
                payload=ensure_dict(e.get("payload") or {}),
                metadata=ensure_dict(e.get("metadata") or {}),
                recorded_at=recorded_at,
                prev_hash=prev_hash,
            )
        )
        prev_hash = computed_hashes[-1]

    audit_stream_id = f"audit-{entity_id}"
    prev_integrity: str | None = None
    try:
        audit_events = await store.load_stream(audit_stream_id, from_position=0)
        for ae in reversed(audit_events):
            if str(ae.get("event_type")) == "AuditIntegrityCheckRun":
                prev_integrity = str((ae.get("payload") or {}).get("integrity_hash") or "") or None
                break
    except Exception:
        prev_integrity = None

    integrity_hash = compute_integrity_hash(previous_hash_hex=prev_integrity, computed_event_hashes=computed_hashes)

    evt = AuditIntegrityCheckRun(
        entity_type=str(entity_type),
        entity_id=str(entity_id),
        check_timestamp=datetime.now().astimezone(),
        events_verified_count=len(events),
        integrity_hash=integrity_hash,
        previous_hash=prev_integrity,
        chain_valid=bool(chain_valid),
        tamper_detected=bool(tamper_detected),
    ).to_store_dict()

    expected = await store.stream_version(audit_stream_id)
    await store.append(
        audit_stream_id,
        [evt],
        expected_version=expected,
        correlation_id=correlation_id or entity_id,
        causation_id=causation_id or f"integrity:{entity_id}",
    )

    return IntegrityCheckResult(
        entity_type=str(entity_type),
        entity_id=str(entity_id),
        stream_id=str(stream_id),
        events_verified_count=len(events),
        integrity_hash=integrity_hash,
        previous_hash=prev_integrity,
        chain_valid=bool(chain_valid),
        tamper_detected=bool(tamper_detected),
        errors=list(errors),
    )
