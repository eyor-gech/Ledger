"""
ledger/domain/aggregates/audit_ledger.py

AuditLedger aggregate (Phase 1):
- Stores audit integrity check runs for an entity stream.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from ledger.schema.events import BaseEvent, StoredEvent, deserialize_event


@dataclass(slots=True)
class AuditLedger:
    entity_id: str
    last_check_at: datetime | None = None
    last_integrity_hash: str | None = None
    last_previous_hash: str | None = None
    last_chain_valid: bool | None = None
    last_tamper_detected: bool | None = None
    version: int = -1

    def apply(self, event: BaseEvent) -> None:
        handler = getattr(self, f"on_{event.event_type}", None)
        if handler is None:
            return
        handler(event)
        self.version += 1

    def apply_stored(self, stored: StoredEvent) -> None:
        event = deserialize_event(stored.event_type, stored.payload)
        handler = getattr(self, f"on_{event.event_type}", None)
        if handler is None:
            self.version = stored.stream_position
            return
        handler(event)
        self.version = stored.stream_position

    def on_AuditIntegrityCheckRun(self, event: BaseEvent) -> None:
        p = event.to_payload()
        ts = p["check_timestamp"]
        self.last_check_at = ts if isinstance(ts, datetime) else datetime.fromisoformat(str(ts))
        self.last_integrity_hash = str(p["integrity_hash"])
        self.last_previous_hash = str(p.get("previous_hash") or "") or None
        self.last_chain_valid = bool(p["chain_valid"])
        self.last_tamper_detected = bool(p["tamper_detected"])

    @classmethod
    def rebuild(cls, events: Iterable[BaseEvent] | Iterable[StoredEvent]) -> "AuditLedger":
        events_list = list(events)
        if not events_list:
            raise ValueError("cannot rebuild AuditLedger from empty event list")
        if isinstance(events_list[0], StoredEvent):
            first = deserialize_event(events_list[0].event_type, events_list[0].payload).to_payload()
            entity_id = str(first.get("entity_id") or "")
            if not entity_id:
                raise ValueError("first audit event must include entity_id")
            agg = cls(entity_id=entity_id)
            for se in events_list:
                agg.apply_stored(se)
            return agg
        first = events_list[0].to_payload()  # type: ignore[union-attr]
        entity_id = str(first.get("entity_id") or "")
        if not entity_id:
            raise ValueError("first audit event must include entity_id")
        agg = cls(entity_id=entity_id)
        for e in events_list:  # type: ignore[assignment]
            agg.apply(e)  # type: ignore[arg-type]
        return agg
