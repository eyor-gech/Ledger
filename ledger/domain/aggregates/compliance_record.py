"""
ledger/domain/aggregates/compliance_record.py

ComplianceRecord aggregate (Phase 1):
- Deterministic state reconstruction from the compliance stream.
- Enforces hard-block rules and completion invariants.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from ledger.domain.errors import InvariantViolation
from ledger.schema.events import BaseEvent, ComplianceVerdict, StoredEvent, deserialize_event


@dataclass(slots=True)
class ComplianceRecord:
    application_id: str
    initiated_at: datetime | None = None
    completed_at: datetime | None = None
    overall_verdict: ComplianceVerdict | None = None
    has_hard_block: bool = False
    rules: dict[str, dict] = field(default_factory=dict)  # rule_id -> last evaluation payload
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

    def on_ComplianceCheckInitiated(self, event: BaseEvent) -> None:
        p = event.to_payload()
        initiated_at = p["initiated_at"]
        self.initiated_at = initiated_at if isinstance(initiated_at, datetime) else datetime.fromisoformat(str(initiated_at))

    def on_ComplianceRulePassed(self, event: BaseEvent) -> None:
        p = event.to_payload()
        self.rules[str(p["rule_id"])] = dict(p)

    def on_ComplianceRuleFailed(self, event: BaseEvent) -> None:
        p = event.to_payload()
        self.rules[str(p["rule_id"])] = dict(p)
        if bool(p.get("is_hard_block")):
            self.has_hard_block = True

    def on_ComplianceRuleNoted(self, event: BaseEvent) -> None:
        p = event.to_payload()
        self.rules[str(p["rule_id"])] = dict(p)

    def on_ComplianceCheckCompleted(self, event: BaseEvent) -> None:
        if self.initiated_at is None:
            raise InvariantViolation("ComplianceCheckCompleted without ComplianceCheckInitiated")
        p = event.to_payload()
        completed_at = p["completed_at"]
        self.completed_at = completed_at if isinstance(completed_at, datetime) else datetime.fromisoformat(str(completed_at))
        ov = p["overall_verdict"]
        self.overall_verdict = ov if isinstance(ov, ComplianceVerdict) else ComplianceVerdict(str(ov))
        if bool(p.get("has_hard_block")) and self.overall_verdict != ComplianceVerdict.BLOCKED:
            raise InvariantViolation("has_hard_block requires overall_verdict == BLOCKED")
        if self.has_hard_block and self.overall_verdict != ComplianceVerdict.BLOCKED:
            raise InvariantViolation("hard-block rule failed but overall_verdict != BLOCKED")

    @classmethod
    def rebuild(cls, events: Iterable[BaseEvent] | Iterable[StoredEvent]) -> "ComplianceRecord":
        events_list = list(events)
        if not events_list:
            raise ValueError("cannot rebuild ComplianceRecord from empty event list")
        if isinstance(events_list[0], StoredEvent):
            first = deserialize_event(events_list[0].event_type, events_list[0].payload).to_payload()
            app_id = str(first.get("application_id") or "")
            if not app_id:
                raise ValueError("first compliance event must include application_id")
            agg = cls(application_id=app_id)
            for se in events_list:
                agg.apply_stored(se)
            return agg
        first = events_list[0].to_payload()  # type: ignore[union-attr]
        app_id = str(first.get("application_id") or "")
        if not app_id:
            raise ValueError("first compliance event must include application_id")
        agg = cls(application_id=app_id)
        for e in events_list:  # type: ignore[assignment]
            agg.apply(e)  # type: ignore[arg-type]
        return agg
