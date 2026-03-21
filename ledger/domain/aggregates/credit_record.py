"""
ledger/domain/aggregates/credit_record.py

CreditRecord aggregate (Phase 1):
- Deterministic state reconstruction from the credit stream.
- Enforces basic invariants on the analysis output.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable

from ledger.domain.errors import InvariantViolation
from ledger.schema.events import BaseEvent, CreditDecision, RiskTier, StoredEvent, deserialize_event


@dataclass(slots=True)
class CreditRecord:
    application_id: str
    applicant_id: str | None = None
    opened_at: datetime | None = None
    last_decision: CreditDecision | None = None
    last_risk_tier: RiskTier | None = None
    last_confidence: float | None = None
    last_recommended_limit_usd: Decimal | None = None
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

    def on_CreditRecordOpened(self, event: BaseEvent) -> None:
        p = event.to_payload()
        self.applicant_id = str(p["applicant_id"])
        opened_at = p["opened_at"]
        self.opened_at = opened_at if isinstance(opened_at, datetime) else datetime.fromisoformat(str(opened_at))

    def on_CreditAnalysisCompleted(self, event: BaseEvent) -> None:
        p = event.to_payload()
        decision_obj = p.get("decision")
        decision = CreditDecision(**decision_obj) if isinstance(decision_obj, dict) else decision_obj
        if not isinstance(decision, CreditDecision):
            raise InvariantViolation("CreditAnalysisCompleted.decision missing/invalid")
        if not (0.0 <= float(decision.confidence) <= 1.0):
            raise InvariantViolation("credit decision confidence out of range")
        if Decimal(str(decision.recommended_limit_usd)) < 0:
            raise InvariantViolation("recommended_limit_usd must be >= 0")
        self.last_decision = decision
        self.last_risk_tier = decision.risk_tier
        self.last_confidence = float(decision.confidence)
        self.last_recommended_limit_usd = Decimal(str(decision.recommended_limit_usd))

    @classmethod
    def rebuild(cls, events: Iterable[BaseEvent] | Iterable[StoredEvent]) -> "CreditRecord":
        events_list = list(events)
        if not events_list:
            raise ValueError("cannot rebuild CreditRecord from empty event list")
        if isinstance(events_list[0], StoredEvent):
            first = deserialize_event(events_list[0].event_type, events_list[0].payload).to_payload()
            app_id = str(first.get("application_id") or "")
            if not app_id:
                raise ValueError("first credit event must include application_id")
            agg = cls(application_id=app_id)
            for se in events_list:
                agg.apply_stored(se)
            return agg
        first = events_list[0].to_payload()  # type: ignore[union-attr]
        app_id = str(first.get("application_id") or "")
        if not app_id:
            raise ValueError("first credit event must include application_id")
        agg = cls(application_id=app_id)
        for e in events_list:  # type: ignore[assignment]
            agg.apply(e)  # type: ignore[arg-type]
        return agg
