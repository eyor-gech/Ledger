"""
ledger/domain/aggregates/fraud_screening.py

FraudScreening aggregate (Phase 1):
- Deterministic state reconstruction from the fraud stream.
- Enforces basic invariants on anomaly counting and fraud score bounds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from ledger.domain.errors import InvariantViolation
from ledger.schema.events import BaseEvent, StoredEvent, deserialize_event


@dataclass(slots=True)
class FraudScreening:
    application_id: str
    initiated_at: datetime | None = None
    completed_at: datetime | None = None
    fraud_score: float | None = None
    risk_level: str | None = None
    recommendation: str | None = None
    anomalies: list[dict] = field(default_factory=list)
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

    def on_FraudScreeningInitiated(self, event: BaseEvent) -> None:
        p = event.to_payload()
        initiated_at = p["initiated_at"]
        self.initiated_at = initiated_at if isinstance(initiated_at, datetime) else datetime.fromisoformat(str(initiated_at))

    def on_FraudAnomalyDetected(self, event: BaseEvent) -> None:
        p = event.to_payload()
        anomaly = p.get("anomaly")
        self.anomalies.append(dict(anomaly) if isinstance(anomaly, dict) else {"value": anomaly})

    def on_FraudScreeningCompleted(self, event: BaseEvent) -> None:
        p = event.to_payload()
        completed_at = p["completed_at"]
        self.completed_at = completed_at if isinstance(completed_at, datetime) else datetime.fromisoformat(str(completed_at))
        score = float(p["fraud_score"])
        if not (0.0 <= score <= 1.0):
            raise InvariantViolation("fraud_score out of range")
        self.fraud_score = score
        self.risk_level = str(p["risk_level"])
        self.recommendation = str(p["recommendation"])
        expected = int(p.get("anomalies_found") or 0)
        if expected != len(self.anomalies):
            raise InvariantViolation("anomalies_found does not match detected anomalies count")

    @classmethod
    def rebuild(cls, events: Iterable[BaseEvent] | Iterable[StoredEvent]) -> "FraudScreening":
        events_list = list(events)
        if not events_list:
            raise ValueError("cannot rebuild FraudScreening from empty event list")
        if isinstance(events_list[0], StoredEvent):
            first = deserialize_event(events_list[0].event_type, events_list[0].payload).to_payload()
            app_id = str(first.get("application_id") or "")
            if not app_id:
                raise ValueError("first fraud event must include application_id")
            agg = cls(application_id=app_id)
            for se in events_list:
                agg.apply_stored(se)
            return agg
        first = events_list[0].to_payload()  # type: ignore[union-attr]
        app_id = str(first.get("application_id") or "")
        if not app_id:
            raise ValueError("first fraud event must include application_id")
        agg = cls(application_id=app_id)
        for e in events_list:  # type: ignore[assignment]
            agg.apply(e)  # type: ignore[arg-type]
        return agg
