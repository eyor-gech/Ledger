"""
ledger/domain/aggregates/loan_application.py

LoanApplication aggregate (Phase 1):
- Deterministic state reconstruction from the loan stream.
- Strict state transitions.
- Invariants around decision confidence + compliance hard-blocks (when known).

LoanApplication lifecycle states:

1. SUBMITTED
2. DOCUMENTS_PENDING
3. DOCUMENTS_UPLOADED
4. DOCUMENTS_PROCESSED
5. CREDIT_ANALYSIS_REQUESTED → CREDIT_ANALYSIS_COMPLETE
6. FRAUD_SCREENING_REQUESTED → FRAUD_SCREENING_COMPLETE
7. COMPLIANCE_CHECK_REQUESTED → COMPLIANCE_CHECK_COMPLETE

Terminal outcomes:
- APPROVED
- DECLINED
- DECLINED_COMPLIANCE
- REFERRED
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Callable, Iterable

from ledger.domain.errors import IllegalStateTransition, InvariantViolation
from ledger.schema.events import ApplicationState, BaseEvent, StoredEvent, deserialize_event


_ALLOWED: dict[ApplicationState, set[ApplicationState]] = {
    ApplicationState.SUBMITTED: {ApplicationState.DOCUMENTS_PENDING},
    ApplicationState.DOCUMENTS_PENDING: {ApplicationState.DOCUMENTS_UPLOADED},
    ApplicationState.DOCUMENTS_UPLOADED: {ApplicationState.DOCUMENTS_PROCESSED},
    ApplicationState.DOCUMENTS_PROCESSED: {ApplicationState.CREDIT_ANALYSIS_REQUESTED},
    ApplicationState.CREDIT_ANALYSIS_REQUESTED: {ApplicationState.CREDIT_ANALYSIS_COMPLETE},
    ApplicationState.CREDIT_ANALYSIS_COMPLETE: {ApplicationState.FRAUD_SCREENING_REQUESTED},
    ApplicationState.FRAUD_SCREENING_REQUESTED: {ApplicationState.FRAUD_SCREENING_COMPLETE},
    ApplicationState.FRAUD_SCREENING_COMPLETE: {ApplicationState.COMPLIANCE_CHECK_REQUESTED},
    ApplicationState.COMPLIANCE_CHECK_REQUESTED: {
        ApplicationState.COMPLIANCE_CHECK_COMPLETE,
        ApplicationState.DECLINED_COMPLIANCE,
    },
    ApplicationState.COMPLIANCE_CHECK_COMPLETE: {ApplicationState.PENDING_DECISION, ApplicationState.DECLINED_COMPLIANCE},
    ApplicationState.PENDING_DECISION: {
        ApplicationState.APPROVED,
        ApplicationState.DECLINED,
        ApplicationState.PENDING_HUMAN_REVIEW,
    },
    ApplicationState.PENDING_HUMAN_REVIEW: {ApplicationState.APPROVED, ApplicationState.DECLINED},
}


@dataclass(slots=True)
class LoanApplication:
    application_id: str
    state: ApplicationState | None = None
    applicant_id: str | None = None
    requested_amount_usd: Decimal | None = None
    loan_purpose: str | None = None
    loan_term_months: int | None = None
    submission_channel: str | None = None
    contact_email: str | None = None
    contact_name: str | None = None
    submitted_at: datetime | None = None
    last_decision_recommendation: str | None = None  # APPROVE|DECLINE|REFER
    last_decision_confidence: float | None = None
    version: int = -1  # stream_version
    document_ids: set[str] = field(default_factory=set)

    def _transition(self, target: ApplicationState) -> None:
        if self.state is None:
            self.state = target
            return
        if self.state == target:
            return
        allowed = _ALLOWED.get(self.state, set())
        if target not in allowed:
            raise IllegalStateTransition("LoanApplication", self.state.value, target.value)
        self.state = target

    # ── Guard methods (used by command handlers; no business conditionals there) ──

    def guard_can_request_credit_analysis(self) -> None:
        if self.state != ApplicationState.DOCUMENTS_PROCESSED:
            raise InvariantViolation(f"Credit analysis requires DOCUMENTS_PROCESSED, got {self.state}")

    def guard_can_request_fraud_screening(self) -> None:
        if self.state != ApplicationState.CREDIT_ANALYSIS_COMPLETE:
            raise InvariantViolation(f"Fraud screening requires CREDIT_ANALYSIS_COMPLETE, got {self.state}")

    def guard_can_request_compliance(self) -> None:
        if self.state != ApplicationState.FRAUD_SCREENING_COMPLETE:
            raise InvariantViolation(f"Compliance requires FRAUD_SCREENING_COMPLETE, got {self.state}")

    def guard_can_request_decision(self) -> None:
        if self.state != ApplicationState.COMPLIANCE_CHECK_COMPLETE:
            raise InvariantViolation(f"Decision requires COMPLIANCE_CHECK_COMPLETE, got {self.state}")

    def guard_can_accept_credit_analysis_result(self) -> None:
        if self.state != ApplicationState.CREDIT_ANALYSIS_REQUESTED:
            raise InvariantViolation(f"Credit analysis result requires CREDIT_ANALYSIS_REQUESTED, got {self.state}")

    def guard_can_complete_human_review(self) -> None:
        if self.state != ApplicationState.PENDING_HUMAN_REVIEW:
            raise InvariantViolation(f"Human review completion requires PENDING_HUMAN_REVIEW, got {self.state}")

    # ── Event application ──

    def apply(self, event: BaseEvent) -> None:
        handler = getattr(self, f"on_{event.event_type}", None)
        if handler is None:
            # Forward-compatible: ignore unknown events.
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

    # ── Per-event handlers ──

    def on_ApplicationSubmitted(self, event: BaseEvent) -> None:
        p = event.to_payload()
        self._transition(ApplicationState.SUBMITTED)
        self.applicant_id = str(p["applicant_id"])
        self.requested_amount_usd = Decimal(str(p["requested_amount_usd"]))
        self.loan_purpose = str(p["loan_purpose"])
        self.loan_term_months = int(p["loan_term_months"])
        self.submission_channel = str(p["submission_channel"])
        self.contact_email = str(p["contact_email"])
        self.contact_name = str(p["contact_name"])
        self.submitted_at = p["submitted_at"] if isinstance(p["submitted_at"], datetime) else datetime.fromisoformat(str(p["submitted_at"]))

    def on_DocumentUploadRequested(self, event: BaseEvent) -> None:
        self._transition(ApplicationState.DOCUMENTS_PENDING)

    def on_DocumentUploaded(self, event: BaseEvent) -> None:
        p = event.to_payload()
        self._transition(ApplicationState.DOCUMENTS_UPLOADED)
        self.document_ids.add(str(p["document_id"]))

    def on_PackageReadyForAnalysis(self, event: BaseEvent) -> None:
        # This event lives in docpkg stream, but is sometimes included in loan projections.
        self._transition(ApplicationState.DOCUMENTS_PROCESSED)

    def on_CreditAnalysisRequested(self, event: BaseEvent) -> None:
        self._transition(ApplicationState.CREDIT_ANALYSIS_REQUESTED)

    def on_CreditAnalysisCompleted(self, event: BaseEvent) -> None:
        self._transition(ApplicationState.CREDIT_ANALYSIS_COMPLETE)

    def on_FraudScreeningRequested(self, event: BaseEvent) -> None:
        p = event.to_payload()
        if not str(p.get("triggered_by_event_id") or "").strip():
            raise InvariantViolation("FraudScreeningRequested.triggered_by_event_id required")
        self._transition(ApplicationState.FRAUD_SCREENING_REQUESTED)

    def on_FraudScreeningCompleted(self, event: BaseEvent) -> None:
        self._transition(ApplicationState.FRAUD_SCREENING_COMPLETE)

    def on_ComplianceCheckRequested(self, event: BaseEvent) -> None:
        p = event.to_payload()
        if not str(p.get("triggered_by_event_id") or "").strip():
            raise InvariantViolation("ComplianceCheckRequested.triggered_by_event_id required")
        self._transition(ApplicationState.COMPLIANCE_CHECK_REQUESTED)

    def on_ComplianceCheckCompleted(self, event: BaseEvent) -> None:
        self._transition(ApplicationState.COMPLIANCE_CHECK_COMPLETE)

    def on_DecisionRequested(self, event: BaseEvent) -> None:
        p = event.to_payload()
        if not str(p.get("triggered_by_event_id") or "").strip():
            raise InvariantViolation("DecisionRequested.triggered_by_event_id required")
        self._transition(ApplicationState.PENDING_DECISION)

    def on_DecisionGenerated(self, event: BaseEvent) -> None:
        p = event.to_payload()
        recommendation = str(p["recommendation"])
        confidence = float(p["confidence"])
        if confidence < 0.60 and recommendation != "REFER":
            raise InvariantViolation("confidence < 0.60 requires recommendation == 'REFER'")
        self.last_decision_recommendation = recommendation
        self.last_decision_confidence = confidence
        if recommendation == "REFER":
            self.state = ApplicationState.REFERRED

    def on_HumanReviewRequested(self, event: BaseEvent) -> None:
        self._transition(ApplicationState.PENDING_HUMAN_REVIEW)

    def on_ApplicationApproved(self, event: BaseEvent) -> None:
        self._transition(ApplicationState.APPROVED)

    def on_ApplicationDeclined(self, event: BaseEvent) -> None:
        # Compliance declines are terminal but distinct for reporting.
        if self.state in (ApplicationState.COMPLIANCE_CHECK_REQUESTED, ApplicationState.COMPLIANCE_CHECK_COMPLETE):
            self.state = ApplicationState.DECLINED_COMPLIANCE
        else:
            self.state = ApplicationState.DECLINED

    @classmethod
    def rebuild(cls, events: Iterable[BaseEvent] | Iterable[StoredEvent]) -> "LoanApplication":
        events_list = list(events)
        if not events_list:
            raise ValueError("cannot rebuild LoanApplication from empty event list")

        if isinstance(events_list[0], StoredEvent):
            first = deserialize_event(events_list[0].event_type, events_list[0].payload).to_payload()
            app_id = str(first.get("application_id") or "")
            if not app_id:
                raise ValueError("first loan event must include application_id")
            agg = cls(application_id=app_id)
            for se in events_list:
                agg.apply_stored(se)
            return agg

        first = events_list[0].to_payload()  # type: ignore[union-attr]
        app_id = str(first.get("application_id") or "")
        if not app_id:
            raise ValueError("first loan event must include application_id")
        agg = cls(application_id=app_id)
        for e in events_list:  # type: ignore[assignment]
            agg.apply(e)  # type: ignore[arg-type]
        return agg
    
    @classmethod
    def load(cls, event_fetcher: Callable[[str], Iterable[StoredEvent]], application_id: str) -> "LoanApplication":
        """
        Load a LoanApplication from the event store.
        """
        events = list(event_fetcher(application_id))
        if not events:
            raise ValueError(f"No events found for LoanApplication {application_id}")
        return cls.rebuild(events)