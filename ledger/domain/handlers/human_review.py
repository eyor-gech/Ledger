"""
ledger/domain/handlers/human_review.py
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from ledger.domain.aggregates.loan_application import LoanApplication
from ledger.schema.events import (
    ApplicationApproved,
    ApplicationDeclined,
    HumanReviewCompleted,
    StoredEvent,
)


async def _load_stored(store: Any, stream_id: str) -> list[StoredEvent]:
    rows = await store.load_stream(stream_id, from_position=0)
    return [StoredEvent.from_row(r) for r in rows]


@dataclass(frozen=True, slots=True)
class HumanReviewCompletedCommand:
    correlation_id: str
    causation_id: str
    application_id: str
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str  # APPROVE|DECLINE
    override_reason: str | None = None
    approved_amount_usd: Decimal | None = None


async def handle_human_review_completed(store: Any, cmd: HumanReviewCompletedCommand) -> list[int]:
    loan_stream = f"loan-{cmd.application_id}"

    # 1) load
    loan_events = await _load_stored(store, loan_stream)
    if not loan_events:
        raise ValueError(f"missing stream: {loan_stream}")
    loan = LoanApplication.rebuild(loan_events)

    # 2) validate via guards
    loan.guard_can_complete_human_review()

    # 3) events only
    events: list[dict[str, Any]] = [
        HumanReviewCompleted(
            application_id=cmd.application_id,
            reviewer_id=cmd.reviewer_id,
            override=bool(cmd.override),
            original_recommendation=str(cmd.original_recommendation),
            final_decision=str(cmd.final_decision),
            override_reason=cmd.override_reason,
            reviewed_at=datetime.now(timezone.utc),
        ).to_store_dict()
    ]
    if str(cmd.final_decision) == "APPROVE":
        if cmd.approved_amount_usd is None:
            raise ValueError("approved_amount_usd required for APPROVE")
        events.append(
            ApplicationApproved(
                application_id=cmd.application_id,
                approved_amount_usd=cmd.approved_amount_usd,
                interest_rate_pct=8.0,
                term_months=36,
                conditions=["Monthly financial reporting required", "Personal guarantee from principal"],
                approved_by=str(cmd.reviewer_id),
                effective_date=datetime.now(timezone.utc).date().isoformat(),
                approved_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )
    else:
        events.append(
            ApplicationDeclined(
                application_id=cmd.application_id,
                decline_reasons=["Human review final decision"],
                declined_by=str(cmd.reviewer_id),
                adverse_action_notice_required=True,
                adverse_action_codes=["HUMAN_DECLINE"],
                declined_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

    # 4) append with tracked version
    return await store.append(
        stream_id=loan_stream,
        events=events,
        expected_version=loan.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )

