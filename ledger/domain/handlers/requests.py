"""
ledger/domain/handlers/requests.py

MCP tools that trigger work are modeled as command handlers producing request events.
No business rules live here; all validation is via aggregate guards.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ledger.domain.aggregates.loan_application import LoanApplication
from ledger.schema.events import ComplianceCheckRequested, CreditAnalysisRequested, StoredEvent


async def _load_stored(store: Any, stream_id: str) -> list[StoredEvent]:
    rows = await store.load_stream(stream_id, from_position=0)
    return [StoredEvent.from_row(r) for r in rows]


@dataclass(frozen=True, slots=True)
class RequestCreditAnalysisCommand:
    correlation_id: str
    causation_id: str
    application_id: str
    requested_by: str


async def handle_request_credit_analysis(store: Any, cmd: RequestCreditAnalysisCommand) -> list[int]:
    loan_stream = f"loan-{cmd.application_id}"

    # 1) load
    loan_events = await _load_stored(store, loan_stream)
    if not loan_events:
        raise ValueError(f"missing stream: {loan_stream}")
    loan = LoanApplication.rebuild(loan_events)

    # 2) validate via guards
    loan.guard_can_request_credit_analysis()

    # 3) events only
    evt = CreditAnalysisRequested(
        application_id=cmd.application_id,
        requested_at=datetime.now(timezone.utc),
        requested_by=str(cmd.requested_by),
        priority="NORMAL",
    ).to_store_dict()

    # 4) append with tracked version
    return await store.append(
        stream_id=loan_stream,
        events=[evt],
        expected_version=loan.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


@dataclass(frozen=True, slots=True)
class RequestComplianceCheckCommand:
    correlation_id: str
    causation_id: str
    application_id: str
    triggered_by_event_id: str


async def handle_request_compliance_check(store: Any, cmd: RequestComplianceCheckCommand) -> list[int]:
    loan_stream = f"loan-{cmd.application_id}"

    # 1) load
    loan_events = await _load_stored(store, loan_stream)
    if not loan_events:
        raise ValueError(f"missing stream: {loan_stream}")
    loan = LoanApplication.rebuild(loan_events)

    # 2) validate via guards
    loan.guard_can_request_compliance()

    # 3) events only
    evt = ComplianceCheckRequested(
        application_id=cmd.application_id,
        requested_at=datetime.now(timezone.utc),
        triggered_by_event_id=str(cmd.triggered_by_event_id),
        regulation_set_version="2026-Q1",
        rules_to_evaluate=[f"REG-00{i}" for i in range(1, 7)],
    ).to_store_dict()

    # 4) append
    return await store.append(
        stream_id=loan_stream,
        events=[evt],
        expected_version=loan.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
