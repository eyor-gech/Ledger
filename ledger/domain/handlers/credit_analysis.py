"""
ledger/domain/handlers/credit_analysis.py

Command handler pattern (Phase 1):
  1) load aggregates
  2) validate using aggregate guards only
  3) produce events only (no side effects)
  4) append with aggregate-tracked version + correlation/causation ids
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ledger.domain.aggregates.agent_session import AgentSession
from ledger.domain.aggregates.credit_record import CreditRecord
from ledger.domain.aggregates.loan_application import LoanApplication
from ledger.schema.events import (
    CreditAnalysisCompleted,
    CreditDecision,
    StoredEvent,
)


@dataclass(frozen=True, slots=True)
class CreditAnalysisCompletedCommand:
    correlation_id: str
    causation_id: str
    application_id: str
    session_id: str
    agent_stream_id: str
    decision: CreditDecision
    model_version: str
    model_deployment_id: str
    input_data_hash: str
    analysis_duration_ms: int
    regulatory_basis: list[str]


async def _load_stored(store: Any, stream_id: str) -> list[StoredEvent]:
    rows = await store.load_stream(stream_id, from_position=0)
    return [StoredEvent.from_row(r) for r in rows]


async def handle_credit_analysis_completed(store: Any, cmd: CreditAnalysisCompletedCommand) -> list[int]:
    loan_stream = f"loan-{cmd.application_id}"
    agent_stream = cmd.agent_stream_id
    credit_stream = f"credit-{cmd.application_id}"

    # 1) load
    loan_events = await _load_stored(store, loan_stream)
    if not loan_events:
        raise ValueError(f"missing stream: {loan_stream}")
    loan = LoanApplication.rebuild(loan_events)

    agent_events = await _load_stored(store, agent_stream)
    if not agent_events:
        raise ValueError(f"missing stream: {agent_stream}")
    session = AgentSession.rebuild(agent_events)

    credit_events = await _load_stored(store, credit_stream)
    if not credit_events:
        raise ValueError(f"missing stream: {credit_stream}")
    credit = CreditRecord.rebuild(credit_events)

    # 2) validate (aggregate guards only)
    loan.guard_can_accept_credit_analysis_result()
    session.guard_can_write_output(cmd.model_version)

    # 3) events only
    evt = CreditAnalysisCompleted(
        application_id=cmd.application_id,
        session_id=cmd.session_id,
        decision=cmd.decision,
        model_version=cmd.model_version,
        model_deployment_id=cmd.model_deployment_id,
        model_versions={"credit_analysis": cmd.model_version},
        input_data_hash=cmd.input_data_hash,
        analysis_duration_ms=int(cmd.analysis_duration_ms),
        regulatory_basis=list(cmd.regulatory_basis),
        completed_at=datetime.now(timezone.utc),
    ).to_store_dict()

    # 4) append using aggregate-tracked version
    return await store.append(
        stream_id=credit_stream,
        events=[evt],
        expected_version=credit.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
