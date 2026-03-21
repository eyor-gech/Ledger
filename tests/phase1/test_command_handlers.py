"""
tests/phase1/test_command_handlers.py

Validates the command handler 4-step pattern and correlation/causation propagation.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from ledger.domain.errors import InvariantViolation
from ledger.domain.handlers import CreditAnalysisCompletedCommand, handle_credit_analysis_completed
from ledger.event_store import InMemoryEventStore
from ledger.schema.events import (
    AgentContextLoaded,
    AgentSessionStarted,
    AgentType,
    ApplicationSubmitted,
    CreditAnalysisRequested,
    CreditDecision,
    CreditRecordOpened,
    DocumentFormat,
    DocumentType,
    DocumentUploadRequested,
    DocumentUploaded,
    LoanPurpose,
    PackageReadyForAnalysis,
    RiskTier,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_credit_analysis_handler_appends_with_ids_and_expected_version():
    store = InMemoryEventStore()
    app_id = "APEX-0001"

    loan_stream = f"loan-{app_id}"
    credit_stream = f"credit-{app_id}"
    session_id = "sess-cre-0001"
    agent_stream = f"agent-credit_analysis-{session_id}"

    # Build loan state up to CREDIT_ANALYSIS_REQUESTED.
    await store.append(
        loan_stream,
        [
            ApplicationSubmitted(
                application_id=app_id,
                applicant_id="COMP-001",
                requested_amount_usd=Decimal("500000"),
                loan_purpose=LoanPurpose.WORKING_CAPITAL,
                loan_term_months=36,
                submission_channel="web",
                contact_email="x@y.com",
                contact_name="X",
                submitted_at=_now(),
                application_reference=app_id,
            ).to_store_dict(),
            DocumentUploadRequested(
                application_id=app_id,
                required_document_types=[DocumentType.APPLICATION_PROPOSAL],
                deadline=_now() + timedelta(days=7),
                requested_by="system",
            ).to_store_dict(),
            DocumentUploaded(
                application_id=app_id,
                document_id="doc-1",
                document_type=DocumentType.APPLICATION_PROPOSAL,
                document_format=DocumentFormat.PDF,
                filename="a.pdf",
                file_path="documents/a.pdf",
                file_size_bytes=123,
                file_hash="h",
                fiscal_year=None,
                uploaded_at=_now(),
                uploaded_by="applicant",
            ).to_store_dict(),
            # For Phase 1 we allow this marker to exist on loan stream to model doc processing completion.
            PackageReadyForAnalysis(
                package_id=app_id,
                application_id=app_id,
                documents_processed=1,
                has_quality_flags=False,
                quality_flag_count=0,
                ready_at=_now(),
            ).to_store_dict(),
            CreditAnalysisRequested(
                application_id=app_id,
                requested_at=_now(),
                requested_by="system",
                priority="NORMAL",
            ).to_store_dict(),
        ],
        expected_version=-1,
        correlation_id="corr-init",
        causation_id="cause-init",
    )

    # Build agent session (must include AgentContextLoaded for guard_can_write_output).
    await store.append(
        agent_stream,
        [
            AgentSessionStarted(
                session_id=session_id,
                agent_type=AgentType.CREDIT_ANALYSIS,
                agent_id="agent-1",
                application_id=app_id,
                model_version="model-x",
                langgraph_graph_version="g1",
                context_source="fresh",
                context_token_count=0,
                started_at=_now(),
            ).to_store_dict(),
            AgentContextLoaded(
                session_id=session_id,
                agent_type=AgentType.CREDIT_ANALYSIS,
                application_id=app_id,
                model_version="model-x",
                context_source="fresh",
                context_hash="ctx-hash",
                loaded_at=_now(),
            ).to_store_dict(),
        ],
        expected_version=-1,
    )

    # Credit record opened at version 0.
    await store.append(
        credit_stream,
        [
            CreditRecordOpened(
                application_id=app_id,
                applicant_id="COMP-001",
                opened_at=_now(),
            ).to_store_dict()
        ],
        expected_version=-1,
    )

    cmd = CreditAnalysisCompletedCommand(
        correlation_id="corr-1",
        causation_id="cause-1",
        application_id=app_id,
        session_id=session_id,
        agent_stream_id=agent_stream,
        decision=CreditDecision(
            risk_tier=RiskTier.LOW,
            recommended_limit_usd=Decimal("100000"),
            confidence=0.75,
            rationale="ok",
        ),
        model_version="model-x",
        model_deployment_id="dep-1",
        input_data_hash="h",
        analysis_duration_ms=10,
        regulatory_basis=[],
    )

    positions = await handle_credit_analysis_completed(store, cmd)
    assert positions == [1]
    assert await store.stream_version(credit_stream) == 1

    credit_events = await store.load_stream(credit_stream)
    last = credit_events[-1]
    assert last["event_type"] == "CreditAnalysisCompleted"
    assert last["metadata"]["correlation_id"] == "corr-1"
    assert last["metadata"]["causation_id"] == "cause-1"


@pytest.mark.asyncio
async def test_credit_analysis_handler_rejects_wrong_loan_state():
    store = InMemoryEventStore()
    app_id = "APEX-0002"

    loan_stream = f"loan-{app_id}"
    credit_stream = f"credit-{app_id}"
    session_id = "sess-cre-0002"
    agent_stream = f"agent-credit_analysis-{session_id}"

    await store.append(
        loan_stream,
        [
            ApplicationSubmitted(
                application_id=app_id,
                applicant_id="COMP-001",
                requested_amount_usd=Decimal("500000"),
                loan_purpose=LoanPurpose.WORKING_CAPITAL,
                loan_term_months=36,
                submission_channel="web",
                contact_email="x@y.com",
                contact_name="X",
                submitted_at=_now(),
                application_reference=app_id,
            ).to_store_dict(),
        ],
        expected_version=-1,
    )

    await store.append(
        agent_stream,
        [
            AgentSessionStarted(
                session_id=session_id,
                agent_type=AgentType.CREDIT_ANALYSIS,
                agent_id="agent-1",
                application_id=app_id,
                model_version="model-x",
                langgraph_graph_version="g1",
                context_source="fresh",
                context_token_count=0,
                started_at=_now(),
            ).to_store_dict(),
            AgentContextLoaded(
                session_id=session_id,
                agent_type=AgentType.CREDIT_ANALYSIS,
                application_id=app_id,
                model_version="model-x",
                context_source="fresh",
                context_hash="ctx-hash",
                loaded_at=_now(),
            ).to_store_dict(),
        ],
        expected_version=-1,
    )

    await store.append(
        credit_stream,
        [
            CreditRecordOpened(
                application_id=app_id,
                applicant_id="COMP-001",
                opened_at=_now(),
            ).to_store_dict()
        ],
        expected_version=-1,
    )

    cmd = CreditAnalysisCompletedCommand(
        correlation_id="corr-1",
        causation_id="cause-1",
        application_id=app_id,
        session_id=session_id,
        agent_stream_id=agent_stream,
        decision=CreditDecision(
            risk_tier=RiskTier.LOW,
            recommended_limit_usd=Decimal("100000"),
            confidence=0.75,
            rationale="ok",
        ),
        model_version="model-x",
        model_deployment_id="dep-1",
        input_data_hash="h",
        analysis_duration_ms=10,
        regulatory_basis=[],
    )

    with pytest.raises(InvariantViolation):
        await handle_credit_analysis_completed(store, cmd)

