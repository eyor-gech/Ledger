# tests/phase2/test_domain.py - Fixed production-ready test suite
import pytest
from uuid import uuid4
from datetime import datetime, timezone
from decimal import Decimal

from ledger.domain.handlers.credit_analysis import (
    CreditAnalysisCompletedCommand,
    handle_credit_analysis_completed,
)
from ledger.domain.errors import InvariantViolation  # ✅ Added import
from ledger.event_store import InMemoryEventStore


@pytest.mark.anyio
async def test_credit_analysis_completed_appends_event():
    """Verify handler processes CreditAnalysisCompletedCommand in valid CREDIT_ANALYSIS_REQUESTED state."""
    store = InMemoryEventStore()
    now = datetime.now(timezone.utc).isoformat()
    application_id = str(uuid4())
    agent_session_id = str(uuid4())
    agent_stream_id = f"agent-{agent_session_id}"
    credit_stream_id = f"credit-{application_id}"

    # ✅ EXACT state flow from loan_application.py: SUBMITTED → DOCUMENTS_PROCESSED → CREDIT_ANALYSIS_REQUESTED
    await store.append(
        stream_id=f"loan-{application_id}",
        events=[
            # 1. SUBMITTED
            {
                "event_type": "ApplicationSubmitted",
                "payload": {
                    "application_id": application_id,
                    "applicant_id": "applicant-123",
                    "requested_amount_usd": 10000,
                    "loan_purpose": "working_capital",
                    "loan_term_months": 12,
                    "submission_channel": "ONLINE",
                    "contact_email": "test@example.com",
                    "contact_name": "Alice",
                    "submitted_at": now,
                    "application_reference": f"app-ref-{application_id}",
                },
            },
            # 2. Skip document events → directly to DOCUMENTS_PROCESSED via PackageReadyForAnalysis
            {
                "event_type": "PackageReadyForAnalysis",
                "payload": {
                    "package_id": f"pkg-{application_id}",
                    "application_id": application_id,
                    "documents_processed": 1,
                    "has_quality_flags": False,
                    "quality_flag_count": 0,
                    "ready_at": now,
                },
            },
            # 3. CREDIT_ANALYSIS_REQUESTED ✓ satisfies guard_can_accept_credit_analysis_result()
            {
                "event_type": "CreditAnalysisRequested",
                "payload": {
                    "application_id": application_id,
                    "requested_by": "SYSTEM",
                    "requested_at": now,
                },
            },
        ],
        expected_version=-1,
    )

    # Supporting streams
    await store.append(
        stream_id=agent_stream_id,
        events=[{"event_type": "AgentContextLoaded", "payload": {}}],
        expected_version=-1,
    )
    await store.append(stream_id=credit_stream_id, events=[], expected_version=-1)

    # ✅ CreditDecision schema from credit_record.py (no 'recommendation')
    decision = {
        "risk_tier": "LOW",
        "confidence": 0.85,
        "recommended_limit_usd": 10000,
    }

    cmd = CreditAnalysisCompletedCommand(
        correlation_id=str(uuid4()),
        causation_id=str(uuid4()),
        application_id=application_id,
        session_id=agent_session_id,
        agent_stream_id=agent_stream_id,
        decision=decision,
        model_version="v1.0",
        model_deployment_id="deployment-123",
        input_data_hash="hash123",
        analysis_duration_ms=1234,
        regulatory_basis=["FCRA", "ECOA"],
    )

    # Act
    result_positions = await handle_credit_analysis_completed(store, cmd)

    # Assert
    assert len(result_positions) == 1
    events = await store.load_stream(credit_stream_id)
    assert len(events) == 1
    
    event = events[0]
    assert event["event_type"] == "CreditAnalysisCompleted"
    payload = event["payload"]
    
    assert payload["application_id"] == application_id
    assert payload["model_version"] == "v1.0"
    assert payload["decision"]["risk_tier"] == "LOW"
    assert payload["decision"]["confidence"] == 0.85
    assert payload["decision"]["recommended_limit_usd"] == 10000


@pytest.mark.anyio
async def test_credit_analysis_completed_wrong_state_fails():
    """Verify handler rejects non-CREDIT_ANALYSIS_REQUESTED state."""
    store = InMemoryEventStore()
    application_id = str(uuid4())
    now = datetime.now(timezone.utc).isoformat()

    # Arrange: Wrong state - only SUBMITTED
    await store.append(
        stream_id=f"loan-{application_id}",
        events=[{
            "event_type": "ApplicationSubmitted",
            "payload": {
                "application_id": application_id,
                "applicant_id": "applicant-123",
                "requested_amount_usd": 10000,
                "loan_purpose": "working_capital",
                "loan_term_months": 12,
                "submission_channel": "ONLINE",
                "contact_email": "test@example.com",
                "contact_name": "Alice",
                "submitted_at": now,
                "application_reference": f"app-ref-{application_id}",
            },
        }],
        expected_version=-1,
    )

    cmd = CreditAnalysisCompletedCommand(
        correlation_id=str(uuid4()),
        causation_id=str(uuid4()),
        application_id=application_id,
        session_id=str(uuid4()),
        agent_stream_id=f"agent-{uuid4()}",
        decision={"risk_tier": "LOW", "confidence": 0.85, "recommended_limit_usd": 10000},
        model_version="v1.0",
        model_deployment_id="deployment-123",
        input_data_hash="hash123",
        analysis_duration_ms=1234,
        regulatory_basis=["FCRA"],
    )

    # Act & Assert: Should fail guard_can_accept_credit_analysis_result()
    with pytest.raises(InvariantViolation):
        await handle_credit_analysis_completed(store, cmd)
