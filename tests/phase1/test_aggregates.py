"""
tests/phase1/test_aggregates.py

Deterministic aggregate rebuild + key invariants.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from ledger.domain.aggregates.agent_session import AgentSession
from ledger.domain.aggregates.compliance_record import ComplianceRecord
from ledger.domain.aggregates.loan_application import LoanApplication
from ledger.domain.errors import InvariantViolation
from ledger.schema.events import (
    AgentNodeExecuted,
    AgentSessionStarted,
    AgentType,
    ApplicationSubmitted,
    ComplianceCheckCompleted,
    ComplianceCheckInitiated,
    ComplianceRuleFailed,
    ComplianceVerdict,
    DecisionGenerated,
    LoanPurpose,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_loan_application_confidence_floor_enforced():
    evts = [
        ApplicationSubmitted(
            application_id="APEX-1",
            applicant_id="COMP-1",
            requested_amount_usd=Decimal("1000"),
            loan_purpose=LoanPurpose.WORKING_CAPITAL,
            loan_term_months=12,
            submission_channel="web",
            contact_email="a@b.com",
            contact_name="A",
            submitted_at=_now(),
            application_reference="ref",
        ),
        DecisionGenerated(
            application_id="APEX-1",
            orchestrator_session_id="sess-1",
            recommendation="APPROVE",
            confidence=0.59,
            approved_amount_usd=Decimal("1000"),
            conditions=[],
            executive_summary="x",
            key_risks=[],
            contributing_sessions=[],
            model_versions={},
            generated_at=_now(),
        ),
    ]
    agg = LoanApplication.rebuild([evts[0]])
    with pytest.raises(InvariantViolation):
        agg.apply(evts[1])


def test_compliance_hard_block_requires_blocked_verdict():
    evts = [
        ComplianceCheckInitiated(
            application_id="APEX-2",
            session_id="sess-2",
            regulation_set_version="REGSET-1",
            rules_to_evaluate=["REG-003"],
            initiated_at=_now(),
        ),
        ComplianceRuleFailed(
            application_id="APEX-2",
            session_id="sess-2",
            rule_id="REG-003",
            rule_name="Jurisdiction",
            rule_version="1",
            failure_reason="MT",
            is_hard_block=True,
            remediation_available=False,
            remediation_description=None,
            evidence_hash="x",
            evaluated_at=_now(),
        ),
    ]
    agg = ComplianceRecord.rebuild(evts)
    with pytest.raises(InvariantViolation):
        agg.apply(
            ComplianceCheckCompleted(
                application_id="APEX-2",
                session_id="sess-2",
                rules_evaluated=1,
                rules_passed=0,
                rules_failed=1,
                rules_noted=0,
                has_hard_block=True,
                overall_verdict=ComplianceVerdict.CLEAR,
                completed_at=_now(),
            )
        )


def test_agent_session_node_sequence_must_be_contiguous():
    started = AgentSessionStarted(
        session_id="sess-3",
        agent_type=AgentType.CREDIT_ANALYSIS,
        agent_id="agent-1",
        application_id="APEX-3",
        model_version="m",
        langgraph_graph_version="g",
        context_source="fresh",
        context_token_count=0,
        started_at=_now(),
    )
    agg = AgentSession.rebuild([started])
    agg.apply(
        AgentNodeExecuted(
            session_id="sess-3",
            agent_type=AgentType.CREDIT_ANALYSIS,
            node_name="n1",
            node_sequence=1,
            input_keys=[],
            output_keys=[],
            llm_called=False,
            llm_tokens_input=None,
            llm_tokens_output=None,
            llm_cost_usd=None,
            duration_ms=1,
            executed_at=_now(),
        )
    )
    with pytest.raises(InvariantViolation):
        agg.apply(
            AgentNodeExecuted(
                session_id="sess-3",
                agent_type=AgentType.CREDIT_ANALYSIS,
                node_name="n3",
                node_sequence=3,
                input_keys=[],
                output_keys=[],
                llm_called=False,
                llm_tokens_input=None,
                llm_tokens_output=None,
                llm_cost_usd=None,
                duration_ms=1,
                executed_at=_now(),
            )
        )
