from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import asyncpg


async def project_application_summary(conn: asyncpg.Connection, event: dict[str, Any]) -> None:
    et = str(event.get("event_type"))
    p = dict(event.get("payload") or {})
    gp = int(event["global_position"])
    app_id = p.get("application_id")
    if not app_id:
        return

    # Pull commonly used fields
    applicant_id = p.get("applicant_id")
    requested_amount = p.get("requested_amount_usd")
    loan_purpose = p.get("loan_purpose")

    if et == "ApplicationSubmitted":
        await conn.execute(
            """
            INSERT INTO application_summary(
                application_id, applicant_id, state, requested_amount_usd, loan_purpose, last_global_position, updated_at
            )
            VALUES ($1,$2,'SUBMITTED',$3,$4,$5,clock_timestamp())
            ON CONFLICT (application_id) DO UPDATE
              SET applicant_id=EXCLUDED.applicant_id,
                  state='SUBMITTED',
                  requested_amount_usd=EXCLUDED.requested_amount_usd,
                  loan_purpose=EXCLUDED.loan_purpose,
                  last_global_position=GREATEST(application_summary.last_global_position, EXCLUDED.last_global_position),
                  updated_at=clock_timestamp()
            """,
            str(app_id),
            str(applicant_id) if applicant_id is not None else None,
            requested_amount,
            str(loan_purpose) if loan_purpose is not None else None,
            gp,
        )
        return

    # State updates from later lifecycle events
    state_map = {
        "DocumentUploadRequested": "DOCUMENTS_PENDING",
        "DocumentUploaded": "DOCUMENTS_UPLOADED",
        "PackageReadyForAnalysis": "DOCUMENTS_PROCESSED",
        "CreditAnalysisRequested": "CREDIT_ANALYSIS_REQUESTED",
        "CreditAnalysisCompleted": "CREDIT_ANALYSIS_COMPLETE",
        "FraudScreeningRequested": "FRAUD_SCREENING_REQUESTED",
        "FraudScreeningCompleted": "FRAUD_SCREENING_COMPLETE",
        "ComplianceCheckRequested": "COMPLIANCE_CHECK_REQUESTED",
        "ComplianceCheckCompleted": "COMPLIANCE_CHECK_COMPLETE",
        "DecisionRequested": "PENDING_DECISION",
        "ApplicationApproved": "APPROVED",
        "ApplicationDeclined": "DECLINED",
        "HumanReviewRequested": "PENDING_HUMAN_REVIEW",
    }
    if et in state_map:
        await conn.execute(
            """
            INSERT INTO application_summary(application_id, state, last_global_position, updated_at)
            VALUES ($1,$2,$3,clock_timestamp())
            ON CONFLICT (application_id) DO UPDATE
              SET state=EXCLUDED.state,
                  last_global_position=GREATEST(application_summary.last_global_position, EXCLUDED.last_global_position),
                  updated_at=clock_timestamp()
            """,
            str(app_id),
            state_map[et],
            gp,
        )
        return

    if et == "DecisionGenerated":
        await conn.execute(
            """
            INSERT INTO application_summary(
                application_id, last_decision_recommendation, last_decision_confidence, last_global_position, updated_at
            )
            VALUES ($1,$2,$3,$4,clock_timestamp())
            ON CONFLICT (application_id) DO UPDATE
              SET last_decision_recommendation=EXCLUDED.last_decision_recommendation,
                  last_decision_confidence=EXCLUDED.last_decision_confidence,
                  last_global_position=GREATEST(application_summary.last_global_position, EXCLUDED.last_global_position),
                  updated_at=clock_timestamp()
            """,
            str(app_id),
            str(p.get("recommendation") or ""),
            float(p.get("confidence") or 0.0),
            gp,
        )


async def project_compliance_audit(conn: asyncpg.Connection, event: dict[str, Any]) -> None:
    et = str(event.get("event_type"))
    p = dict(event.get("payload") or {})
    gp = int(event["global_position"])
    app_id = p.get("application_id")
    if not app_id:
        return

    if et == "ComplianceRuleFailed":
        rule_id = str(p.get("rule_id") or "")
        await conn.execute(
            """
            INSERT INTO compliance_audit(application_id, failed_rule_ids, last_global_position, updated_at)
            VALUES ($1, ARRAY[$2]::TEXT[], $3, clock_timestamp())
            ON CONFLICT (application_id) DO UPDATE
              SET failed_rule_ids = (
                    SELECT ARRAY(SELECT DISTINCT unnest(compliance_audit.failed_rule_ids || EXCLUDED.failed_rule_ids))
                ),
                  last_global_position=GREATEST(compliance_audit.last_global_position, EXCLUDED.last_global_position),
                  updated_at=clock_timestamp()
            """,
            str(app_id),
            rule_id,
            gp,
        )
        return

    if et == "ComplianceCheckCompleted":
        await conn.execute(
            """
            INSERT INTO compliance_audit(
                application_id, overall_verdict, has_hard_block,
                rules_passed, rules_failed, rules_noted,
                last_global_position, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,clock_timestamp())
            ON CONFLICT (application_id) DO UPDATE
              SET overall_verdict=EXCLUDED.overall_verdict,
                  has_hard_block=EXCLUDED.has_hard_block,
                  rules_passed=EXCLUDED.rules_passed,
                  rules_failed=EXCLUDED.rules_failed,
                  rules_noted=EXCLUDED.rules_noted,
                  last_global_position=GREATEST(compliance_audit.last_global_position, EXCLUDED.last_global_position),
                  updated_at=clock_timestamp()
            """,
            str(app_id),
            str(p.get("overall_verdict") or ""),
            bool(p.get("has_hard_block") or False),
            int(p.get("rules_passed") or 0),
            int(p.get("rules_failed") or 0),
            int(p.get("rules_noted") or 0),
            gp,
        )


async def project_agent_trace(conn: asyncpg.Connection, event: dict[str, Any]) -> None:
    et = str(event.get("event_type"))
    p = dict(event.get("payload") or {})
    gp = int(event["global_position"])
    session_id = p.get("session_id")
    agent_type = p.get("agent_type")
    app_id = p.get("application_id")

    if et == "AgentSessionStarted":
        if not session_id or not agent_type or not app_id:
            return
        await conn.execute(
            """
            INSERT INTO agent_trace(
                session_id, agent_type, application_id, context_source, model_version, started_at, last_global_position, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,clock_timestamp())
            ON CONFLICT (session_id) DO UPDATE
              SET context_source=EXCLUDED.context_source,
                  model_version=EXCLUDED.model_version,
                  started_at=EXCLUDED.started_at,
                  last_global_position=GREATEST(agent_trace.last_global_position, EXCLUDED.last_global_position),
                  updated_at=clock_timestamp()
            """,
            str(session_id),
            str(agent_type),
            str(app_id),
            str(p.get("context_source") or ""),
            str(p.get("model_version") or ""),
            p.get("started_at"),
            gp,
        )
        return

    if et == "AgentSessionRecovered":
        if not session_id or not app_id:
            return
        await conn.execute(
            """
            UPDATE agent_trace
            SET recovered_from=$2,
                last_global_position=GREATEST(last_global_position, $3),
                updated_at=clock_timestamp()
            WHERE session_id=$1
            """,
            str(session_id),
            str(p.get("recovered_from_session_id") or ""),
            gp,
        )
        return

    if et == "AgentNodeExecuted":
        if not session_id:
            return
        await conn.execute(
            """
            UPDATE agent_trace
            SET nodes_executed = nodes_executed || $2::jsonb,
                total_nodes_executed = GREATEST(total_nodes_executed, $3),
                last_global_position=GREATEST(last_global_position, $4),
                updated_at=clock_timestamp()
            WHERE session_id=$1
            """,
            str(session_id),
            [p],
            int(p.get("node_sequence") or 0),
            gp,
        )
        return

    if et == "AgentToolCalled":
        if not session_id:
            return
        await conn.execute(
            """
            UPDATE agent_trace
            SET tool_calls = tool_calls || $2::jsonb,
                last_global_position=GREATEST(last_global_position, $3),
                updated_at=clock_timestamp()
            WHERE session_id=$1
            """,
            str(session_id),
            [p],
            gp,
        )
        return

    if et == "AgentSessionCompleted":
        if not session_id:
            return
        await conn.execute(
            """
            UPDATE agent_trace
            SET completed_at=$2,
                total_nodes_executed=$3,
                total_llm_calls=$4,
                total_tokens_used=$5,
                total_cost_usd=$6,
                total_duration_ms=$7,
                last_global_position=GREATEST(last_global_position, $8),
                updated_at=clock_timestamp()
            WHERE session_id=$1
            """,
            str(session_id),
            p.get("completed_at"),
            int(p.get("total_nodes_executed") or 0),
            int(p.get("total_llm_calls") or 0),
            int(p.get("total_tokens_used") or 0),
            float(p.get("total_cost_usd") or 0.0),
            int(p.get("total_duration_ms") or 0),
            gp,
        )
        return

    if et == "AgentSessionFailed":
        if not session_id:
            return
        await conn.execute(
            """
            UPDATE agent_trace
            SET failed_at=$2,
                last_global_position=GREATEST(last_global_position, $3),
                updated_at=clock_timestamp()
            WHERE session_id=$1
            """,
            str(session_id),
            p.get("failed_at"),
            gp,
        )

