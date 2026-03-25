from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(slots=True)
class InMemoryProjections:
    checkpoints: dict[str, int] = field(default_factory=dict)
    application_summary: dict[str, dict[str, Any]] = field(default_factory=dict)
    compliance_audit: dict[str, dict[str, Any]] = field(default_factory=dict)
    agent_trace: dict[str, dict[str, Any]] = field(default_factory=dict)

    def checkpoint(self, name: str) -> int:
        return int(self.checkpoints.get(name, 0))

    def save_checkpoint(self, name: str, pos: int) -> None:
        self.checkpoints[name] = int(pos)


def apply_application_summary(mem: InMemoryProjections, event: dict[str, Any]) -> None:
    et = str(event.get("event_type"))
    p = dict(event.get("payload") or {})
    gp = int(event["global_position"])
    app_id = p.get("application_id")
    if not app_id:
        return
    row = mem.application_summary.setdefault(str(app_id), {"application_id": str(app_id), "last_global_position": 0})
    if gp <= int(row.get("last_global_position") or 0):
        return

    if et == "ApplicationSubmitted":
        row.update(
            {
                "applicant_id": p.get("applicant_id"),
                "state": "SUBMITTED",
                "requested_amount_usd": p.get("requested_amount_usd"),
                "loan_purpose": p.get("loan_purpose"),
            }
        )
    elif et == "DecisionGenerated":
        row.update(
            {
                "last_decision_recommendation": p.get("recommendation"),
                "last_decision_confidence": p.get("confidence"),
            }
        )
    else:
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
            row["state"] = state_map[et]

    row["last_global_position"] = gp


def apply_compliance_audit(mem: InMemoryProjections, event: dict[str, Any]) -> None:
    et = str(event.get("event_type"))
    p = dict(event.get("payload") or {})
    gp = int(event["global_position"])
    app_id = p.get("application_id")
    if not app_id:
        return
    row = mem.compliance_audit.setdefault(str(app_id), {"application_id": str(app_id), "failed_rule_ids": [], "last_global_position": 0})
    if gp <= int(row.get("last_global_position") or 0) and et != "ComplianceRuleFailed":
        return

    if et == "ComplianceRuleFailed":
        rid = str(p.get("rule_id") or "")
        if rid and rid not in row["failed_rule_ids"]:
            row["failed_rule_ids"].append(rid)
        row["last_global_position"] = max(int(row.get("last_global_position") or 0), gp)
        return

    if et == "ComplianceCheckCompleted":
        row.update(
            {
                "overall_verdict": p.get("overall_verdict"),
                "has_hard_block": bool(p.get("has_hard_block") or False),
                "rules_passed": int(p.get("rules_passed") or 0),
                "rules_failed": int(p.get("rules_failed") or 0),
                "rules_noted": int(p.get("rules_noted") or 0),
                "last_global_position": gp,
            }
        )


def apply_agent_trace(mem: InMemoryProjections, event: dict[str, Any]) -> None:
    et = str(event.get("event_type"))
    p = dict(event.get("payload") or {})
    gp = int(event["global_position"])
    session_id = p.get("session_id")
    if not session_id:
        return
    row = mem.agent_trace.setdefault(str(session_id), {"session_id": str(session_id), "nodes_executed": [], "tool_calls": [], "last_global_position": 0})
    if gp <= int(row.get("last_global_position") or 0) and et not in {"AgentNodeExecuted", "AgentToolCalled"}:
        return

    if et == "AgentSessionStarted":
        row.update(
            {
                "agent_type": p.get("agent_type"),
                "application_id": p.get("application_id"),
                "context_source": p.get("context_source"),
                "model_version": p.get("model_version"),
                "started_at": p.get("started_at"),
            }
        )
    elif et == "AgentSessionRecovered":
        row["recovered_from"] = p.get("recovered_from_session_id")
    elif et == "AgentNodeExecuted":
        row["nodes_executed"].append(p)
        row["total_nodes_executed"] = max(int(row.get("total_nodes_executed") or 0), int(p.get("node_sequence") or 0))
    elif et == "AgentToolCalled":
        row["tool_calls"].append(p)
    elif et == "AgentSessionCompleted":
        row.update(
            {
                "completed_at": p.get("completed_at"),
                "total_nodes_executed": p.get("total_nodes_executed"),
                "total_llm_calls": p.get("total_llm_calls"),
                "total_tokens_used": p.get("total_tokens_used"),
                "total_cost_usd": p.get("total_cost_usd"),
                "total_duration_ms": p.get("total_duration_ms"),
            }
        )
    elif et == "AgentSessionFailed":
        row["failed_at"] = p.get("failed_at")

    row["last_global_position"] = max(int(row.get("last_global_position") or 0), gp)


async def run_in_memory_projection(
    *,
    store: Any,
    projection_name: str,
    apply_fn: Callable[[InMemoryProjections, dict[str, Any]], None],
    mem: InMemoryProjections,
) -> None:
    """
    Sequentially applies events in global_position order using the store's `load_all`.
    Intended for tests (no DB dependency).
    """
    start = mem.checkpoint(projection_name)

    async for ev in store.load_all(from_position=start, batch_size=200):
        gp = int(ev["global_position"])
        if gp <= mem.checkpoint(projection_name):
            continue
        apply_fn(mem, ev)
        mem.save_checkpoint(projection_name, gp)
