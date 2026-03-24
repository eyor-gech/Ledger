from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from langgraph.graph import END, StateGraph

from ledger.agents.base_agent import BaseApexAgent, RecoveryInfo
from ledger.schema.events import (
    ApplicationApproved,
    ApplicationDeclined,
    DecisionGenerated,
    HumanReviewRequested,
)


class DecisionOrchestratorAgent(BaseApexAgent):
    agent_node_order = {
        "validate_inputs": 1,
        "open_aggregate_record": 2,
        "load_external_data": 3,
        "synthesize_decision": 4,
        "apply_hard_constraints": 5,
        "write_output": 6,
    }

    def build_graph(self) -> Any:
        g = StateGraph(dict)
        g.add_node("validate_inputs", self._node_validate_inputs)
        g.add_node("open_aggregate_record", self._node_open)
        g.add_node("load_external_data", self._node_load)
        g.add_node("synthesize_decision", self._node_synthesize)
        g.add_node("apply_hard_constraints", self._node_constraints)
        g.add_node("write_output", self._node_write_output)
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs", "open_aggregate_record")
        g.add_edge("open_aggregate_record", "load_external_data")
        g.add_edge("load_external_data", "synthesize_decision")
        g.add_edge("synthesize_decision", "apply_hard_constraints")
        g.add_edge("apply_hard_constraints", "write_output")
        g.add_edge("write_output", END)
        return g.compile()

    def _skip(self, state: dict, node_name: str) -> bool:
        recovery: RecoveryInfo | None = state.get("recovery")
        if recovery is None:
            return False
        return int(recovery.last_successful_node_sequence) >= int(self.agent_node_order[node_name])

    async def _node_validate_inputs(self, state: dict) -> dict:
        if self._skip(state, "validate_inputs"):
            return state
        t0 = time.time()
        app_id = state["application_id"]
        comp_events = await self.store.load_stream(f"compliance-{app_id}", from_position=0)
        if not any(e.get("event_type") == "ComplianceCheckCompleted" for e in comp_events):
            raise ValueError("ComplianceCheckCompleted required before decision")
        await self.record_input_validated(["application_id", "compliance_complete"], int((time.time() - t0) * 1000))
        await self.record_node_execution("validate_inputs", ["compliance_stream"], ["ready"], int((time.time() - t0) * 1000))
        return state

    async def _node_open(self, state: dict) -> dict:
        if self._skip(state, "open_aggregate_record"):
            return state
        t0 = time.time()
        await self.record_node_execution("open_aggregate_record", ["application_id"], ["ready"], int((time.time() - t0) * 1000))
        return state

    async def _node_load(self, state: dict) -> dict:
        if self._skip(state, "load_external_data"):
            return state
        t0 = time.time()
        app_id = state["application_id"]
        credit_events = await self.store.load_stream(f"credit-{app_id}", from_position=0)
        fraud_events = await self.store.load_stream(f"fraud-{app_id}", from_position=0)
        comp_events = await self.store.load_stream(f"compliance-{app_id}", from_position=0)
        credit = next((e for e in reversed(credit_events) if e.get("event_type") == "CreditAnalysisCompleted"), None)
        fraud = next((e for e in reversed(fraud_events) if e.get("event_type") == "FraudScreeningCompleted"), None)
        comp = next((e for e in reversed(comp_events) if e.get("event_type") == "ComplianceCheckCompleted"), None)

        await self.record_tool_call(
            "load_all_analyses",
            {"application_id": app_id},
            {"credit": bool(credit), "fraud": bool(fraud), "compliance": bool(comp)},
            int((time.time() - t0) * 1000),
        )
        await self.record_node_execution("load_external_data", ["credit", "fraud", "compliance"], ["analyses"], int((time.time() - t0) * 1000))
        return {**state, "credit": credit, "fraud": fraud, "compliance": comp}

    async def _node_synthesize(self, state: dict) -> dict:
        if self._skip(state, "synthesize_decision"):
            return state
        t0 = time.time()
        credit_p = (state.get("credit") or {}).get("payload") or {}
        decision = (credit_p.get("decision") or {}) if isinstance(credit_p.get("decision"), dict) else {}
        risk_tier = str(decision.get("risk_tier") or "MEDIUM")
        credit_conf = float(decision.get("confidence") or 0.0)
        recommended_limit = int(Decimal(str(decision.get("recommended_limit_usd") or 0)))

        fraud_p = (state.get("fraud") or {}).get("payload") or {}
        fraud_score = float(fraud_p.get("fraud_score") or 0.0)
        fraud_rec = str(fraud_p.get("recommendation") or "PROCEED")

        comp_p = (state.get("compliance") or {}).get("payload") or {}
        comp_verdict = str(comp_p.get("overall_verdict") or "UNKNOWN")
        comp_hard = bool(comp_p.get("has_hard_block") or False)

        # Base recommendation
        recommendation = "APPROVE"
        key_risks: list[str] = []
        if comp_hard or comp_verdict == "BLOCKED":
            recommendation = "DECLINE"
            key_risks.append("Compliance hard block")
        elif fraud_rec == "DECLINE" or fraud_score > 0.60:
            recommendation = "DECLINE"
            key_risks.append("Fraud risk elevated")
        elif risk_tier == "HIGH":
            recommendation = "DECLINE"
            key_risks.append("Credit risk tier HIGH")

        # Confidence floor business rule: confidence < 0.60 -> REFER
        confidence = max(0.0, min(1.0, credit_conf))
        if confidence < 0.60:
            recommendation = "REFER"
            key_risks.append("Confidence below 0.60")

        summary = (
            f"Recommendation {recommendation}. Credit {risk_tier} (conf={confidence:.2f}), "
            f"fraud_score={fraud_score:.2f}, compliance={comp_verdict}."
        )

        await self.record_node_execution("synthesize_decision", ["analyses"], ["draft_decision"], int((time.time() - t0) * 1000))
        return {
            **state,
            "draft_decision": {
                "recommendation": recommendation,
                "confidence": confidence,
                "approved_amount_usd": recommended_limit if recommendation == "APPROVE" else None,
                "conditions": ["Monthly financial reporting required", "Personal guarantee from principal"] if recommendation == "APPROVE" else [],
                "executive_summary": summary,
                "key_risks": key_risks,
            },
        }

    async def _node_constraints(self, state: dict) -> dict:
        if self._skip(state, "apply_hard_constraints"):
            return state
        t0 = time.time()
        d = dict(state.get("draft_decision") or {})
        # Compliance required before approval: double-lock
        comp_p = (state.get("compliance") or {}).get("payload") or {}
        if d.get("recommendation") == "APPROVE" and str(comp_p.get("overall_verdict") or "") not in {"CLEAR", "CONDITIONAL"}:
            d["recommendation"] = "REFER"
            d["key_risks"] = list(d.get("key_risks") or []) + ["Compliance not clear"]
        await self.record_node_execution("apply_hard_constraints", ["draft_decision"], ["final_decision"], int((time.time() - t0) * 1000))
        return {**state, "final_decision": d}

    async def _node_write_output(self, state: dict) -> dict:
        if self._skip(state, "write_output"):
            return state
        t0 = time.time()
        app_id = state["application_id"]
        loan_stream = f"loan-{app_id}"

        existing = await self.store.load_stream(loan_stream, from_position=0)
        if any(e.get("event_type") == "DecisionGenerated" for e in existing):
            await self.record_output_written([], "Decision already generated; no-op.")
            await self.record_node_execution("write_output", ["loan_stream"], ["noop"], int((time.time() - t0) * 1000))
            return {**state, "next_agent_triggered": None}

        d = dict(state.get("final_decision") or {})
        rec = str(d.get("recommendation") or "REFER")
        conf = float(d.get("confidence") or 0.0)
        approved_amt = Decimal(str(int(d.get("approved_amount_usd") or 0))) if rec == "APPROVE" else None

        await self.append_events_with_occ_retry(
            loan_stream,
            [
                DecisionGenerated(
                    application_id=app_id,
                    orchestrator_session_id=self.session_id,
                    recommendation=rec,
                    confidence=conf,
                    approved_amount_usd=approved_amt,
                    conditions=list(d.get("conditions") or []),
                    executive_summary=str(d.get("executive_summary") or ""),
                    key_risks=list(d.get("key_risks") or []),
                    contributing_sessions=[],
                    model_versions={"orchestrator": self.model_version},
                    generated_at=datetime.now(timezone.utc),
                ).to_store_dict()
            ],
            correlation_id=app_id,
            causation_id=self.session_id,
            idempotency_key=f"{app_id}:DecisionGenerated",
        )

        followup_type: str
        followup_event: dict[str, Any]
        compliance_payload = (state.get("compliance") or {}).get("payload") or {}
        compliance_hard_block = bool(compliance_payload.get("has_hard_block") or False) or str(compliance_payload.get("overall_verdict") or "") == "BLOCKED"

        if rec == "APPROVE" and approved_amt is not None:
            followup_event = ApplicationApproved(
                application_id=app_id,
                approved_amount_usd=approved_amt,
                interest_rate_pct=8.5,
                term_months=36,
                conditions=["Monthly financial reporting required"],
                approved_by="decision_orchestrator",
                effective_date=datetime.now(timezone.utc).date().isoformat(),
                approved_at=datetime.now(timezone.utc),
            ).to_store_dict()
            followup_type = "ApplicationApproved"
        elif rec == "DECLINE" and compliance_hard_block:
            followup_event = ApplicationDeclined(
                application_id=app_id,
                decline_reasons=list(d.get("key_risks") or ["Risk constraints"]),
                declined_by="decision_orchestrator",
                adverse_action_notice_required=True,
                adverse_action_codes=["POLICY_DECLINE"],
                declined_at=datetime.now(timezone.utc),
            ).to_store_dict()
            followup_type = "ApplicationDeclined"
        else:
            followup_event = HumanReviewRequested(
                application_id=app_id,
                reason="Human review required (overrideable decision)",
                decision_event_id=self._sha(self.session_id),
                assigned_to=None,
                requested_at=datetime.now(timezone.utc),
            ).to_store_dict()
            followup_type = "HumanReviewRequested"

        await self.append_events_with_occ_retry(
            loan_stream,
            [followup_event],
            correlation_id=app_id,
            causation_id=self.session_id,
            idempotency_key=f"{app_id}:{followup_type}",
        )

        events_written = [{"stream_id": loan_stream, "event_type": "DecisionGenerated"}, {"stream_id": loan_stream, "event_type": followup_type}]
        await self.record_output_written(events_written, f"Decision: {rec} (confidence={conf:.2f}).")
        await self.record_node_execution("write_output", ["final_decision"], ["events_written"], int((time.time() - t0) * 1000))
        return {**state, "next_agent_triggered": None, "events_written": events_written}
