from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from langgraph.graph import END, StateGraph

from ledger.agents.base_agent import BaseApexAgent, RecoveryInfo
from ledger.schema.events import (
    ComplianceCheckRequested,
    FraudAnomaly,
    FraudAnomalyDetected,
    FraudAnomalyType,
    FraudScreeningCompleted,
    FraudScreeningInitiated,
)


class FraudDetectionAgent(BaseApexAgent):
    agent_node_order = {
        "validate_inputs": 1,
        "open_aggregate_record": 2,
        "load_external_data": 3,
        "analyze_fraud_patterns": 4,
        "write_output": 5,
    }

    def build_graph(self) -> Any:
        g = StateGraph(dict)
        g.add_node("validate_inputs", self._node_validate_inputs)
        g.add_node("open_aggregate_record", self._node_open_fraud_stream)
        g.add_node("load_external_data", self._node_load_external_data)
        g.add_node("analyze_fraud_patterns", self._node_analyze)
        g.add_node("write_output", self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs", "open_aggregate_record")
        g.add_edge("open_aggregate_record", "load_external_data")
        g.add_edge("load_external_data", "analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns", "write_output")
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
        credit_events = await self.store.load_stream(f"credit-{app_id}", from_position=0)
        if not any(e.get("event_type") == "CreditAnalysisCompleted" for e in credit_events):
            raise ValueError("CreditAnalysisCompleted required before fraud screening")

        await self.record_input_validated(["application_id", "credit_analysis_complete"], int((time.time() - t0) * 1000))
        await self.record_node_execution("validate_inputs", ["credit_stream"], ["ready"], int((time.time() - t0) * 1000))
        return state

    async def _node_open_fraud_stream(self, state: dict) -> dict:
        if self._skip(state, "open_aggregate_record"):
            return state
        t0 = time.time()
        app_id = state["application_id"]
        fraud_stream = f"fraud-{app_id}"
        existing = await self.store.load_stream(fraud_stream, from_position=0)
        if not any(e.get("event_type") == "FraudScreeningInitiated" for e in existing):
            await self.append_events_with_occ_retry(
                fraud_stream,
                [
                    FraudScreeningInitiated(
                        application_id=app_id,
                        session_id=self.session_id,
                        screening_model_version=self.model_version,
                        initiated_at=datetime.now(timezone.utc),
                    ).to_store_dict()
                ],
                correlation_id=app_id,
                causation_id=self.session_id,
                idempotency_key=f"{app_id}:FraudScreeningInitiated",
            )
        await self.record_node_execution("open_aggregate_record", ["application_id"], ["fraud_stream_ready"], int((time.time() - t0) * 1000))
        return state

    async def _node_load_external_data(self, state: dict) -> dict:
        if self._skip(state, "load_external_data"):
            return state
        t0 = time.time()
        app_id = state["application_id"]

        # Load extracted facts
        docpkg_events = await self.store.load_stream(f"docpkg-{app_id}", from_position=0)
        extracted = [e for e in docpkg_events if e.get("event_type") == "ExtractionCompleted"]
        merged: dict[str, Any] = {}
        for ev in extracted:
            facts = (ev.get("payload") or {}).get("facts") or {}
            if isinstance(facts, dict):
                for k, v in facts.items():
                    if v is not None and k not in merged:
                        merged[k] = v
        # Load registry prior year revenue
        loan_events = await self.store.load_stream(f"loan-{app_id}", from_position=0)
        submitted = next((e for e in loan_events if e.get("event_type") == "ApplicationSubmitted"), None)
        applicant_id = str((submitted or {}).get("payload", {}).get("applicant_id") or "")
        profile = await self.registry.get_company(applicant_id) if applicant_id else None
        hist = await self.registry.get_financial_history(applicant_id) if applicant_id else []

        await self.record_tool_call(
            "load_docpkg_and_registry",
            {"application_id": app_id, "company_id": applicant_id},
            {"facts_keys": len(merged), "hist_years": len(hist)},
            int((time.time() - t0) * 1000),
        )
        await self.record_node_execution("load_external_data", ["docpkg", "registry"], ["facts", "hist"], int((time.time() - t0) * 1000))
        return {
            **state,
            "facts": merged,
            "company_profile": self._as_dict(profile) if profile else None,
            "historical_financials": [h if isinstance(h, dict) else (getattr(h, "__dict__", None) or {}) for h in hist],
        }

    async def _node_analyze(self, state: dict) -> dict:
        if self._skip(state, "analyze_fraud_patterns"):
            return state
        t0 = time.time()
        facts = dict(state.get("facts") or {})
        hist = list(state.get("historical_financials") or [])
        trajectory = str((state.get("company_profile") or {}).get("trajectory") or "UNKNOWN")

        doc_rev = float(facts.get("total_revenue") or 0.0)
        prior_rev = float(hist[-1].get("total_revenue") or 0.0) if hist else 0.0
        anomalies: list[dict[str, Any]] = []

        fraud_score = 0.05
        if prior_rev > 0 and doc_rev > 0:
            gap = abs(doc_rev - prior_rev) / prior_rev
            if gap > 0.40 and trajectory not in ("GROWTH", "RECOVERING"):
                anomalies.append({"type": FraudAnomalyType.REVENUE_DISCREPANCY.value, "severity": "HIGH", "gap": round(gap, 3)})
                fraud_score += 0.25
            elif gap > 0.20:
                anomalies.append({"type": FraudAnomalyType.REVENUE_DISCREPANCY.value, "severity": "MEDIUM", "gap": round(gap, 3)})
                fraud_score += 0.12

        # Balance sheet internal check (if fields exist)
        if facts.get("total_assets") and facts.get("total_liabilities") and facts.get("total_equity"):
            lhs = float(facts["total_assets"])
            rhs = float(facts["total_liabilities"]) + float(facts["total_equity"])
            if abs(lhs - rhs) / max(lhs, 1.0) > 0.05:
                anomalies.append({"type": FraudAnomalyType.BALANCE_SHEET_INCONSISTENCY.value, "severity": "MEDIUM"})
                fraud_score += 0.10

        fraud_score = max(0.0, min(1.0, fraud_score))
        verdict = "PROCEED" if fraud_score < 0.30 else "FLAG_FOR_REVIEW" if fraud_score < 0.60 else "DECLINE"

        # Persist assessment in session log to support Gas Town recovery (resume without recompute).
        await self.record_tool_call(
            "fraud_assessment",
            {"doc_revenue": doc_rev, "prior_revenue": prior_rev, "trajectory": trajectory},
            {"fraud_score": fraud_score, "verdict": verdict, "anomalies": anomalies},
            int((time.time() - t0) * 1000),
        )

        await self.record_node_execution("analyze_fraud_patterns", ["facts", "hist"], ["fraud_assessment"], int((time.time() - t0) * 1000))
        return {**state, "fraud_score": fraud_score, "verdict": verdict, "anomalies": anomalies}

    async def _node_write_output(self, state: dict) -> dict:
        if self._skip(state, "write_output"):
            return state
        t0 = time.time()
        app_id = state["application_id"]
        fraud_stream = f"fraud-{app_id}"
        loan_stream = f"loan-{app_id}"

        existing = await self.store.load_stream(fraud_stream, from_position=0)
        if any(e.get("event_type") == "FraudScreeningCompleted" for e in existing):
            await self.record_output_written([], "Fraud screening already completed; no-op.")
            await self.record_node_execution("write_output", ["fraud_stream"], ["noop"], int((time.time() - t0) * 1000))
            return {**state, "next_agent_triggered": "compliance"}

        # Recovery: if state doesn't have assessment, load from prior session tool log.
        if (state.get("fraud_score") is None or state.get("verdict") is None) and state.get("recovery") is not None:
            rec: RecoveryInfo = state["recovery"]
            prior_events = await self.store.load_stream(rec.prior_session_stream, from_position=0)
            tool_events = [e for e in prior_events if e.get("event_type") == "AgentToolCalled" and (e.get("payload") or {}).get("tool_name") == "fraud_assessment"]
            if tool_events:
                import json

                out = (tool_events[-1].get("payload") or {}).get("tool_output_summary") or "{}"
                try:
                    d = json.loads(out)
                    state = {**state, "fraud_score": d.get("fraud_score"), "verdict": d.get("verdict"), "anomalies": d.get("anomalies")}
                except Exception:
                    pass

        anomaly_events: list[dict[str, Any]] = []
        for a in list(state.get("anomalies") or []):
            if str(a.get("severity")) in {"MEDIUM", "HIGH"}:
                anomaly_events.append(
                    FraudAnomalyDetected(
                        application_id=app_id,
                        session_id=self.session_id,
                        anomaly=FraudAnomaly(
                            anomaly_type=FraudAnomalyType(str(a.get("type") or FraudAnomalyType.REVENUE_DISCREPANCY.value)),
                            description=str(a.get("type") or "anomaly"),
                            severity=str(a.get("severity") or "LOW"),
                            evidence=str(a),
                            affected_fields=[],
                        ),
                        detected_at=datetime.now(timezone.utc),
                    ).to_store_dict()
                )

        completed = FraudScreeningCompleted(
            application_id=app_id,
            session_id=self.session_id,
            fraud_score=float(state.get("fraud_score") or 0.0),
            risk_level="LOW" if float(state.get("fraud_score") or 0.0) < 0.30 else "MEDIUM" if float(state.get("fraud_score") or 0.0) < 0.60 else "HIGH",
            anomalies_found=len(anomaly_events),
            recommendation=str(state.get("verdict") or "PROCEED"),
            screening_model_version=self.model_version,
            input_data_hash=self._sha({"facts": state.get("facts"), "hist": state.get("historical_financials")}),
            completed_at=datetime.now(timezone.utc),
        ).to_store_dict()

        await self.append_events_with_occ_retry(
            fraud_stream,
            anomaly_events + [completed],
            correlation_id=app_id,
            causation_id=self.session_id,
            idempotency_key=f"{app_id}:FraudScreeningCompleted",
        )
        # Mirror completion to loan stream for deterministic LoanApplication replay.
        await self.append_events_with_occ_retry(
            loan_stream,
            [completed],
            correlation_id=app_id,
            causation_id=self.session_id,
            idempotency_key=f"{app_id}:Loan:FraudScreeningCompleted",
        )
        await self.append_events_with_occ_retry(
            loan_stream,
            [
                ComplianceCheckRequested(
                    application_id=app_id,
                    requested_at=datetime.now(timezone.utc),
                    triggered_by_event_id=str(self.session_id),
                    regulation_set_version="2026-Q1",
                    rules_to_evaluate=[f"REG-00{i}" for i in range(1, 7)],
                ).to_store_dict()
            ],
            correlation_id=app_id,
            causation_id=self.session_id,
            idempotency_key=f"{app_id}:ComplianceCheckRequested",
        )

        events_written = [{"stream_id": fraud_stream, "event_type": "FraudScreeningCompleted"}, {"stream_id": loan_stream, "event_type": "ComplianceCheckRequested"}]
        await self.record_output_written(events_written, f"Fraud screening complete (score={float(state.get('fraud_score') or 0.0):.2f}).")
        await self.record_node_execution("write_output", ["fraud_assessment"], ["events_written"], int((time.time() - t0) * 1000))
        return {**state, "next_agent_triggered": "compliance", "events_written": events_written}
