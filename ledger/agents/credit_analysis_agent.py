from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from langgraph.graph import END, StateGraph

from ledger.agents.base_agent import BaseApexAgent, RecoveryInfo
from ledger.schema.events import (
    AgentType,
    CreditAnalysisCompleted,
    CreditDecision,
    CreditRecordOpened,
    ExtractedFactsConsumed,
    FraudScreeningRequested,
    HistoricalProfileConsumed,
    RiskTier,
)


class CreditAnalysisAgent(BaseApexAgent):
    agent_node_order = {
        "validate_inputs": 1,
        "open_aggregate_record": 2,
        "load_external_data": 3,
        "analyze_credit_risk": 4,
        "apply_policy_constraints": 5,
        "write_output": 6,
    }

    def build_graph(self) -> Any:
        g = StateGraph(dict)
        g.add_node("validate_inputs", self._node_validate_inputs)
        g.add_node("open_aggregate_record", self._node_open_credit_record)
        g.add_node("load_external_data", self._node_load_external_data)
        g.add_node("analyze_credit_risk", self._node_analyze)
        g.add_node("apply_policy_constraints", self._node_policy)
        g.add_node("write_output", self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs", "open_aggregate_record")
        g.add_edge("open_aggregate_record", "load_external_data")
        g.add_edge("load_external_data", "analyze_credit_risk")
        g.add_edge("analyze_credit_risk", "apply_policy_constraints")
        g.add_edge("apply_policy_constraints", "write_output")
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
        loan_stream = f"loan-{app_id}"
        docpkg_stream = f"docpkg-{app_id}"

        loan_events = await self.store.load_stream(loan_stream, from_position=0)
        submitted = next((e for e in loan_events if e.get("event_type") == "ApplicationSubmitted"), None)
        if not submitted:
            raise ValueError("Missing ApplicationSubmitted")
        applicant_id = str((submitted.get("payload") or {}).get("applicant_id"))
        requested_amount = float((submitted.get("payload") or {}).get("requested_amount_usd") or 0.0)
        loan_purpose = str((submitted.get("payload") or {}).get("loan_purpose") or "UNKNOWN")

        docpkg_events = await self.store.load_stream(docpkg_stream, from_position=0)
        if not any(e.get("event_type") == "PackageReadyForAnalysis" for e in docpkg_events):
            raise ValueError("Document package not ready for analysis")

        await self.record_input_validated(["application_id", "applicant_id", "docpkg_ready"], int((time.time() - t0) * 1000))
        await self.record_node_execution(
            "validate_inputs",
            ["loan_stream", "docpkg_stream"],
            ["applicant_id", "requested_amount_usd", "loan_purpose"],
            int((time.time() - t0) * 1000),
        )
        return {
            **state,
            "applicant_id": applicant_id,
            "requested_amount_usd": requested_amount,
            "loan_purpose": loan_purpose,
        }

    async def _node_open_credit_record(self, state: dict) -> dict:
        if self._skip(state, "open_aggregate_record"):
            return state
        t0 = time.time()
        app_id = state["application_id"]
        credit_stream = f"credit-{app_id}"

        existing = await self.store.load_stream(credit_stream, from_position=0)
        if not any(e.get("event_type") == "CreditRecordOpened" for e in existing):
            await self.append_events_with_occ_retry(
                credit_stream,
                [
                    CreditRecordOpened(
                        application_id=app_id,
                        applicant_id=state.get("applicant_id"),
                        opened_at=datetime.now(timezone.utc),
                    ).to_store_dict()
                ],
                correlation_id=app_id,
                causation_id=self.session_id,
                idempotency_key=f"{app_id}:CreditRecordOpened",
            )

        await self.record_node_execution("open_aggregate_record", ["application_id"], ["credit_stream_ready"], int((time.time() - t0) * 1000))
        return state

    async def _node_load_external_data(self, state: dict) -> dict:
        if self._skip(state, "load_external_data"):
            return state
        t0 = time.time()
        app_id = state["application_id"]

        applicant_id = str(state["applicant_id"])
        # Registry tool calls
        t_reg = time.time()
        profile = await self.registry.get_company(applicant_id)
        hist = await self.registry.get_financial_history(applicant_id)
        flags = await self.registry.get_compliance_flags(applicant_id, active_only=False)
        loans = await self.registry.get_loan_relationships(applicant_id)
        await self.record_tool_call(
            "query_applicant_registry",
            {"company_id": applicant_id},
            {"hist_years": len(hist), "flags": len(flags), "loans": len(loans)},
            int((time.time() - t_reg) * 1000),
        )

        # Document package facts consumption
        t_docs = time.time()
        docpkg_events = await self.store.load_stream(f"docpkg-{app_id}", from_position=0)
        extracted = [e for e in docpkg_events if e.get("event_type") == "ExtractionCompleted"]
        quality = next((e for e in docpkg_events if e.get("event_type") == "QualityAssessmentCompleted"), None)
        quality_missing = list(((quality or {}).get("payload") or {}).get("critical_missing_fields") or [])
        merged_facts: dict[str, Any] = {}
        doc_ids: list[str] = []
        for ev in extracted:
            p = ev.get("payload") or {}
            doc_ids.append(str(p.get("document_id") or ""))
            facts = p.get("facts") or {}
            if isinstance(facts, dict):
                for k, v in facts.items():
                    if v is not None and k not in merged_facts:
                        merged_facts[k] = v
        await self.record_tool_call("load_docpkg_extracted_facts", {"stream_id": f"docpkg-{app_id}"}, {"docs": len(extracted)}, int((time.time() - t_docs) * 1000))

        await self.append_events_with_occ_retry(
            f"credit-{app_id}",
            [
                HistoricalProfileConsumed(
                    application_id=app_id,
                    session_id=self.session_id,
                    fiscal_years_loaded=[int((y.get("fiscal_year") if isinstance(y, dict) else getattr(y, "fiscal_year", 0)) or 0) for y in hist],
                    has_prior_loans=bool(loans),
                    has_defaults=any(bool(l.get("default_occurred")) for l in loans or []),
                    revenue_trajectory=str(getattr(profile, "trajectory", "UNKNOWN") if profile else "UNKNOWN"),
                    data_hash=self._sha({"profile": getattr(profile, "__dict__", None), "hist": [getattr(h, "__dict__", None) for h in hist]}),
                    consumed_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                ExtractedFactsConsumed(
                    application_id=app_id,
                    session_id=self.session_id,
                    document_ids_consumed=[d for d in doc_ids if d],
                    facts_summary=f"keys={sorted(list(merged_facts.keys()))[:10]}",
                    quality_flags_present=bool(quality_missing),
                    consumed_at=datetime.now(timezone.utc),
                ).to_store_dict(),
            ],
            correlation_id=app_id,
            causation_id=self.session_id,
            idempotency_key=f"{app_id}:CreditInputsConsumed",
        )

        await self.record_node_execution(
            "load_external_data",
            ["registry", "docpkg"],
            ["company_profile", "historical_financials", "merged_facts"],
            int((time.time() - t0) * 1000),
        )
        return {
            **state,
            "company_profile": self._as_dict(profile) if profile else None,
            "historical_financials": [h if isinstance(h, dict) else (getattr(h, "__dict__", None) or {}) for h in hist],
            "compliance_flags": [f if isinstance(f, dict) else (getattr(f, "__dict__", None) or {}) for f in flags],
            "loan_history": loans,
            "extracted_facts": merged_facts,
            "quality_missing_fields": quality_missing,
        }

    async def _node_analyze(self, state: dict) -> dict:
        if self._skip(state, "analyze_credit_risk"):
            return state
        t0 = time.time()
        hist = list(state.get("historical_financials") or [])
        flags = list(state.get("compliance_flags") or [])
        loans = list(state.get("loan_history") or [])
        facts = dict(state.get("extracted_facts") or {})

        rev = float(hist[-1].get("total_revenue", 0.0)) if hist else float(facts.get("total_revenue") or 0.0)
        debt_to_ebitda = float(hist[-1].get("debt_to_ebitda", 0.0)) if hist else 0.0
        current_ratio = float(hist[-1].get("current_ratio", 0.0)) if hist else 0.0
        debt_to_equity = float(hist[-1].get("debt_to_equity", 0.0)) if hist else 0.0
        trajectory = str((state.get("company_profile") or {}).get("trajectory") or "STABLE")

        score = 0.0
        score += max(0.0, 1.0 - min(debt_to_ebitda / 6.0, 1.0)) * 0.30
        score += min(current_ratio / 2.0, 1.0) * 0.25
        score += max(0.0, 1.0 - min(debt_to_equity / 3.0, 1.0)) * 0.25
        score += (0.30 if trajectory in ("GROWTH", "STABLE") else 0.10 if trajectory == "DECLINING" else 0.20) * 0.20

        risk = RiskTier.LOW if score > 0.70 else RiskTier.MEDIUM if score > 0.45 else RiskTier.HIGH
        has_default = any(bool(l.get("default_occurred")) for l in loans)
        if has_default:
            risk = RiskTier.HIGH

        requested = float(state.get("requested_amount_usd") or 0.0)
        base_limit = int(min(requested, rev * 0.35)) if rev > 0 else int(requested * 0.25)
        recommended_limit = int(max(0, base_limit))

        confidence = round(0.55 + score * 0.38, 2)
        if risk == RiskTier.HIGH:
            confidence = min(confidence, 0.70)
        if any((f.get("severity") == "HIGH" and f.get("is_active")) for f in flags if isinstance(f, dict)):
            confidence = min(confidence, 0.50)

        caveats: list[str] = []
        missing = list(state.get("quality_missing_fields") or [])
        if missing:
            caveats.append(f"Document extraction missing critical fields: {missing}")
            confidence = min(confidence, 0.75)
        if not hist:
            caveats.append("No historical registry financials available.")
            confidence = min(confidence, 0.72)

        rationale = (
            f"Risk tier {risk.value} based on leverage and liquidity. "
            f"Recommended limit capped at 35% of revenue (${recommended_limit:,.0f})."
        )
        decision = {
            "risk_tier": risk.value,
            "recommended_limit_usd": recommended_limit,
            "confidence": float(confidence),
            "rationale": rationale,
            "key_concerns": ["Prior default"] if has_default else [],
            "data_quality_caveats": caveats,
            "policy_overrides_applied": [],
        }

        await self.record_node_execution(
            "analyze_credit_risk",
            ["historical_financials", "extracted_facts"],
            ["credit_decision"],
            int((time.time() - t0) * 1000),
        )
        return {**state, "credit_decision": decision}

    async def _node_policy(self, state: dict) -> dict:
        if self._skip(state, "apply_policy_constraints"):
            return state
        t0 = time.time()
        d = dict(state.get("credit_decision") or {})
        hist = list(state.get("historical_financials") or [])
        flags = list(state.get("compliance_flags") or [])
        loans = list(state.get("loan_history") or [])
        viols: list[str] = []

        if hist:
            rev = float(hist[-1].get("total_revenue", 0.0))
            cap = int(rev * 0.35)
            if cap > 0 and int(d.get("recommended_limit_usd") or 0) > cap:
                d["recommended_limit_usd"] = cap
                viols.append("POLICY_REV_CAP_35_PCT")

        if any(bool(l.get("default_occurred")) for l in loans):
            if d.get("risk_tier") != RiskTier.HIGH.value:
                d["risk_tier"] = RiskTier.HIGH.value
                viols.append("POLICY_PRIOR_DEFAULT")

        if any((f.get("severity") == "HIGH" and f.get("is_active")) for f in flags if isinstance(f, dict)):
            if float(d.get("confidence") or 0.0) > 0.50:
                d["confidence"] = 0.50
                viols.append("POLICY_ACTIVE_HIGH_COMPLIANCE_FLAG")

        if viols:
            d["policy_overrides_applied"] = list(d.get("policy_overrides_applied") or []) + viols

        await self.record_node_execution(
            "apply_policy_constraints",
            ["credit_decision"],
            ["credit_decision"],
            int((time.time() - t0) * 1000),
        )
        return {**state, "credit_decision": d, "policy_violations": viols}

    async def _node_write_output(self, state: dict) -> dict:
        if self._skip(state, "write_output"):
            return state
        t0 = time.time()
        app_id = state["application_id"]
        credit_stream = f"credit-{app_id}"
        loan_stream = f"loan-{app_id}"

        # Idempotency: if already completed, do not append again.
        existing = await self.store.load_stream(credit_stream, from_position=0)
        if any(e.get("event_type") == "CreditAnalysisCompleted" for e in existing):
            await self.record_output_written([], "Credit analysis already completed; no-op.")
            await self.record_node_execution("write_output", ["credit_stream"], ["noop"], int((time.time() - t0) * 1000))
            return {**state, "next_agent_triggered": "fraud_detection"}

        d = dict(state["credit_decision"])
        ev = CreditAnalysisCompleted(
            application_id=app_id,
            session_id=self.session_id,
            decision=CreditDecision(
                risk_tier=RiskTier(d["risk_tier"]),
                recommended_limit_usd=Decimal(str(int(d["recommended_limit_usd"]))),
                confidence=float(d["confidence"]),
                rationale=str(d.get("rationale") or ""),
                key_concerns=list(d.get("key_concerns") or []),
                data_quality_caveats=list(d.get("data_quality_caveats") or []),
                policy_overrides_applied=list(d.get("policy_overrides_applied") or []),
            ),
            model_version=self.model_version,
            model_deployment_id=f"dep-{uuid4().hex[:8]}",
            input_data_hash=self._sha({"registry": state.get("historical_financials"), "facts": state.get("extracted_facts")}),
            analysis_duration_ms=int((time.time() - self._t0) * 1000),
            completed_at=datetime.now(timezone.utc),
        ).to_store_dict()

        positions = await self.append_events_with_occ_retry(
            credit_stream,
            [ev],
            correlation_id=app_id,
            causation_id=self.session_id,
            idempotency_key=f"{app_id}:CreditAnalysisCompleted",
        )
        # Mirror to loan stream for deterministic LoanApplication replay.
        await self.append_events_with_occ_retry(
            loan_stream,
            [ev],
            correlation_id=app_id,
            causation_id=self.session_id,
            idempotency_key=f"{app_id}:Loan:CreditAnalysisCompleted",
        )

        await self.append_events_with_occ_retry(
            loan_stream,
            [
                FraudScreeningRequested(
                    application_id=app_id,
                    requested_at=datetime.now(timezone.utc),
                    triggered_by_event_id=str(self.session_id),
                ).to_store_dict()
            ],
            correlation_id=app_id,
            causation_id=self.session_id,
            idempotency_key=f"{app_id}:FraudScreeningRequested",
        )

        events_written = [
            {"stream_id": credit_stream, "event_type": "CreditAnalysisCompleted", "stream_position": (positions[0] if positions else None)},
            {"stream_id": loan_stream, "event_type": "FraudScreeningRequested"},
        ]
        await self.record_output_written(events_written, f"Credit analysis complete ({d['risk_tier']}, conf={float(d['confidence']):.2f}).")
        await self.record_node_execution("write_output", ["credit_decision"], ["events_written"], int((time.time() - t0) * 1000))
        return {**state, "next_agent_triggered": "fraud_detection", "events_written": events_written}
