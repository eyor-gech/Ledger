"""
ledger/agents/base_agent.py
===========================
BASE LANGGRAPH AGENT + all 5 agent class stubs.
CreditAnalysisAgent is the reference implementation with full LangGraph pattern.
The other 4 agents are stubs with complete docstrings for implementation.
"""
from __future__ import annotations
import asyncio
import hashlib
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4
from anthropic import AsyncAnthropic
from langgraph.graph import StateGraph, END

from ledger.event_store import OptimisticConcurrencyError
from ledger.schema.events import (
    AgentInputValidationFailed,
    AgentInputValidated,
    AgentContextLoaded,
    AgentNodeExecuted,
    AgentOutputWritten,
    AgentSessionCompleted,
    AgentSessionFailed,
    AgentSessionStarted,
    AgentToolCalled,
    AgentType,
    ApplicationApproved,
    ApplicationDeclined,
    ComplianceCheckCompleted,
    ComplianceCheckInitiated,
    ComplianceCheckRequested,
    ComplianceVerdict,
    ComplianceRuleFailed,
    ComplianceRuleNoted,
    ComplianceRulePassed,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    CreditDecision,
    CreditRecordOpened,
    DecisionGenerated,
    DecisionRequested,
    DocumentAdded,
    DocumentFormat,
    DocumentFormatValidated,
    DocumentType,
    ExtractedFactsConsumed,
    ExtractionCompleted,
    ExtractionStarted,
    FraudAnomalyDetected,
    FraudScreeningCompleted,
    FraudScreeningInitiated,
    FraudScreeningRequested,
    HistoricalProfileConsumed,
    HumanReviewRequested,
    PackageCreated,
    PackageReadyForAnalysis,
    QualityAssessmentCompleted,
    RiskTier,
)

LANGGRAPH_VERSION = "1.0.0"
MAX_OCC_RETRIES = 5

class BaseApexAgent(ABC):
    """
    Base for all 5 Apex agents. Provides Gas Town session management,
    per-node event recording, tool call recording, OCC retry scaffolding.

    AGENT NODE SEQUENCE (all agents follow this):
        start_session → validate_inputs → load_context → [domain nodes] → write_output → end_session

    Each node must call self._record_node_execution() at its end.
    Each tool/registry call must call self._record_tool_call().
    The write_output node must call self._record_output_written() then self._record_node_execution().
    """
    def __init__(
        self,
        agent_id: str,
        agent_type: AgentType | str,
        store,
        registry,
        client: AsyncAnthropic,
        model: str = "claude-sonnet-4-20250514",
    ):
        self.agent_id = agent_id
        self.agent_type = agent_type if isinstance(agent_type, AgentType) else AgentType(str(agent_type))
        self.store = store; self.registry = registry; self.client = client; self.model = model
        self.session_id = None; self.application_id = None
        self._session_stream = None; self._t0 = None
        self._seq = 0; self._llm_calls = 0; self._tokens = 0; self._cost = 0.0
        self._graph = None

    @abstractmethod
    def build_graph(self): raise NotImplementedError

    async def process_application(self, application_id: str) -> None:
        if not self._graph: self._graph = self.build_graph()
        self.application_id = application_id
        self.session_id = f"sess-{self.agent_type.value[:3]}-{uuid4().hex[:8]}"
        self._session_stream = f"agent-{self.agent_type.value}-{self.session_id}"
        self._t0 = time.time(); self._seq = 0; self._llm_calls = 0; self._tokens = 0; self._cost = 0.0
        await self._start_session(application_id)
        try:
            result = await self._graph.ainvoke(self._initial_state(application_id))
            await self._complete_session(result)
        except Exception as e:
            await self._fail_session(type(e).__name__, str(e)); raise

    def _initial_state(self, app_id):
        return {"application_id": app_id, "session_id": self.session_id,
                "agent_id": self.agent_id, "errors": [], "output_events_written": [], "next_agent_triggered": None}

    async def _start_session(self, app_id):
        ev = AgentSessionStarted(
            session_id=self.session_id,
            agent_type=self.agent_type,
            agent_id=self.agent_id,
            application_id=app_id,
            model_version=self.model,
            langgraph_graph_version=LANGGRAPH_VERSION,
            context_source="fresh",
            context_token_count=0,
            started_at=datetime.now(timezone.utc),
        )
        await self._append_session(ev.to_store_dict())
        ctx = AgentContextLoaded(
            session_id=self.session_id,
            agent_type=self.agent_type,
            application_id=app_id,
            model_version=self.model,
            context_source="fresh",
            context_hash=self._sha({"application_id": app_id, "session_id": self.session_id}),
            loaded_at=datetime.now(timezone.utc),
        )
        await self._append_session(ctx.to_store_dict())

    async def _record_node_execution(self, name, in_keys, out_keys, ms, tok_in=None, tok_out=None, cost=None):
        self._seq += 1
        if tok_in is not None:
            self._tokens += int(tok_in) + int(tok_out or 0)
            self._llm_calls += 1
        if cost is not None:
            self._cost += float(cost)

        ev = AgentNodeExecuted(
            session_id=self.session_id,
            agent_type=self.agent_type,
            node_name=name,
            node_sequence=self._seq,
            input_keys=list(in_keys),
            output_keys=list(out_keys),
            llm_called=tok_in is not None,
            llm_tokens_input=int(tok_in) if tok_in is not None else None,
            llm_tokens_output=int(tok_out) if tok_out is not None else None,
            llm_cost_usd=float(cost) if cost is not None else None,
            duration_ms=int(ms),
            executed_at=datetime.now(timezone.utc),
        )
        await self._append_session(ev.to_store_dict())

    async def _record_tool_call(self, tool, inp, out, ms):
        ev = AgentToolCalled(
            session_id=self.session_id,
            agent_type=self.agent_type,
            tool_name=str(tool),
            tool_input_summary=str(inp),
            tool_output_summary=str(out),
            tool_duration_ms=int(ms),
            called_at=datetime.now(timezone.utc),
        )
        await self._append_session(ev.to_store_dict())

    async def _record_output_written(self, events_written, summary):
        ev = AgentOutputWritten(
            session_id=self.session_id,
            agent_type=self.agent_type,
            application_id=self.application_id,
            events_written=list(events_written),
            output_summary=str(summary),
            written_at=datetime.now(timezone.utc),
        )
        await self._append_session(ev.to_store_dict())

    async def _complete_session(self, result):
        ms = int((time.time()-self._t0)*1000)
        ev = AgentSessionCompleted(
            session_id=self.session_id,
            agent_type=self.agent_type,
            application_id=self.application_id,
            total_nodes_executed=self._seq,
            total_llm_calls=self._llm_calls,
            total_tokens_used=self._tokens,
            total_cost_usd=round(self._cost, 6),
            total_duration_ms=int(ms),
            next_agent_triggered=result.get("next_agent_triggered"),
            completed_at=datetime.now(timezone.utc),
        )
        await self._append_session(ev.to_store_dict())

    async def _fail_session(self, etype, emsg):
        ev = AgentSessionFailed(
            session_id=self.session_id,
            agent_type=self.agent_type,
            application_id=self.application_id,
            error_type=str(etype),
            error_message=str(emsg)[:500],
            last_successful_node=f"node_{self._seq}",
            recoverable=str(etype) in {"llm_timeout", "RateLimitError"},
            failed_at=datetime.now(timezone.utc),
        )
        await self._append_session(ev.to_store_dict())

    async def _append_events_with_occ_retry(
        self,
        stream_id: str,
        event_dicts: list[dict[str, Any]],
        *,
        causation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        for attempt in range(MAX_OCC_RETRIES):
            expected = await self.store.stream_version(stream_id)
            try:
                await self.store.append(
                    stream_id=stream_id,
                    events=event_dicts,
                    expected_version=expected,
                    causation_id=causation_id,
                    metadata=metadata,
                )
                return
            except OptimisticConcurrencyError as exc:
                # Best-effort idempotency: if our idempotency_key is already present, treat as success.
                if metadata and metadata.get("idempotency_key"):
                    tail = await self.store.load_stream(stream_id, from_position=max(0, exc.actual - 10))
                    if any((e.get("metadata") or {}).get("idempotency_key") == metadata["idempotency_key"] for e in tail):
                        return
                if attempt < MAX_OCC_RETRIES - 1:
                    await asyncio.sleep(0.05 * (2**attempt))
                    continue
                raise

    async def _append_session(self, event: dict[str, Any]) -> None:
        if self._session_stream is None:
            raise RuntimeError("session stream not initialized")
        idem = f"{self.session_id}:{event.get('event_type')}:{event.get('payload', {}).get('node_sequence','')}"
        await self._append_events_with_occ_retry(
            self._session_stream,
            [event],
            metadata={"idempotency_key": idem},
        )

    async def _append_stream(self, stream_id: str, event_dict: dict[str, Any], *, causation_id: str | None = None) -> None:
        await self._append_events_with_occ_retry(stream_id, [event_dict], causation_id=causation_id)

    async def _call_llm(self, system, user, max_tokens=1024):
        resp = await self.client.messages.create(model=self.model, max_tokens=max_tokens,
            system=system, messages=[{"role":"user","content":user}])
        text_parts = []
        for block in getattr(resp, "content", []) or []:
            t = getattr(block, "text", None)
            if t:
                text_parts.append(t)
        text = "\n".join(text_parts) if text_parts else str(resp.content)
        usage = getattr(resp, "usage", None)
        tok_in = int(getattr(usage, "input_tokens", 0) or 0)
        tok_out = int(getattr(usage, "output_tokens", 0) or 0)
        # Cost is model/pricing dependent; treat as advisory.
        est_cost = round(tok_in / 1e6 * 3.0 + tok_out / 1e6 * 15.0, 6)
        return text, tok_in, tok_out, est_cost

    @staticmethod
    def _sha(d: Any) -> str:
        return hashlib.sha256(json.dumps(d, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()[:16]


class CreditAnalysisAgent(BaseApexAgent):
    """
    Reference implementation. LangGraph nodes:
      validate_inputs → open_credit_record → load_applicant_registry
      → load_extracted_facts → analyze_credit_risk → apply_policy_constraints → write_output

    Output streams:
      credit-{id}: CreditRecordOpened, HistoricalProfileConsumed, ExtractedFactsConsumed, CreditAnalysisCompleted
      loan-{id}: FraudScreeningRequested  (triggers next agent)
    """
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            applicant_id: str | None; requested_amount_usd: float | None; loan_purpose: str | None
            historical_financials: list | None; company_profile: dict | None
            compliance_flags: list | None; loan_history: list | None
            extracted_facts: dict | None; quality_flags: list | None
            credit_decision: dict | None; policy_violations: list | None
            errors: list; output_events_written: list; next_agent_triggered: str | None

        g = StateGraph(S)
        for name, fn in [
            ("validate_inputs",          self._node_validate_inputs),
            ("open_credit_record",       self._node_open_credit_record),
            ("load_applicant_registry",  self._node_load_registry),
            ("load_extracted_facts",     self._node_load_facts),
            ("analyze_credit_risk",      self._node_analyze),
            ("apply_policy_constraints", self._node_policy),
            ("write_output",             self._node_write),
        ]: g.add_node(name, fn)
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs","open_credit_record")
        g.add_edge("open_credit_record","load_applicant_registry")
        g.add_edge("load_applicant_registry","load_extracted_facts")
        g.add_edge("load_extracted_facts","analyze_credit_risk")
        g.add_edge("analyze_credit_risk","apply_policy_constraints")
        g.add_edge("apply_policy_constraints","write_output")
        g.add_edge("write_output", END)
        return g.compile()

    async def _node_validate_inputs(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        loan_stream = f"loan-{app_id}"
        docpkg_stream = f"docpkg-{app_id}"

        loan_events = await self.store.load_stream(loan_stream, from_position=0)
        submitted = next((e for e in loan_events if e.get("event_type") == "ApplicationSubmitted"), None)
        if submitted is None:
            await self._append_session(
                AgentInputValidationFailed(
                    session_id=self.session_id,
                    agent_type=self.agent_type,
                    application_id=app_id,
                    missing_inputs=["ApplicationSubmitted"],
                    validation_errors=["loan stream missing ApplicationSubmitted"],
                    failed_at=datetime.now(timezone.utc),
                ).to_store_dict()
            )
            raise ValueError("loan stream missing ApplicationSubmitted")

        payload = submitted.get("payload") or {}
        applicant_id = payload.get("applicant_id")
        requested_amount = payload.get("requested_amount_usd")
        loan_purpose = payload.get("loan_purpose")
        if not applicant_id or requested_amount is None or not loan_purpose:
            raise ValueError("ApplicationSubmitted missing applicant_id/requested_amount_usd/loan_purpose")

        docpkg_events = await self.store.load_stream(docpkg_stream, from_position=0)
        if not any(e.get("event_type") == "PackageReadyForAnalysis" for e in docpkg_events):
            raise ValueError("document package not ready for analysis")

        await self._append_session(
            AgentInputValidated(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=app_id,
                inputs_validated=["ApplicationSubmitted", "PackageReadyForAnalysis"],
                validation_duration_ms=int((time.time() - t0) * 1000),
                validated_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

        state = {
            **state,
            "applicant_id": str(applicant_id),
            "requested_amount_usd": float(requested_amount),
            "loan_purpose": str(loan_purpose),
        }
        await self._record_node_execution(
            "validate_inputs",
            ["application_id"],
            ["applicant_id", "requested_amount_usd", "loan_purpose"],
            int((time.time() - t0) * 1000),
        )
        return state

    async def _node_open_credit_record(self, state):
        t = time.time()
        app_id = state["application_id"]
        credit_stream = f"credit-{app_id}"
        if await self.store.stream_version(credit_stream) == -1:
            await self._append_stream(
                credit_stream,
                CreditRecordOpened(
                    application_id=app_id,
                    applicant_id=str(state["applicant_id"]),
                    opened_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
        await self._record_node_execution("open_credit_record",["applicant_id"],["credit_stream_opened"],int((time.time()-t)*1000))
        return state

    async def _node_load_registry(self, state):
        t = time.time()
        company_id = state["applicant_id"]
        profile = None
        hist = []
        flags = []
        loans = []
        if self.registry is not None:
            get_company = getattr(self.registry, "get_company", None)
            get_financial_history = getattr(self.registry, "get_financial_history", None)
            get_compliance_flags = getattr(self.registry, "get_compliance_flags", None)
            get_loan_relationships = getattr(self.registry, "get_loan_relationships", None)
            if callable(get_company):
                profile = await get_company(company_id)
            if callable(get_financial_history):
                hist = await get_financial_history(company_id)
            if callable(get_compliance_flags):
                flags = await get_compliance_flags(company_id)
            if callable(get_loan_relationships):
                loans = await get_loan_relationships(company_id)
        ms = int((time.time()-t)*1000)
        await self._record_tool_call("query_applicant_registry", f"company_id={state['applicant_id']}", "3yr financials loaded", ms)
        await self._append_stream(
            f"credit-{state['application_id']}",
            HistoricalProfileConsumed(
                application_id=state["application_id"],
                session_id=self.session_id,
                fiscal_years_loaded=[int(getattr(x, "fiscal_year", 0) or 0) for x in (hist or []) if getattr(x, "fiscal_year", None)],
                has_prior_loans=bool(loans),
                has_defaults=any(bool(l.get("default_occurred")) for l in (loans or []) if isinstance(l, dict)),
                revenue_trajectory=str(getattr(profile, "trajectory", "UNKNOWN") if profile is not None else "UNKNOWN"),
                data_hash=self._sha({"profile": str(profile), "hist": [str(h) for h in hist], "flags": [str(f) for f in flags], "loans": loans}),
                consumed_at=datetime.now(timezone.utc),
            ).to_store_dict(),
            causation_id=self.session_id,
        )
        await self._record_node_execution("load_applicant_registry",["applicant_id"],["historical_financials","compliance_flags","loan_history"],ms)
        return {
            **state,
            "company_profile": profile.__dict__ if hasattr(profile, "__dict__") else (profile or {}),
            "historical_financials": [h.__dict__ if hasattr(h, "__dict__") else h for h in (hist or [])],
            "compliance_flags": [f.__dict__ if hasattr(f, "__dict__") else f for f in (flags or [])],
            "loan_history": loans or [],
        }

    async def _node_load_facts(self, state):
        t = time.time()
        docpkg_stream = f"docpkg-{state['application_id']}"
        doc_events = await self.store.load_stream(docpkg_stream, from_position=0)
        extracted = [e for e in doc_events if e.get("event_type") == "ExtractionCompleted"]
        doc_ids = [str(e.get("payload", {}).get("document_id")) for e in extracted if e.get("payload", {}).get("document_id")]
        facts_summary = f"{len(extracted)} docs extracted"
        quality_flags_present = any(
            e.get("event_type") == "QualityAssessmentCompleted" and (e.get("payload", {}).get("critical_missing_fields") or [])
            for e in doc_events
        )
        ms = int((time.time()-t)*1000)
        await self._record_tool_call("load_event_store_stream", docpkg_stream, "ExtractionCompleted events loaded", ms)
        await self._append_stream(
            f"credit-{state['application_id']}",
            ExtractedFactsConsumed(
                application_id=state["application_id"],
                session_id=self.session_id,
                document_ids_consumed=doc_ids or [],
                facts_summary=facts_summary,
                quality_flags_present=quality_flags_present,
                consumed_at=datetime.now(timezone.utc),
            ).to_store_dict(),
            causation_id=self.session_id,
        )
        await self._record_node_execution("load_extracted_facts",["document_package_events"],["extracted_facts","quality_flags"],ms)
        return {**state, "extracted_facts": {"facts_summary": facts_summary}, "quality_flags": ["critical_missing"] if quality_flags_present else []}

    async def _node_analyze(self, state):
        t = time.time()
        hist = state.get("historical_financials") or []
        fin_table = "\n".join([f"FY{f['fiscal_year'] if isinstance(f,dict) else ''}: (historical data)" for f in hist]) if hist else "No historical data loaded — TODO: implement load_applicant_registry"
        system = """You are a commercial credit analyst at Apex Financial Services.
Evaluate the loan application and return ONLY a JSON object with these fields:
{"risk_tier":"LOW"|"MEDIUM"|"HIGH","recommended_limit_usd":<int>,"confidence":<float 0-1>,
 "rationale":"<3-5 sentences>","key_concerns":[],"data_quality_caveats":[],"policy_overrides_applied":[]}
Hard policy rules you must enforce:
1. recommended_limit_usd <= annual_revenue * 0.35
2. Any prior default → risk_tier must be HIGH
3. Active HIGH compliance flag → confidence must be <= 0.50"""
        user = f"""Applicant: {state.get('company_profile',{}).get('name','Unknown')}
Requested: ${state.get('requested_amount_usd',0):,.0f} for {state.get('loan_purpose','unknown')}
Historical financials:\n{fin_table}
Current year extracted facts: {json.dumps(state.get('extracted_facts',{}),default=str)[:1000]}
Quality flags: {state.get('quality_flags',[])}
Compliance flags: {state.get('compliance_flags',[])}
Prior loans: {state.get('loan_history',[])}"""
        try:
            content, tok_in, tok_out, cost = await self._call_llm(system, user, max_tokens=800)
            import re; m = re.search(r'\{.*\}', content, re.DOTALL)
            decision = json.loads(m.group()) if m else {}
        except Exception as e:
            decision = {"risk_tier":"MEDIUM","recommended_limit_usd":int(state.get("requested_amount_usd",0)*0.8),"confidence":0.45,"rationale":f"Analysis deferred: {e}","key_concerns":["LLM analysis failed — human review required"],"data_quality_caveats":[],"policy_overrides_applied":[]}
            tok_in=tok_out=0; cost=0.0
        ms = int((time.time()-t)*1000)
        await self._record_node_execution("analyze_credit_risk",["historical_financials","extracted_facts"],["credit_decision"],ms,tok_in,tok_out,cost)
        return {**state,"credit_decision":decision}

    async def _node_policy(self, state):
        t = time.time()
        d = state.get("credit_decision") or {}; violations = []
        hist = state.get("historical_financials") or []
        if hist:
            rev = hist[-1].get("total_revenue",0) if isinstance(hist[-1],dict) else 0
            if rev > 0 and d.get("recommended_limit_usd",0) > rev*0.35:
                d["recommended_limit_usd"] = int(rev*0.35); violations.append("REV_CAP")
        if any(l.get("default_occurred") for l in (state.get("loan_history") or [])):
            d["risk_tier"] = "HIGH"; violations.append("PRIOR_DEFAULT")
        if any(f.get("severity")=="HIGH" and f.get("is_active") for f in (state.get("compliance_flags") or [])):
            d["confidence"] = min(d.get("confidence",1.0), 0.50); violations.append("COMPLIANCE_FLAG")
        if violations: d["policy_overrides_applied"] = d.get("policy_overrides_applied",[]) + violations
        await self._record_node_execution("apply_policy_constraints",["credit_decision"],["credit_decision"],int((time.time()-t)*1000))
        return {**state,"credit_decision":d,"policy_violations":violations}

    async def _node_write(self, state):
        t = time.time()
        app_id = state["application_id"]; d = state["credit_decision"]
        credit_stream = f"credit-{app_id}"
        loan_stream = f"loan-{app_id}"

        credit_events = await self.store.load_stream(credit_stream, from_position=0)
        if not any(e.get("event_type") == "CreditAnalysisCompleted" for e in credit_events):
            decision = CreditDecision(
                risk_tier=RiskTier(str(d.get("risk_tier", "MEDIUM"))),
                recommended_limit_usd=Decimal(str(int(d.get("recommended_limit_usd", 0)))),
                confidence=float(d.get("confidence", 0.0)),
                rationale=str(d.get("rationale", "")),
                key_concerns=list(d.get("key_concerns") or []),
                data_quality_caveats=list(d.get("data_quality_caveats") or []),
                policy_overrides_applied=list(d.get("policy_overrides_applied") or []),
            )
            await self._append_stream(
                credit_stream,
                CreditAnalysisCompleted(
                    application_id=app_id,
                    session_id=self.session_id,
                    decision=decision,
                    model_version=self.model,
                    model_deployment_id=f"dep-{uuid4().hex[:8]}",
                    model_versions={"credit_analysis": self.model},
                    input_data_hash=self._sha({"app": app_id, "decision": d}),
                    analysis_duration_ms=int((time.time() - t) * 1000),
                    regulatory_basis=[],
                    completed_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )

        loan_events = await self.store.load_stream(loan_stream, from_position=0)
        if not any(e.get("event_type") == "FraudScreeningRequested" for e in loan_events):
            await self._append_stream(
                loan_stream,
                FraudScreeningRequested(
                    application_id=app_id,
                    requested_at=datetime.now(timezone.utc),
                    triggered_by_event_id=self._sha(self.session_id),
                ).to_store_dict(),
                causation_id=self.session_id,
            )

        events_written = [
            {"stream_id": credit_stream, "event_type": "CreditAnalysisCompleted"},
            {"stream_id": loan_stream, "event_type": "FraudScreeningRequested"},
        ]
        await self._record_output_written(events_written, f"Credit: {d.get('risk_tier')} risk, ${d.get('recommended_limit_usd',0):,.0f} limit, {d.get('confidence',0):.0%} confidence. Fraud screening triggered.")
        await self._record_node_execution("write_output",["credit_decision"],["events_written"],int((time.time()-t)*1000))
        return {**state,"output_events_written":events_written,"next_agent_triggered":"fraud_detection"}


class DocumentProcessingAgent(BaseApexAgent):
    """
    Wraps the Week 3 Document Intelligence pipeline as a LangGraph agent.

    NODES TO IMPLEMENT:
        validate_inputs → validate_document_format → run_week3_extraction
        → assess_quality (LLM) → write_output

    WEEK 3 INTEGRATION — in _node_run_week3_extraction:
        from document_refinery.pipeline import extract_financial_facts
        for each doc in package:
            append ExtractionStarted to docpkg stream
            facts = await extract_financial_facts(file_path, document_type)
            append ExtractionCompleted(facts=facts) to docpkg stream

    LLM ROLE — in _node_assess_quality:
        System prompt: "You are a financial document quality analyst.
        Check extracted facts for internal consistency. Do NOT make credit decisions.
        Return DocumentQualityAssessment JSON."
        Specifically check: balance_sheet_balances, EBITDA plausibility,
        margin ranges for industry, critical missing fields.

    OUTPUT STREAMS:
        docpkg-{id}: DocumentFormatValidated, ExtractionStarted, ExtractionCompleted,
                     QualityAssessmentCompleted, PackageReadyForAnalysis
        loan-{id}: CreditAnalysisRequested
    """
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            document_ids: list | None; extracted_facts_by_doc: dict | None
            quality_assessment: dict | None; has_critical_issues: bool | None
            errors: list; output_events_written: list; next_agent_triggered: str | None
        g = StateGraph(S)
        g.add_node("validate_inputs",         self._node_validate_inputs)
        g.add_node("validate_document_format",self._node_validate_format)
        g.add_node("run_week3_extraction",     self._node_extract)
        g.add_node("assess_quality",           self._node_assess_quality)
        g.add_node("write_output",             self._node_write_output)
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs","validate_document_format")
        g.add_edge("validate_document_format","run_week3_extraction")
        g.add_edge("run_week3_extraction","assess_quality")
        g.add_edge("assess_quality","write_output")
        g.add_edge("write_output", END)
        return g.compile()

    async def _node_validate_inputs(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        loan_stream = f"loan-{app_id}"
        loan_events = await self.store.load_stream(loan_stream, from_position=0)
        uploaded = [e for e in loan_events if e.get("event_type") == "DocumentUploaded"]
        if not uploaded:
            await self._append_session(
                AgentInputValidationFailed(
                    session_id=self.session_id,
                    agent_type=self.agent_type,
                    application_id=app_id,
                    missing_inputs=["DocumentUploaded"],
                    validation_errors=["loan stream has no DocumentUploaded events"],
                    failed_at=datetime.now(timezone.utc),
                ).to_store_dict()
            )
            raise ValueError("no documents uploaded")

        await self._append_session(
            AgentInputValidated(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=app_id,
                inputs_validated=["DocumentUploaded"],
                validation_duration_ms=int((time.time() - t0) * 1000),
                validated_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )
        await self._record_node_execution(
            "validate_inputs",
            ["application_id"],
            ["document_ids"],
            int((time.time() - t0) * 1000),
        )
        return {**state, "document_ids": [e["payload"]["document_id"] for e in uploaded]}
    async def _node_validate_format(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        docpkg_stream = f"docpkg-{app_id}"
        package_id = f"pkg-{app_id}"
        if await self.store.stream_version(docpkg_stream) == -1:
            await self._append_stream(
                docpkg_stream,
                PackageCreated(
                    package_id=package_id,
                    application_id=app_id,
                    required_documents=[],
                    created_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )

        # Mirror loan DocumentUploaded into docpkg DocumentAdded + DocumentFormatValidated.
        loan_events = await self.store.load_stream(f"loan-{app_id}", from_position=0)
        uploaded = [e for e in loan_events if e.get("event_type") == "DocumentUploaded"]
        for e in uploaded:
            p = e["payload"]
            doc_id = str(p["document_id"])
            await self._append_stream(
                docpkg_stream,
                DocumentAdded(
                    package_id=package_id,
                    document_id=doc_id,
                    document_type=DocumentType(str(p["document_type"])),
                    document_format=DocumentFormat(str(p["document_format"])),
                    file_hash=str(p.get("file_hash") or ""),
                    added_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=str(e.get("event_id") or self.session_id),
            )
            await self._append_stream(
                docpkg_stream,
                DocumentFormatValidated(
                    package_id=package_id,
                    document_id=doc_id,
                    document_type=DocumentType(str(p["document_type"])),
                    page_count=1,
                    detected_format=str(p["document_format"]),
                    validated_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=str(e.get("event_id") or self.session_id),
            )

        await self._record_node_execution(
            "validate_document_format",
            ["document_ids"],
            ["formats_validated"],
            int((time.time() - t0) * 1000),
        )
        return state
    async def _node_extract(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        docpkg_stream = f"docpkg-{app_id}"
        package_id = f"pkg-{app_id}"
        loan_events = await self.store.load_stream(f"loan-{app_id}", from_position=0)
        uploaded = [e for e in loan_events if e.get("event_type") == "DocumentUploaded"]
        for e in uploaded:
            p = e["payload"]
            doc_id = str(p["document_id"])
            doc_type = DocumentType(str(p["document_type"]))
            await self._append_stream(
                docpkg_stream,
                ExtractionStarted(
                    package_id=package_id,
                    document_id=doc_id,
                    document_type=doc_type,
                    pipeline_version="week3-stub",
                    extraction_model="none",
                    started_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=str(e.get("event_id") or self.session_id),
            )
            await self._append_stream(
                docpkg_stream,
                ExtractionCompleted(
                    package_id=package_id,
                    document_id=doc_id,
                    document_type=doc_type,
                    facts=None,
                    raw_text_length=0,
                    tables_extracted=0,
                    processing_ms=0,
                    completed_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=str(e.get("event_id") or self.session_id),
            )

        await self._record_node_execution(
            "run_week3_extraction",
            ["document_ids"],
            ["extracted_facts_by_doc"],
            int((time.time() - t0) * 1000),
        )
        return {**state, "extracted_facts_by_doc": {}}
    async def _node_assess_quality(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        docpkg_stream = f"docpkg-{app_id}"
        package_id = f"pkg-{app_id}"
        # Minimal deterministic quality assessment (no LLM in Phase 2 scaffolding).
        doc_events = await self.store.load_stream(docpkg_stream, from_position=0)
        docs = {e["payload"]["document_id"] for e in doc_events if e.get("event_type") == "DocumentAdded"}
        for doc_id in docs:
            await self._append_stream(
                docpkg_stream,
                QualityAssessmentCompleted(
                    package_id=package_id,
                    document_id=str(doc_id),
                    overall_confidence=1.0,
                    is_coherent=True,
                    anomalies=[],
                    critical_missing_fields=[],
                    reextraction_recommended=False,
                    auditor_notes="stub quality assessment",
                    assessed_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )

        await self._record_node_execution(
            "assess_quality",
            ["extracted_facts_by_doc"],
            ["quality_assessment"],
            int((time.time() - t0) * 1000),
        )
        return {**state, "quality_assessment": {"overall_confidence": 1.0}, "has_critical_issues": False}
    async def _node_write_output(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        docpkg_stream = f"docpkg-{app_id}"
        package_id = f"pkg-{app_id}"

        doc_events = await self.store.load_stream(docpkg_stream, from_position=0)
        extracted = [e for e in doc_events if e.get("event_type") == "ExtractionCompleted"]
        await self._append_stream(
            docpkg_stream,
            PackageReadyForAnalysis(
                package_id=package_id,
                application_id=app_id,
                documents_processed=len(extracted),
                has_quality_flags=False,
                quality_flag_count=0,
                ready_at=datetime.now(timezone.utc),
            ).to_store_dict(),
            causation_id=self.session_id,
        )
        await self._append_stream(
            f"loan-{app_id}",
            CreditAnalysisRequested(
                application_id=app_id,
                requested_at=datetime.now(timezone.utc),
                requested_by="document_processing_agent",
                priority="NORMAL",
            ).to_store_dict(),
            causation_id=self.session_id,
        )

        events_written = [
            {"stream_id": docpkg_stream, "event_type": "PackageReadyForAnalysis"},
            {"stream_id": f"loan-{app_id}", "event_type": "CreditAnalysisRequested"},
        ]
        await self._record_output_written(events_written, "Document package processed; credit analysis requested.")
        await self._record_node_execution("write_output", ["quality_assessment"], ["events_written"], int((time.time() - t0) * 1000))
        return {**state, "output_events_written": events_written, "next_agent_triggered": "credit_analysis"}


class FraudDetectionAgent(BaseApexAgent):
    """
    Detects inconsistencies between submitted documents and registry history.

    NODES TO IMPLEMENT:
        validate_inputs → load_document_facts → cross_reference_registry
        → analyze_fraud_patterns (LLM) → write_output

    LLM ROLE — in _node_analyze_fraud_patterns:
        Compare extracted current-year facts against historical_financials from registry.
        Flag: revenue_discrepancy (> 50% unexplained gap year-on-year),
              balance_sheet_inconsistency, unusual_submission_pattern.
        Compute fraud_score as weighted sum of anomaly severities.
        Return FraudAssessment JSON with named anomalies.
        RULE: fraud_score > 0.3 → must include at least one named anomaly with evidence.

    OUTPUT STREAMS:
        fraud-{id}: FraudScreeningInitiated, FraudAnomalyDetected (0+), FraudScreeningCompleted
        loan-{id}: ComplianceCheckRequested
    """
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            extracted_facts: dict | None; historical_financials: list | None
            company_profile: dict | None; fraud_assessment: dict | None
            errors: list; output_events_written: list; next_agent_triggered: str | None
        g = StateGraph(S)
        for name in ["validate_inputs","load_document_facts","cross_reference_registry","analyze_fraud_patterns","write_output"]:
            g.add_node(name, getattr(self, f"_node_{name}"))
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs","load_document_facts")
        g.add_edge("load_document_facts","cross_reference_registry")
        g.add_edge("cross_reference_registry","analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns","write_output")
        g.add_edge("write_output",END)
        return g.compile()

    async def _node_validate_inputs(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}", from_position=0)
        if not any(e.get("event_type") == "FraudScreeningRequested" for e in loan_events):
            await self._append_session(
                AgentInputValidationFailed(
                    session_id=self.session_id,
                    agent_type=self.agent_type,
                    application_id=app_id,
                    missing_inputs=["FraudScreeningRequested"],
                    validation_errors=["loan stream missing FraudScreeningRequested"],
                    failed_at=datetime.now(timezone.utc),
                ).to_store_dict()
            )
            raise ValueError("FraudScreeningRequested missing")

        await self._append_session(
            AgentInputValidated(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=app_id,
                inputs_validated=["FraudScreeningRequested"],
                validation_duration_ms=int((time.time() - t0) * 1000),
                validated_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

        fraud_stream = f"fraud-{app_id}"
        if await self.store.stream_version(fraud_stream) == -1:
            await self._append_stream(
                fraud_stream,
                FraudScreeningInitiated(
                    application_id=app_id,
                    session_id=self.session_id,
                    screening_model_version="fraud-stub",
                    initiated_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )

        await self._record_node_execution(
            "validate_inputs",
            ["application_id"],
            ["fraud_stream_initiated"],
            int((time.time() - t0) * 1000),
        )
        return state

    async def _node_load_document_facts(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        doc_events = await self.store.load_stream(f"docpkg-{app_id}", from_position=0)
        extracted = [e for e in doc_events if e.get("event_type") == "ExtractionCompleted"]
        await self._record_tool_call(
            "load_event_store_stream",
            f"docpkg-{app_id}",
            f"{len(extracted)} ExtractionCompleted",
            int((time.time() - t0) * 1000),
        )
        await self._record_node_execution(
            "load_document_facts",
            ["application_id"],
            ["extracted_facts"],
            int((time.time() - t0) * 1000),
        )
        return {**state, "extracted_facts": {"documents_extracted": len(extracted)}}

    async def _node_cross_reference_registry(self, state):
        t0 = time.time()
        company_profile: dict[str, Any] = {}
        historical_financials: list[Any] = []
        app_id = state["application_id"]
        if self.registry is not None:
            get_company = getattr(self.registry, "get_company", None)
            get_financial_history = getattr(self.registry, "get_financial_history", None)
            if callable(get_company):
                loan_events = await self.store.load_stream(f"loan-{app_id}", from_position=0)
                submitted = next((e for e in loan_events if e.get("event_type") == "ApplicationSubmitted"), None)
                company_id = (submitted or {}).get("payload", {}).get("applicant_id") if submitted else None
                if company_id:
                    prof = await get_company(str(company_id))
                    company_profile = prof.__dict__ if hasattr(prof, "__dict__") else (prof or {})
                    if callable(get_financial_history):
                        historical_financials = await get_financial_history(str(company_id))

        await self._record_tool_call("query_applicant_registry", "company+history", "loaded", int((time.time() - t0) * 1000))
        await self._record_node_execution(
            "cross_reference_registry",
            ["extracted_facts"],
            ["historical_financials"],
            int((time.time() - t0) * 1000),
        )
        return {
            **state,
            "company_profile": company_profile,
            "historical_financials": [h.__dict__ if hasattr(h, "__dict__") else h for h in (historical_financials or [])],
        }

    async def _node_analyze_fraud_patterns(self, state):
        t0 = time.time()
        docs_extracted = int((state.get("extracted_facts") or {}).get("documents_extracted") or 0)
        fraud_score = 0.05 if docs_extracted else 0.25
        assessment = {
            "fraud_score": fraud_score,
            "risk_level": "LOW" if fraud_score < 0.2 else "MEDIUM",
            "anomalies": [],
        }
        await self._record_node_execution(
            "analyze_fraud_patterns",
            ["extracted_facts", "historical_financials"],
            ["fraud_assessment"],
            int((time.time() - t0) * 1000),
        )
        return {**state, "fraud_assessment": assessment}

    async def _node_write_output(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        fraud_stream = f"fraud-{app_id}"
        assessment = state.get("fraud_assessment") or {}
        fraud_score = float(assessment.get("fraud_score") or 0.0)
        risk_level = str(assessment.get("risk_level") or "LOW")

        await self._append_stream(
            fraud_stream,
            FraudScreeningCompleted(
                application_id=app_id,
                session_id=self.session_id,
                fraud_score=fraud_score,
                risk_level=risk_level,
                anomalies_found=0,
                recommendation="PROCEED" if fraud_score < 0.3 else "FLAG_FOR_REVIEW",
                screening_model_version="fraud-stub",
                input_data_hash=self._sha({"app": app_id, "assessment": assessment}),
                completed_at=datetime.now(timezone.utc),
            ).to_store_dict(),
            causation_id=self.session_id,
        )
        await self._append_stream(
            f"loan-{app_id}",
            ComplianceCheckRequested(
                application_id=app_id,
                requested_at=datetime.now(timezone.utc),
                triggered_by_event_id=self._sha(self.session_id),
                regulation_set_version="REGSET-1",
                rules_to_evaluate=["REG-001", "REG-002", "REG-003", "REG-004", "REG-005", "REG-006"],
            ).to_store_dict(),
            causation_id=self.session_id,
        )

        events_written = [
            {"stream_id": fraud_stream, "event_type": "FraudScreeningCompleted"},
            {"stream_id": f"loan-{app_id}", "event_type": "ComplianceCheckRequested"},
        ]
        await self._record_output_written(events_written, f"Fraud screening complete: score={fraud_score:.2f} risk={risk_level}")
        await self._record_node_execution("write_output", ["fraud_assessment"], ["events_written"], int((time.time() - t0) * 1000))
        return {**state, "output_events_written": events_written, "next_agent_triggered": "compliance"}


class ComplianceAgent(BaseApexAgent):
    """
    Evaluates 6 deterministic regulatory rules. No LLM in decision path.

    NODES (6 rule nodes + bookend nodes):
        validate_inputs → check_reg001 → check_reg002 → check_reg003
        → check_reg004 → check_reg005 → check_reg006 → write_output

    Use conditional edges after each hard-block rule:
        graph.add_conditional_edges("check_reg002", self._should_continue,
                                     {"continue":"check_reg003","hard_block":"write_output"})

    RULE IMPLEMENTATIONS (deterministic — no LLM):
        REG-001: not any AML_WATCH flag is_active  → ComplianceRulePassed/Failed
        REG-002: not any SANCTIONS_REVIEW is_active → hard_block=True if failed
        REG-003: jurisdiction != "MT"               → hard_block=True if failed
        REG-004: not (Sole Proprietor AND >$250K)   → remediation_available=True if failed
        REG-005: founded_year <= 2022               → hard_block=True if failed
        REG-006: Always passes → ComplianceRuleNoted(CRA_CONSIDERATION)

    OUTPUT STREAMS:
        compliance-{id}: ComplianceCheckInitiated, ComplianceRulePassed/Failed/Noted (6x), ComplianceCheckCompleted
        loan-{id}: DecisionRequested (if CLEAR/CONDITIONAL) OR ApplicationDeclined (if BLOCKED)
    """
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            company_profile: dict | None; rules_results: list | None
            hard_block: bool | None; overall_verdict: str | None
            errors: list; output_events_written: list; next_agent_triggered: str | None
        g = StateGraph(S)
        for name in ["validate_inputs","check_reg001","check_reg002","check_reg003","check_reg004","check_reg005","check_reg006","write_output"]:
            g.add_node(name, getattr(self, f"_node_{name}"))
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs","check_reg001")
        g.add_edge("check_reg001","check_reg002")
        # REG-002 and REG-003 are hard blocks: conditional edge to write_output if failed
        g.add_conditional_edges("check_reg002", lambda s: "write_output" if s.get("hard_block") else "check_reg003")
        g.add_conditional_edges("check_reg003", lambda s: "write_output" if s.get("hard_block") else "check_reg004")
        g.add_edge("check_reg004","check_reg005")
        g.add_conditional_edges("check_reg005", lambda s: "write_output" if s.get("hard_block") else "check_reg006")
        g.add_edge("check_reg006","write_output")
        g.add_edge("write_output",END)
        return g.compile()

    async def _node_validate_inputs(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}", from_position=0)
        if not any(e.get("event_type") == "ComplianceCheckRequested" for e in loan_events):
            await self._append_session(
                AgentInputValidationFailed(
                    session_id=self.session_id,
                    agent_type=self.agent_type,
                    application_id=app_id,
                    missing_inputs=["ComplianceCheckRequested"],
                    validation_errors=["loan stream missing ComplianceCheckRequested"],
                    failed_at=datetime.now(timezone.utc),
                ).to_store_dict()
            )
            raise ValueError("ComplianceCheckRequested missing")

        await self._append_session(
            AgentInputValidated(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=app_id,
                inputs_validated=["ComplianceCheckRequested"],
                validation_duration_ms=int((time.time() - t0) * 1000),
                validated_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

        # Best-effort company profile (used for deterministic rules).
        company_profile: dict[str, Any] = {}
        submitted = next((e for e in loan_events if e.get("event_type") == "ApplicationSubmitted"), None)
        company_id = (submitted or {}).get("payload", {}).get("applicant_id") if submitted else None
        if company_id and self.registry is not None:
            get_company = getattr(self.registry, "get_company", None)
            if callable(get_company):
                prof = await get_company(str(company_id))
                company_profile = prof.__dict__ if hasattr(prof, "__dict__") else (prof or {})

        compliance_stream = f"compliance-{app_id}"
        if await self.store.stream_version(compliance_stream) == -1:
            await self._append_stream(
                compliance_stream,
                ComplianceCheckInitiated(
                    application_id=app_id,
                    session_id=self.session_id,
                    regulation_set_version="REGSET-1",
                    rules_to_evaluate=["REG-001", "REG-002", "REG-003", "REG-004", "REG-005", "REG-006"],
                    initiated_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )

        await self._record_node_execution(
            "validate_inputs",
            ["application_id"],
            ["company_profile"],
            int((time.time() - t0) * 1000),
        )
        return {**state, "company_profile": company_profile, "rules_results": [], "hard_block": False}

    async def _node_check_reg001(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        compliance_stream = f"compliance-{app_id}"
        await self._append_stream(
            compliance_stream,
            ComplianceRulePassed(
                application_id=app_id,
                session_id=self.session_id,
                rule_id="REG-001",
                rule_name="BSA / AML Watch",
                rule_version="1",
                evidence_hash=self._sha({"rule": "REG-001"}),
                evaluation_notes="No active AML_WATCH flags (stub).",
                evaluated_at=datetime.now(timezone.utc),
            ).to_store_dict(),
            causation_id=self.session_id,
        )
        await self._record_node_execution("check_reg001", ["company_profile"], ["rules_results"], int((time.time() - t0) * 1000))
        state["rules_results"].append({"rule_id": "REG-001", "passed": True})
        return state

    async def _node_check_reg002(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        compliance_stream = f"compliance-{app_id}"
        await self._append_stream(
            compliance_stream,
            ComplianceRulePassed(
                application_id=app_id,
                session_id=self.session_id,
                rule_id="REG-002",
                rule_name="OFAC Sanctions Review",
                rule_version="1",
                evidence_hash=self._sha({"rule": "REG-002"}),
                evaluation_notes="No active SANCTIONS_REVIEW flags (stub).",
                evaluated_at=datetime.now(timezone.utc),
            ).to_store_dict(),
            causation_id=self.session_id,
        )
        await self._record_node_execution("check_reg002", ["company_profile"], ["rules_results"], int((time.time() - t0) * 1000))
        state["rules_results"].append({"rule_id": "REG-002", "passed": True})
        return state

    async def _node_check_reg003(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        compliance_stream = f"compliance-{app_id}"
        jurisdiction = str((state.get("company_profile") or {}).get("jurisdiction") or "")
        if jurisdiction == "MT":
            state["hard_block"] = True
            await self._append_stream(
                compliance_stream,
                ComplianceRuleFailed(
                    application_id=app_id,
                    session_id=self.session_id,
                    rule_id="REG-003",
                    rule_name="Jurisdiction Restriction",
                    rule_version="1",
                    failure_reason="Jurisdiction MT is not supported",
                    is_hard_block=True,
                    remediation_available=False,
                    remediation_description=None,
                    evidence_hash=self._sha({"jurisdiction": jurisdiction}),
                    evaluated_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
            state["rules_results"].append({"rule_id": "REG-003", "passed": False, "hard_block": True})
        else:
            await self._append_stream(
                compliance_stream,
                ComplianceRulePassed(
                    application_id=app_id,
                    session_id=self.session_id,
                    rule_id="REG-003",
                    rule_name="Jurisdiction Restriction",
                    rule_version="1",
                    evidence_hash=self._sha({"jurisdiction": jurisdiction}),
                    evaluation_notes="Jurisdiction allowed.",
                    evaluated_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
            state["rules_results"].append({"rule_id": "REG-003", "passed": True})
        await self._record_node_execution("check_reg003", ["company_profile"], ["hard_block"], int((time.time() - t0) * 1000))
        return state

    async def _node_check_reg004(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        compliance_stream = f"compliance-{app_id}"
        legal_type = str((state.get("company_profile") or {}).get("legal_type") or "")
        loan_events = await self.store.load_stream(f"loan-{app_id}", from_position=0)
        submitted = next((e for e in loan_events if e.get("event_type") == "ApplicationSubmitted"), None)
        amount = float((submitted or {}).get("payload", {}).get("requested_amount_usd") or 0.0)
        fail = ("sole" in legal_type.lower()) and amount > 250_000
        if fail:
            await self._append_stream(
                compliance_stream,
                ComplianceRuleFailed(
                    application_id=app_id,
                    session_id=self.session_id,
                    rule_id="REG-004",
                    rule_name="Legal Type Threshold",
                    rule_version="1",
                    failure_reason="Sole proprietor requests above threshold",
                    is_hard_block=False,
                    remediation_available=True,
                    remediation_description="Require additional guarantees / documentation",
                    evidence_hash=self._sha({"legal_type": legal_type, "amount": amount}),
                    evaluated_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
            state["rules_results"].append({"rule_id": "REG-004", "passed": False})
        else:
            await self._append_stream(
                compliance_stream,
                ComplianceRulePassed(
                    application_id=app_id,
                    session_id=self.session_id,
                    rule_id="REG-004",
                    rule_name="Legal Type Threshold",
                    rule_version="1",
                    evidence_hash=self._sha({"legal_type": legal_type, "amount": amount}),
                    evaluation_notes="Legal type/amount within policy.",
                    evaluated_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
            state["rules_results"].append({"rule_id": "REG-004", "passed": True})
        await self._record_node_execution("check_reg004", ["company_profile"], ["rules_results"], int((time.time() - t0) * 1000))
        return state

    async def _node_check_reg005(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        compliance_stream = f"compliance-{app_id}"
        founded_year = int((state.get("company_profile") or {}).get("founded_year") or 0)
        if founded_year and founded_year > 2022:
            state["hard_block"] = True
            await self._append_stream(
                compliance_stream,
                ComplianceRuleFailed(
                    application_id=app_id,
                    session_id=self.session_id,
                    rule_id="REG-005",
                    rule_name="Operating History",
                    rule_version="1",
                    failure_reason="Insufficient operating history",
                    is_hard_block=True,
                    remediation_available=False,
                    remediation_description=None,
                    evidence_hash=self._sha({"founded_year": founded_year}),
                    evaluated_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
            state["rules_results"].append({"rule_id": "REG-005", "passed": False, "hard_block": True})
        else:
            await self._append_stream(
                compliance_stream,
                ComplianceRulePassed(
                    application_id=app_id,
                    session_id=self.session_id,
                    rule_id="REG-005",
                    rule_name="Operating History",
                    rule_version="1",
                    evidence_hash=self._sha({"founded_year": founded_year}),
                    evaluation_notes="Operating history sufficient.",
                    evaluated_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
            state["rules_results"].append({"rule_id": "REG-005", "passed": True})
        await self._record_node_execution("check_reg005", ["company_profile"], ["hard_block"], int((time.time() - t0) * 1000))
        return state

    async def _node_check_reg006(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        compliance_stream = f"compliance-{app_id}"
        await self._append_stream(
            compliance_stream,
            ComplianceRuleNoted(
                application_id=app_id,
                session_id=self.session_id,
                rule_id="REG-006",
                rule_name="CRA Consideration",
                note_type="CRA_CONSIDERATION",
                note_text="CRA noted (stub).",
                evaluated_at=datetime.now(timezone.utc),
            ).to_store_dict(),
            causation_id=self.session_id,
        )
        await self._record_node_execution("check_reg006", ["company_profile"], ["rules_results"], int((time.time() - t0) * 1000))
        state["rules_results"].append({"rule_id": "REG-006", "noted": True})
        return state

    async def _node_write_output(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        compliance_stream = f"compliance-{app_id}"
        hard_block = bool(state.get("hard_block"))
        overall = ComplianceVerdict.BLOCKED if hard_block else ComplianceVerdict.CLEAR

        passed = len([r for r in (state.get("rules_results") or []) if r.get("passed") is True])
        failed = len([r for r in (state.get("rules_results") or []) if r.get("passed") is False])
        noted = len([r for r in (state.get("rules_results") or []) if r.get("noted") is True])

        await self._append_stream(
            compliance_stream,
            ComplianceCheckCompleted(
                application_id=app_id,
                session_id=self.session_id,
                rules_evaluated=passed + failed + noted,
                rules_passed=passed,
                rules_failed=failed,
                rules_noted=noted,
                has_hard_block=hard_block,
                overall_verdict=overall,
                completed_at=datetime.now(timezone.utc),
            ).to_store_dict(),
            causation_id=self.session_id,
        )

        if hard_block:
            await self._append_stream(
                f"loan-{app_id}",
                ApplicationDeclined(
                    application_id=app_id,
                    decline_reasons=["Compliance hard block"],
                    declined_by="compliance_agent",
                    adverse_action_notice_required=True,
                    adverse_action_codes=["COMPLIANCE_BLOCKED"],
                    declined_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
            next_agent = None
        else:
            await self._append_stream(
                f"loan-{app_id}",
                DecisionRequested(
                    application_id=app_id,
                    requested_at=datetime.now(timezone.utc),
                    all_analyses_complete=True,
                    triggered_by_event_id=self._sha(self.session_id),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
            next_agent = "decision_orchestrator"

        events_written = [
            {"stream_id": compliance_stream, "event_type": "ComplianceCheckCompleted"},
            {"stream_id": f"loan-{app_id}", "event_type": "ApplicationDeclined" if hard_block else "DecisionRequested"},
        ]
        await self._record_output_written(events_written, f"Compliance verdict: {overall.value}")
        await self._record_node_execution("write_output", ["rules_results"], ["events_written"], int((time.time() - t0) * 1000))
        return {**state, "output_events_written": events_written, "next_agent_triggered": next_agent}


class DecisionOrchestratorAgent(BaseApexAgent):
    """
    Synthesises all prior agent outputs. Reads from ALL prior agent streams.

    NODES:
        validate_inputs → load_all_analyses → synthesize_decision (LLM)
        → apply_hard_constraints → write_output

    READS FROM:
        credit-{id}: CreditAnalysisCompleted (risk_tier, confidence, limit)
        fraud-{id}: FraudScreeningCompleted (fraud_score, anomalies)
        compliance-{id}: ComplianceCheckCompleted (overall_verdict)

    HARD CONSTRAINTS (Python — not LLM, in apply_hard_constraints):
        1. compliance BLOCKED → must DECLINE regardless of LLM
        2. confidence < 0.60 → must REFER
        3. fraud_score > 0.60 → must REFER
        4. risk_tier HIGH AND confidence >= 0.70 → DECLINE eligible

    LLM ROLE (synthesize_decision):
        Given all 3 analyses, produce executive_summary and key_risks.
        Initial recommendation (may be overridden by hard constraints).
        Return OrchestratorDecision JSON.

    OUTPUT STREAMS:
        loan-{id}: DecisionGenerated
        loan-{id}: ApplicationApproved (if APPROVE) OR ApplicationDeclined (if DECLINE)
                   OR HumanReviewRequested (if REFER)
    """
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            credit_analysis: dict | None; fraud_screening: dict | None
            compliance_record: dict | None; orchestrator_decision: dict | None
            errors: list; output_events_written: list; next_agent_triggered: str | None
        g = StateGraph(S)
        for name in ["validate_inputs","load_all_analyses","synthesize_decision","apply_hard_constraints","write_output"]:
            g.add_node(name, getattr(self, f"_node_{name}"))
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs","load_all_analyses")
        g.add_edge("load_all_analyses","synthesize_decision")
        g.add_edge("synthesize_decision","apply_hard_constraints")
        g.add_edge("apply_hard_constraints","write_output")
        g.add_edge("write_output",END)
        return g.compile()

    async def _node_validate_inputs(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}", from_position=0)
        if not any(e.get("event_type") == "DecisionRequested" for e in loan_events):
            await self._append_session(
                AgentInputValidationFailed(
                    session_id=self.session_id,
                    agent_type=self.agent_type,
                    application_id=app_id,
                    missing_inputs=["DecisionRequested"],
                    validation_errors=["loan stream missing DecisionRequested"],
                    failed_at=datetime.now(timezone.utc),
                ).to_store_dict()
            )
            raise ValueError("DecisionRequested missing")

        await self._append_session(
            AgentInputValidated(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=app_id,
                inputs_validated=["DecisionRequested"],
                validation_duration_ms=int((time.time() - t0) * 1000),
                validated_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )
        await self._record_node_execution("validate_inputs", ["application_id"], ["decision_requested"], int((time.time() - t0) * 1000))
        return state

    async def _node_load_all_analyses(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        credit_events = await self.store.load_stream(f"credit-{app_id}", from_position=0)
        fraud_events = await self.store.load_stream(f"fraud-{app_id}", from_position=0)
        compliance_events = await self.store.load_stream(f"compliance-{app_id}", from_position=0)

        credit = next((e for e in reversed(credit_events) if e.get("event_type") == "CreditAnalysisCompleted"), None)
        fraud = next((e for e in reversed(fraud_events) if e.get("event_type") == "FraudScreeningCompleted"), None)
        compliance = next((e for e in reversed(compliance_events) if e.get("event_type") == "ComplianceCheckCompleted"), None)
        if credit is None or fraud is None or compliance is None:
            raise ValueError("missing analyses: credit/fraud/compliance")

        await self._record_node_execution(
            "load_all_analyses",
            ["application_id"],
            ["credit_analysis", "fraud_screening", "compliance_record"],
            int((time.time() - t0) * 1000),
        )
        return {**state, "credit_analysis": credit["payload"], "fraud_screening": fraud["payload"], "compliance_record": compliance["payload"]}

    async def _node_synthesize_decision(self, state):
        t0 = time.time()
        credit = state.get("credit_analysis") or {}
        fraud = state.get("fraud_screening") or {}
        compliance = state.get("compliance_record") or {}

        conf = float(((credit.get("decision") or {}).get("confidence")) or 0.0)
        risk_tier = str(((credit.get("decision") or {}).get("risk_tier")) or "MEDIUM")
        fraud_score = float(fraud.get("fraud_score") or 0.0)
        verdict = str(compliance.get("overall_verdict") or "CLEAR")

        rec = "APPROVE" if verdict == "CLEAR" and fraud_score < 0.3 and conf >= 0.6 and risk_tier != "HIGH" else "REFER"
        summary = f"Credit {risk_tier} (conf={conf:.2f}); fraud={fraud_score:.2f}; compliance={verdict}."
        decision = {"recommendation": rec, "confidence": conf, "executive_summary": summary, "key_risks": []}

        await self._record_node_execution(
            "synthesize_decision",
            ["credit_analysis", "fraud_screening", "compliance_record"],
            ["orchestrator_decision"],
            int((time.time() - t0) * 1000),
        )
        return {**state, "orchestrator_decision": decision}

    async def _node_apply_hard_constraints(self, state):
        t0 = time.time()
        credit = state.get("credit_analysis") or {}
        fraud = state.get("fraud_screening") or {}
        compliance = state.get("compliance_record") or {}
        od = dict(state.get("orchestrator_decision") or {})

        verdict = str(compliance.get("overall_verdict") or "CLEAR")
        fraud_score = float(fraud.get("fraud_score") or 0.0)
        decision = credit.get("decision") or {}
        conf = float(decision.get("confidence") or 0.0)
        risk_tier = str(decision.get("risk_tier") or "MEDIUM")

        if verdict == "BLOCKED":
            od["recommendation"] = "DECLINE"
        elif conf < 0.60 or fraud_score > 0.60:
            od["recommendation"] = "REFER"
        elif risk_tier == "HIGH" and conf >= 0.70:
            od["recommendation"] = "DECLINE"
        elif od.get("recommendation") not in {"APPROVE", "DECLINE", "REFER"}:
            od["recommendation"] = "REFER"

        await self._record_node_execution(
            "apply_hard_constraints",
            ["orchestrator_decision"],
            ["orchestrator_decision"],
            int((time.time() - t0) * 1000),
        )
        return {**state, "orchestrator_decision": od}

    async def _node_write_output(self, state):
        t0 = time.time()
        app_id = state["application_id"]
        loan_stream = f"loan-{app_id}"
        credit = state.get("credit_analysis") or {}
        decision = credit.get("decision") or {}
        od = state.get("orchestrator_decision") or {}
        rec = str(od.get("recommendation") or "REFER")
        conf = float(od.get("confidence") or float(decision.get("confidence") or 0.0))
        approved_amt = Decimal(str(int(decision.get("recommended_limit_usd") or 0))) if rec == "APPROVE" else None

        await self._append_stream(
            loan_stream,
            DecisionGenerated(
                application_id=app_id,
                orchestrator_session_id=self.session_id,
                recommendation=rec,
                confidence=conf,
                approved_amount_usd=approved_amt,
                conditions=["Monthly financial reporting required", "Personal guarantee from principal"] if rec == "APPROVE" else [],
                executive_summary=str(od.get("executive_summary") or ""),
                key_risks=list(od.get("key_risks") or []),
                contributing_sessions=[],
                model_versions={"orchestrator": self.model},
                generated_at=datetime.now(timezone.utc),
            ).to_store_dict(),
            causation_id=self.session_id,
        )

        followup_event_type: str | None = None
        if rec == "APPROVE" and approved_amt is not None:
            await self._append_stream(
                loan_stream,
                ApplicationApproved(
                    application_id=app_id,
                    approved_amount_usd=approved_amt,
                    interest_rate_pct=8.5,
                    term_months=36,
                    conditions=["Monthly financial reporting required"],
                    approved_by="decision_orchestrator",
                    effective_date=datetime.now(timezone.utc).date().isoformat(),
                    approved_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
            followup_event_type = "ApplicationApproved"
        elif rec == "DECLINE":
            await self._append_stream(
                loan_stream,
                ApplicationDeclined(
                    application_id=app_id,
                    decline_reasons=["Risk/compliance constraints"],
                    declined_by="decision_orchestrator",
                    adverse_action_notice_required=True,
                    adverse_action_codes=["POLICY_DECLINE"],
                    declined_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
            followup_event_type = "ApplicationDeclined"
        else:
            await self._append_stream(
                loan_stream,
                HumanReviewRequested(
                    application_id=app_id,
                    reason="Referred by hard constraints",
                    decision_event_id=self._sha(self.session_id),
                    assigned_to=None,
                    requested_at=datetime.now(timezone.utc),
                ).to_store_dict(),
                causation_id=self.session_id,
            )
            followup_event_type = "HumanReviewRequested"

        events_written = [
            {"stream_id": loan_stream, "event_type": "DecisionGenerated"},
            {"stream_id": loan_stream, "event_type": followup_event_type},
        ]
        await self._record_output_written(events_written, f"Decision: {rec} (confidence={conf:.2f})")
        await self._record_node_execution("write_output", ["orchestrator_decision"], ["events_written"], int((time.time() - t0) * 1000))
        return {**state, "output_events_written": events_written, "next_agent_triggered": None}
