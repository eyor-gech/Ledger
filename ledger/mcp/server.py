from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg
from fastmcp import FastMCP
from anthropic import AsyncAnthropic

from ledger.agents import (
    ComplianceAgent,
    CreditAnalysisAgent,
    DecisionOrchestratorAgent,
    DocumentProcessingAgent,
    FraudDetectionAgent,
)
from ledger.agents.testing import FakeAnthropicClient
from ledger.audit import run_integrity_check
from ledger.domain.handlers import (
    HumanReviewCompletedCommand,
    RequestComplianceCheckCommand,
    RequestCreditAnalysisCommand,
    SubmitApplicationCommand,
    handle_human_review_completed,
    handle_request_compliance_check,
    handle_request_credit_analysis,
    handle_submit_application,
)
from ledger.domain.aggregates.loan_application import LoanApplication
from ledger.domain.errors import DomainError, InvariantViolation
from ledger.event_store import EventStore, OptimisticConcurrencyError
from ledger.registry.client import ApplicantRegistryClient
from ledger.schema.events import (
    AgentType,
    DocumentFormat,
    DocumentType,
    DocumentUploaded,
    LoanPurpose,
    StoredEvent,
)
from ledger.upcasters import UpcasterRegistry


def create_mcp_server(*, db_url: str | None = None) -> FastMCP:
    """
    FastMCP server exposing Phase 5 tools/resources.

    Tools:
      - submit_application
      - record_credit_analysis (requests credit analysis)
      - trigger_compliance_check

    Resources:
      - ledger://applications/{application_id}
      - ledger://audit/{application_id}/temporal?as_of=...
    """
    url = db_url or os.environ.get("DATABASE_URL") or "postgresql://localhost/apex_ledger"
    mcp = FastMCP(name="apex-ledger")

    def _err(*, error_type: str, message: str, context: dict[str, Any] | None = None, suggested_action: str = "") -> dict[str, Any]:
        return {
            "error_type": str(error_type),
            "message": str(message),
            "context": dict(context or {}),
            "suggested_action": str(suggested_action),
        }

    def _require_causal_chain(correlation_id: str | None, causation_id: str | None) -> dict[str, Any] | None:
        if not correlation_id or not str(correlation_id).strip():
            return _err(
                error_type="MissingCorrelationId",
                message="correlation_id is required for auditability",
                suggested_action="Provide correlation_id (usually the application_id).",
            )
        if not causation_id or not str(causation_id).strip():
            return _err(
                error_type="MissingCausationId",
                message="causation_id is required for causal chain reconstruction",
                suggested_action="Provide causation_id (usually the triggering event id or session id).",
            )
        return None

    def _llm_client() -> Any:
        key = os.getenv("ANTHROPIC_API_KEY")
        if key:
            return AsyncAnthropic(api_key=key)
        # Test/demo fallback: deterministic fake client.
        return FakeAnthropicClient()

    async def _deps() -> tuple[EventStore, asyncpg.Pool, ApplicantRegistryClient, Any]:
        store = EventStore(url, upcaster_registry=UpcasterRegistry())
        await store.connect()
        pool = await asyncpg.create_pool(url, min_size=1, max_size=5)
        registry = ApplicantRegistryClient(pool)
        client = _llm_client()
        return store, pool, registry, client

    async def _load_loan_aggregate(store: EventStore, application_id: str) -> LoanApplication:
        rows = await store.load_stream(f"loan-{application_id}", from_position=0)
        if not rows:
            raise InvariantViolation(f"missing stream: loan-{application_id}")
        stored = [StoredEvent.from_row(r) for r in rows]
        return LoanApplication.rebuild(stored)  # type: ignore[arg-type]

    @mcp.tool(name="submit_application")
    async def submit_application(
        application_id: str,
        applicant_id: str,
        requested_amount_usd: float,
        loan_purpose: str,
        loan_term_months: int = 36,
        submission_channel: str = "mcp",
        contact_email: str = "applicant@example.com",
        contact_name: str = "Applicant",
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        missing = _require_causal_chain(correlation_id, causation_id)
        if missing:
            return missing

        store, pool, registry, _ = await _deps()
        try:
            company = await registry.get_company(applicant_id)
            if company is None:
                return _err(
                    error_type="ApplicantNotFound",
                    message=f"Applicant registry has no company_id={applicant_id!r}",
                    context={"applicant_id": applicant_id},
                    suggested_action="Seed the applicant registry (datagen) or use a valid applicant_id.",
                )
            cmd = SubmitApplicationCommand(
                correlation_id=str(correlation_id),
                causation_id=str(causation_id),
                application_id=application_id,
                applicant_id=applicant_id,
                requested_amount_usd=Decimal(str(requested_amount_usd)),
                loan_purpose=LoanPurpose(str(loan_purpose)),
                loan_term_months=int(loan_term_months),
                submission_channel=str(submission_channel),
                contact_email=str(contact_email),
                contact_name=str(contact_name),
                required_document_types=[
                    DocumentType.APPLICATION_PROPOSAL,
                    DocumentType.INCOME_STATEMENT,
                    DocumentType.BALANCE_SHEET,
                ],
            )
            await handle_submit_application(store, cmd)
            return {"ok": True, "application_id": application_id}
        except DomainError as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        except Exception as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        finally:
            await pool.close()
            await store.close()

    @mcp.tool(name="record_credit_analysis")
    async def record_credit_analysis(
        application_id: str,
        requested_by: str = "mcp",
        request_only: bool = False,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        missing = _require_causal_chain(correlation_id, causation_id)
        if missing:
            return missing

        store, pool, registry, client = await _deps()
        try:
            if request_only:
                positions = await handle_request_credit_analysis(
                    store,
                    RequestCreditAnalysisCommand(
                        correlation_id=str(correlation_id),
                        causation_id=str(causation_id),
                        application_id=application_id,
                        requested_by=requested_by,
                    ),
                )
                return {"ok": True, "positions": positions}

            agent = CreditAnalysisAgent(
                agent_id="mcp-credit",
                agent_type=AgentType.CREDIT_ANALYSIS,
                store=store,
                registry=registry,
                llm_client=client,
                model_version=os.getenv("CREDIT_ANALYSIS_MODEL", "claude-3-5-haiku-latest"),
            )
            result = await agent.process_application(application_id)
            return {"ok": True, "session_id": agent.session_id, "result": result}
        except (InvariantViolation, OptimisticConcurrencyError) as exc:
            return _err(
                error_type=type(exc).__name__,
                message=str(exc),
                context={"application_id": application_id},
                suggested_action="Reload state and retry with the latest causal/expected version.",
            )
        except DomainError as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        except Exception as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        finally:
            await pool.close()
            await store.close()

    @mcp.tool(name="start_agent_session")
    async def start_agent_session(
        application_id: str,
        agent_type: str = "document_processing",
        resume_if_possible: bool = True,
        simulate_crash_after_node: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Preconditions:
        - `correlation_id` and `causation_id` provided.
        - The application stream exists (submit_application has run).
        """
        missing = _require_causal_chain(correlation_id, causation_id)
        if missing:
            return missing

        store, pool, registry, client = await _deps()
        try:
            # Minimal precondition: application exists.
            evs = await store.load_stream(f"loan-{application_id}", from_position=0)
            if not evs:
                return _err(
                    error_type="PreconditionFailed",
                    message="LoanApplication stream missing",
                    context={"stream_id": f"loan-{application_id}"},
                    suggested_action="Call submit_application first.",
                )

            # For DocumentProcessing, ensure required DocumentUploaded events exist by attaching from the local corpus.
            if str(agent_type) == "document_processing":
                loan = await _load_loan_aggregate(store, application_id)
                applicant_id = str(loan.applicant_id or "")
                if applicant_id:
                    required = {
                        DocumentType.APPLICATION_PROPOSAL.value: "application_proposal.pdf",
                        DocumentType.INCOME_STATEMENT.value: "income_statement_2024.pdf",
                        DocumentType.BALANCE_SHEET.value: "balance_sheet_2024.pdf",
                    }
                    uploaded_types = {
                        str((e.get("payload") or {}).get("document_type") or "")
                        for e in evs
                        if str(e.get("event_type")) == "DocumentUploaded"
                    }
                    missing = [dt for dt in required.keys() if dt not in uploaded_types]
                    if missing:
                        base = Path(__file__).resolve().parents[2] / "documents" / applicant_id
                        events_to_add: list[dict[str, Any]] = []
                        now = datetime.now(timezone.utc)
                        for dt in missing:
                            filename = required[dt]
                            file_path = str((base / filename).as_posix())
                            events_to_add.append(
                                DocumentUploaded(
                                    application_id=application_id,
                                    document_id=f"doc-{uuid4().hex[:8]}",
                                    document_type=DocumentType(dt),
                                    document_format=DocumentFormat.PDF,
                                    filename=filename,
                                    file_path=file_path,
                                    file_size_bytes=0,
                                    file_hash="",
                                    fiscal_year=2024,
                                    uploaded_at=now,
                                    uploaded_by="mcp:auto",
                                ).to_store_dict()
                            )
                        expected = await store.stream_version(f"loan-{application_id}")
                        await store.append(
                            f"loan-{application_id}",
                            events_to_add,
                            expected_version=expected,
                            correlation_id=str(correlation_id),
                            causation_id=str(causation_id),
                        )
                        evs = await store.load_stream(f"loan-{application_id}", from_position=0)

            agent_type_norm = str(agent_type)
            agent_map = {
                "document_processing": (DocumentProcessingAgent, AgentType.DOCUMENT_PROCESSING),
                "credit_analysis": (CreditAnalysisAgent, AgentType.CREDIT_ANALYSIS),
                "fraud_detection": (FraudDetectionAgent, AgentType.FRAUD_DETECTION),
                "compliance": (ComplianceAgent, AgentType.COMPLIANCE),
                "decision_orchestrator": (DecisionOrchestratorAgent, AgentType.DECISION_ORCHESTRATOR),
            }
            entry = agent_map.get(agent_type_norm)
            if entry is None:
                return _err(
                    error_type="InvalidAgentType",
                    message=f"Unknown agent_type={agent_type_norm!r}",
                    context={"agent_type": agent_type_norm},
                    suggested_action=f"Use one of: {sorted(agent_map.keys())}.",
                )
            cls, at = entry

            agent = cls(
                agent_id=f"mcp-{agent_type_norm}",
                agent_type=at,
                store=store,
                registry=registry,
                llm_client=client,
                model_version=os.getenv("AGENT_MODEL", "claude-3-5-haiku-latest"),
            )
            result = await agent.process_application(
                application_id,
                resume_if_possible=bool(resume_if_possible),
                simulate_crash_after_node=simulate_crash_after_node,
            )
            return {"ok": True, "session_id": agent.session_id, "agent_type": agent_type_norm, "result": result}
        except Exception as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        finally:
            await pool.close()
            await store.close()

    @mcp.tool(name="record_fraud_screening")
    async def record_fraud_screening(
        application_id: str,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Preconditions:
        - Application state is `FRAUD_SCREENING_REQUESTED`.
        """
        missing = _require_causal_chain(correlation_id, causation_id)
        if missing:
            return missing

        store, pool, registry, client = await _deps()
        try:
            # Guard at aggregate boundary.
            loan_events = await store.load_stream(f"loan-{application_id}", from_position=0)
            if not loan_events:
                raise InvariantViolation(f"missing stream: loan-{application_id}")
            loan = await _load_loan_aggregate(store, application_id)
            loan.guard_can_request_fraud_screening()

            agent = FraudDetectionAgent(
                agent_id="mcp-fraud",
                agent_type=AgentType.FRAUD_DETECTION,
                store=store,
                registry=registry,
                llm_client=client,
                model_version=os.getenv("FRAUD_MODEL", "claude-3-5-haiku-latest"),
            )
            result = await agent.process_application(application_id)
            return {"ok": True, "session_id": agent.session_id, "result": result}
        except DomainError as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        except Exception as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        finally:
            await pool.close()
            await store.close()

    @mcp.tool(name="record_compliance_check")
    async def record_compliance_check(
        application_id: str,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Preconditions:
        - Application state is `COMPLIANCE_CHECK_REQUESTED`.
        """
        missing = _require_causal_chain(correlation_id, causation_id)
        if missing:
            return missing

        store, pool, registry, client = await _deps()
        try:
            loan_events = await store.load_stream(f"loan-{application_id}", from_position=0)
            if not loan_events:
                raise InvariantViolation(f"missing stream: loan-{application_id}")
            loan = await _load_loan_aggregate(store, application_id)
            loan.guard_can_request_compliance_check()

            agent = ComplianceAgent(
                agent_id="mcp-compliance",
                agent_type=AgentType.COMPLIANCE,
                store=store,
                registry=registry,
                llm_client=client,
                model_version=os.getenv("COMPLIANCE_MODEL", "claude-3-5-haiku-latest"),
            )
            result = await agent.process_application(application_id)
            return {"ok": True, "session_id": agent.session_id, "result": result}
        except DomainError as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        except Exception as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        finally:
            await pool.close()
            await store.close()

    # Back-compat alias
    @mcp.tool(name="trigger_compliance_check")
    async def trigger_compliance_check(
        application_id: str,
        triggered_by_event_id: str,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        # This tool historically requested compliance; keep it as a shim.
        return await record_compliance_check(
            application_id=application_id,
            correlation_id=correlation_id,
            causation_id=causation_id or triggered_by_event_id,
        )

    @mcp.tool(name="generate_decision")
    async def generate_decision(
        application_id: str,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Preconditions:
        - Compliance check completed (application state is `COMPLIANCE_CHECK_COMPLETE`).
        """
        missing = _require_causal_chain(correlation_id, causation_id)
        if missing:
            return missing

        store, pool, registry, client = await _deps()
        try:
            agent = DecisionOrchestratorAgent(
                agent_id="mcp-decision",
                agent_type=AgentType.DECISION_ORCHESTRATOR,
                store=store,
                registry=registry,
                llm_client=client,
                model_version=os.getenv("DECISION_MODEL", "claude-3-5-haiku-latest"),
            )
            result = await agent.process_application(application_id)
            return {"ok": True, "session_id": agent.session_id, "result": result}
        except DomainError as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        except Exception as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        finally:
            await pool.close()
            await store.close()

    @mcp.tool(name="record_human_review")
    async def record_human_review(
        application_id: str,
        reviewer_id: str,
        final_decision: str,
        notes: str = "",
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Preconditions:
        - Application state is `PENDING_HUMAN_REVIEW`.
        """
        missing = _require_causal_chain(correlation_id, causation_id)
        if missing:
            return missing

        store, pool, _, _ = await _deps()
        try:
            cmd = HumanReviewCompletedCommand(
                correlation_id=str(correlation_id),
                causation_id=str(causation_id),
                application_id=application_id,
                reviewer_id=reviewer_id,
                final_decision=final_decision,
                notes=notes,
            )
            positions = await handle_human_review_completed(store, cmd)
            return {"ok": True, "positions": positions}
        except DomainError as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        except Exception as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"application_id": application_id})
        finally:
            await pool.close()
            await store.close()

    @mcp.tool(name="run_integrity_check")
    async def run_integrity_check_tool(
        entity_type: str,
        entity_id: str,
        stream_id: str,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Preconditions:
        - `stream_id` exists.
        """
        missing = _require_causal_chain(correlation_id, causation_id)
        if missing:
            return missing

        store, pool, _, _ = await _deps()
        try:
            res = await run_integrity_check(
                store,
                entity_type=entity_type,
                entity_id=entity_id,
                stream_id=stream_id,
                correlation_id=str(correlation_id),
                causation_id=str(causation_id),
            )
            return {"ok": True, "result": res.__dict__}
        except Exception as exc:
            return _err(error_type=type(exc).__name__, message=str(exc), context={"stream_id": stream_id})
        finally:
            await pool.close()
            await store.close()

    @mcp.resource("ledger://applications/{application_id}")
    async def application_resource(application_id: str) -> dict[str, Any]:
        store, pool, _, _ = await _deps()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM application_summary WHERE application_id=$1",
                    application_id,
                )
            if row is None:
                # Fallback to raw events
                events = await store.load_stream(f"loan-{application_id}", from_position=0)
                return {"application_id": application_id, "events": events}
            return dict(row)
        finally:
            await pool.close()
            await store.close()

    @mcp.resource("ledger://applications/{application_id}/compliance/{as_of}")
    async def compliance_resource(application_id: str, as_of: str) -> str:
        """
        Returns compliance projection + temporal snapshot (if as_of provided).
        """
        store, pool, _, _ = await _deps()
        try:
            events = await store.load_stream(f"compliance-{application_id}", from_position=0)
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM compliance_audit WHERE application_id=$1",
                    application_id,
                )
            result: dict[str, Any] = {
                "application_id": application_id,
                "projection": dict(row) if row else None,
                "events": events,
            }
            if as_of:
                from ledger.projections.views import ComplianceAuditView

                view = ComplianceAuditView(store=store, pool=pool)
                snap = await view.get_compliance_at(application_id=application_id, timestamp=as_of)
                result["as_of"] = as_of
                result["temporal"] = snap.__dict__
            return result
        finally:
            await pool.close()
            await store.close()

    @mcp.resource("ledger://audit/{application_id}/temporal/{as_of}")
    async def audit_temporal_resource(application_id: str, as_of: str) -> str:
        """
        Temporal query (best-effort): returns loan stream events up to `as_of` (ISO timestamp).
        When `as_of` is omitted, returns full stream.
        """
        store, pool, _, _ = await _deps()
        try:
            events = await store.load_stream(f"loan-{application_id}", from_position=0)
            if as_of:
                cutoff = datetime.fromisoformat(as_of)
                if cutoff.tzinfo is None:
                    cutoff = cutoff.replace(tzinfo=timezone.utc)
                events = [e for e in events if (e.get("recorded_at") and e["recorded_at"] <= cutoff) or not e.get("recorded_at")]
            return {"application_id": application_id, "as_of": as_of, "events": events}
        finally:
            await pool.close()
            await store.close()

    @mcp.resource("ledger://audit/{entity_id}")
    async def audit_resource(entity_id: str) -> dict[str, Any]:
        """
        Audit trail resource (justified exception: audit can span multiple streams).
        Returns the AuditIntegrityCheckRun events for the entity.
        """
        store, pool, _, _ = await _deps()
        try:
            events = await store.load_stream(f"audit-{entity_id}", from_position=0)
            return {"entity_id": entity_id, "events": events}
        finally:
            await pool.close()
            await store.close()

    @mcp.resource("ledger://agent_sessions/{session_id}")
    async def agent_session_resource(session_id: str) -> dict[str, Any]:
        """
        Agent session resource (justified exception: session stream id includes agent_type).
        Finds the session stream by scanning AgentSessionStarted envelopes and returns the stream events.
        """
        store, pool, _, _ = await _deps()
        try:
            agent_type: str | None = None
            application_id: str | None = None
            async for e in store.load_all(from_global_position=0, event_types=["AgentSessionStarted"], batch_size=500):
                p = e.get("payload") or {}
                if str(p.get("session_id") or "") == session_id:
                    agent_type = str(p.get("agent_type") or "")
                    application_id = str(p.get("application_id") or "") or None
                    break
            if not agent_type:
                return {"session_id": session_id, "events": []}
            stream_id = f"agent-{agent_type}-{session_id}"
            events = await store.load_stream(stream_id, from_position=0)
            trace_row = None
            async with pool.acquire() as conn:
                trace_row = await conn.fetchrow("SELECT * FROM agent_trace WHERE session_id=$1", session_id)
            return {
                "session_id": session_id,
                "application_id": application_id,
                "agent_type": agent_type,
                "stream_id": stream_id,
                "trace": dict(trace_row) if trace_row else None,
                "events": events,
            }
        finally:
            await pool.close()
            await store.close()

    @mcp.resource("ledger://projections")
    async def projections_resource() -> dict[str, Any]:
        store, pool, _, _ = await _deps()
        try:
            async with pool.acquire() as conn:
                cps = await conn.fetch("SELECT projection_name, global_position, updated_at FROM projection_checkpoints")
            return {"checkpoints": [dict(r) for r in cps]}
        finally:
            await pool.close()
            await store.close()

    @mcp.resource("ledger://health")
    async def health_resource() -> dict[str, Any]:
        store, pool, _, _ = await _deps()
        try:
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
                cps = await conn.fetch("SELECT projection_name, global_position FROM projection_checkpoints")
                latest = await conn.fetchrow("SELECT global_position, recorded_at FROM events ORDER BY global_position DESC LIMIT 1")
                lags: dict[str, int] = {}
                if latest:
                    latest_ts = latest["recorded_at"]
                    latest_gp = int(latest["global_position"])
                    for r in cps:
                        cp_gp = int(r["global_position"] or 0)
                        if cp_gp >= latest_gp:
                            lags[r["projection_name"]] = 0
                            continue
                        cp_ts = await conn.fetchval("SELECT recorded_at FROM events WHERE global_position=$1", cp_gp)
                        lags[r["projection_name"]] = max(0, int((latest_ts - cp_ts).total_seconds() * 1000)) if cp_ts else 0
                return {"ok": True, "db": "up", "projection_lag_ms": lags}
        finally:
            await pool.close()
            await store.close()