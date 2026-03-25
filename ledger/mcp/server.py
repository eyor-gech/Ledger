from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg
from fastmcp import FastMCP

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
from ledger.event_store import EventStore
from ledger.schema.events import DocumentType, LoanPurpose


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

    async def _deps() -> tuple[EventStore, asyncpg.Pool]:
        store = EventStore(url)
        await store.connect()
        pool = await asyncpg.create_pool(url, min_size=1, max_size=5)
        return store, pool

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
        store, pool = await _deps()
        try:
            cmd = SubmitApplicationCommand(
                correlation_id=correlation_id or application_id,
                causation_id=causation_id or f"mcp:{application_id}",
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
        finally:
            await pool.close()
            await store.close()

    @mcp.tool(name="record_credit_analysis")
    async def record_credit_analysis(
        application_id: str,
        requested_by: str = "mcp",
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        store, pool = await _deps()
        try:
            positions = await handle_request_credit_analysis(
                store,
                RequestCreditAnalysisCommand(
                    correlation_id=correlation_id or application_id,
                    causation_id=causation_id or f"mcp:{application_id}",
                    application_id=application_id,
                    requested_by=requested_by,
                ),
            )
            return {"ok": True, "positions": positions}
        finally:
            await pool.close()
            await store.close()

    @mcp.tool(name="trigger_compliance_check")
    async def trigger_compliance_check(
        application_id: str,
        triggered_by_event_id: str,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        store, pool = await _deps()
        try:
            positions = await handle_request_compliance_check(
                store,
                RequestComplianceCheckCommand(
                    correlation_id=correlation_id or application_id,
                    causation_id=causation_id or f"mcp:{application_id}",
                    application_id=application_id,
                    triggered_by_event_id=triggered_by_event_id,
                ),
            )
            return {"ok": True, "positions": positions}
        finally:
            await pool.close()
            await store.close()

    @mcp.resource("ledger://applications/{application_id}")
    async def application_resource(application_id: str) -> dict[str, Any]:
        store, pool = await _deps()
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

    @mcp.resource("ledger://audit/{application_id}/temporal")
    async def audit_temporal_resource(application_id: str, as_of: str | None = None) -> dict[str, Any]:
        """
        Temporal query (best-effort): returns loan stream events up to `as_of` (ISO timestamp).
        When `as_of` is omitted, returns full stream.
        """
        store, pool = await _deps()
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

    return mcp

