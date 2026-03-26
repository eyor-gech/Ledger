from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path

import asyncpg
import pytest

from ledger.domain.handlers.submission import SubmitApplicationCommand, handle_submit_application
from ledger.event_store import EventStore, InMemoryEventStore
from ledger.projections.in_memory import InMemoryProjections, apply_application_summary, run_in_memory_projection
from ledger.projections.views import ComplianceAuditView
from ledger.upcasters import UpcasterRegistry


async def _ensure_schema(db_url: str) -> None:
    sql = Path("schema.sql").read_text(encoding="utf-8")
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(sql)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_projection_lag_slo_under_50_concurrent_submissions_inmemory():
    store = InMemoryEventStore()

    async def submit(i: int) -> None:
        app_id = f"APEX-SLO-{i:03d}"
        cmd = SubmitApplicationCommand(
            correlation_id=app_id,
            causation_id=f"test:{app_id}",
            application_id=app_id,
            applicant_id="COMP-001",
            requested_amount_usd=Decimal("100000"),
            loan_purpose="working_capital",  # pydantic enum coercion happens downstream
            loan_term_months=12,
            submission_channel="TEST",
            contact_email="test@example.com",
            contact_name="Test",
            required_document_types=["income_statement"],
        )
        await handle_submit_application(store, cmd)

    await asyncio.gather(*[submit(i) for i in range(50)])

    mem = InMemoryProjections()
    await run_in_memory_projection(
        store=store,
        projection_name="application_summary",
        apply_fn=apply_application_summary,
        mem=mem,
    )

    latest_gp = len(store._global) - 1
    cp = mem.checkpoint("application_summary")
    assert cp == latest_gp

    # Lag in ms: latest recorded_at - checkpoint recorded_at (should be ~0 when caught up).
    latest_ts = store._global[latest_gp]["recorded_at"]
    cp_ts = store._global[cp]["recorded_at"]
    lag_ms = int((latest_ts - cp_ts).total_seconds() * 1000)
    assert lag_ms < 500


@pytest.mark.asyncio
async def test_compliance_rebuild_from_scratch_does_not_block_reads(db_url):
    try:
        await _ensure_schema(db_url)
    except Exception:
        pytest.skip("Postgres not available for rebuild_from_scratch test")

    store = EventStore(db_url, upcaster_registry=UpcasterRegistry())
    await store.connect()
    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)

    app_id = "APEX-REBUILD-0001"
    compliance_stream = f"compliance-{app_id}"
    try:
        await store.append(
            compliance_stream,
            [
                {
                    "event_type": "ComplianceRuleFailed",
                    "event_version": 1,
                    "payload": {
                        "application_id": app_id,
                        "session_id": "S1",
                        "rule_id": "REG-003",
                        "rule_name": "Jurisdiction Eligibility",
                        "rule_version": "2026-Q1-v1",
                        "failure_reason": "MT",
                        "is_hard_block": True,
                        "remediation_available": False,
                        "evidence_hash": "x",
                        "evaluated_at": "2026-01-01T00:00:00+00:00",
                    },
                }
            ],
            expected_version=-1,
        )

        view = ComplianceAuditView(store=store, pool=pool)

        rebuild_task = asyncio.create_task(view.rebuild_from_scratch())

        async def read_loop() -> None:
            # Reads should not hang even during rebuild.
            async with pool.acquire() as conn:
                await conn.fetchrow("SELECT * FROM compliance_audit LIMIT 1")

        await asyncio.wait_for(read_loop(), timeout=1.0)
        await rebuild_task
    finally:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM events WHERE stream_id=$1", compliance_stream)
            await conn.execute("DELETE FROM event_streams WHERE stream_id=$1", compliance_stream)
        await pool.close()
        await store.close()
