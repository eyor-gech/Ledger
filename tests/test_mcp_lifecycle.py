from __future__ import annotations

from datetime import date
from pathlib import Path
from uuid import uuid4
import json
import pytest
import asyncpg

from ledger.mcp.server import create_mcp_server  # Only import the server factory


async def _ensure_schema(db_url: str) -> None:
    """Ensure the PostgreSQL schema exists."""
    sql = Path("schema.sql").read_text(encoding="utf-8")
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(sql)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_full_lifecycle_via_mcp_tools_and_compliance_resource(db_url: str):
    """Full end-to-end lifecycle test via MCP tools."""

    try:
        await _ensure_schema(db_url)
    except Exception:
        pytest.skip("Postgres not available for MCP lifecycle test")

    applicant_id = "COMP-001"
    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)

    try:
        async with pool.acquire() as conn:
            # Seed company
            await conn.execute(
                """
                INSERT INTO applicant_registry.companies(
                    company_id, name, industry, naics, jurisdiction, legal_type, founded_year,
                    employee_count, risk_segment, trajectory, submission_channel, ip_region
                )
                VALUES($1, 'TestCo', 'Software', '541511', 'CA', 'LLC', 2018,
                       10, 'MEDIUM', 'STABLE', 'MCP', 'US')
                ON CONFLICT (company_id) DO NOTHING
                """,
                applicant_id,
            )

            # Seed financial history
            await conn.execute(
                """
                INSERT INTO applicant_registry.financial_history(
                    company_id, fiscal_year, total_revenue, gross_profit, operating_income,
                    ebitda, net_income, total_assets, total_liabilities, total_equity,
                    long_term_debt, cash_and_equivalents, current_assets, current_liabilities,
                    accounts_receivable, inventory, debt_to_equity, current_ratio,
                    debt_to_ebitda, interest_coverage_ratio, gross_margin, ebitda_margin,
                    net_margin
                )
                VALUES($1, 2024, 1000000, 400000, 150000, 200000, 100000,
                       800000, 300000, 500000, 100000, 200000,
                       300000, 150000, 80000, 20000,
                       0.2, 2.0, 0.5, 10.0, 0.4, 0.2, 0.1)
                ON CONFLICT (company_id, fiscal_year) DO NOTHING
                """,
                applicant_id,
            )

            # Seed compliance flag
            await conn.execute(
                """
                INSERT INTO applicant_registry.compliance_flags(
                    company_id, flag_type, severity, is_active, added_date, note
                )
                VALUES($1, 'OTHER', 'HIGH', TRUE, $2, 'Test high severity flag')
                ON CONFLICT DO NOTHING
                """,
                applicant_id,
                date.today(),
            )
    finally:
        await pool.close()

    # ----------------------
    # Create MCP server
    # ----------------------
    server = create_mcp_server(db_url=db_url)
    app_id = f"APEX-MCP-{uuid4().hex[:8]}"

    async def call(name: str, **kwargs):
        res = await server.call_tool(name, kwargs)
        return json.loads(res[0].text)

    async def read(uri: str):
        res = await server.read_resource(uri)
        return json.loads(res[0].text)

    # ----------------------
    # Lifecycle Tool Calls
    # ----------------------
    out = await call(
        "submit_application",
        application_id=app_id,
        applicant_id=applicant_id,
        requested_amount_usd=250_000,
        loan_purpose="working_capital",
        correlation_id=app_id,
        causation_id="mcp:test",
    )
    assert out.get("ok") is True, f"Submit failed: {out}"

    out = await call(
        "start_agent_session",
        application_id=app_id,
        agent_type="document_processing",
        correlation_id=app_id,
        causation_id="mcp:doc",
    )
    assert out.get("ok") is True, f"Agent session failed: {out}"

    out = await call(
        "record_credit_analysis",
        application_id=app_id,
        correlation_id=app_id,
        causation_id="mcp:credit",
    )
    assert out.get("ok") is True, f"Credit analysis failed: {out}"

    out = await call(
        "record_fraud_screening",
        application_id=app_id,
        correlation_id=app_id,
        causation_id="mcp:fraud",
    )
    assert out.get("ok") is True, f"Fraud screening failed: {out}"

    out = await call(
        "record_compliance_check",
        application_id=app_id,
        correlation_id=app_id,
        causation_id="mcp:compliance",
    )
    assert out.get("ok") is True, f"Compliance check failed: {out}"

    out = await call(
        "generate_decision",
        application_id=app_id,
        correlation_id=app_id,
        causation_id="mcp:decision",
    )
    assert out.get("ok") is True, f"Decision generation failed: {out}"

    out = await call(
        "record_human_review",
        application_id=app_id,
        reviewer_id="officer-1",
        final_decision="APPROVE",
        notes="override approved",
        correlation_id=app_id,
        causation_id="mcp:review",
    )
    assert out.get("ok") is True, f"Human review failed: {out}"

    # ----------------------
    # Validate compliance events
    # ----------------------
    compliance = await read(f"ledger://applications/{app_id}/compliance")
    events = compliance.get("events") or []
    event_types = {e.get("event_type") for e in events}
    for t in {"ComplianceCheckInitiated", "ComplianceCheckCompleted"}:
        assert t in event_types, f"Missing compliance event: {t}"