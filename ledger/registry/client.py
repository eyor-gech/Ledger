"""
ledger/registry/client.py — Applicant Registry read-only client (asyncpg)
===========================================================================
This client reads from the `applicant_registry` schema in PostgreSQL.
It is READ-ONLY. No agent or event store component ever writes here.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol, Sequence
import asyncpg

@dataclass
class CompanyProfile:
    company_id: str; name: str; industry: str; naics: str
    jurisdiction: str; legal_type: str; founded_year: int
    employee_count: int; risk_segment: str; trajectory: str
    submission_channel: str; ip_region: str

@dataclass
class FinancialYear:
    fiscal_year: int; total_revenue: float; gross_profit: float
    operating_income: float; ebitda: float; net_income: float
    total_assets: float; total_liabilities: float; total_equity: float
    long_term_debt: float; cash_and_equivalents: float
    current_assets: float; current_liabilities: float
    accounts_receivable: float; inventory: float
    debt_to_equity: float; current_ratio: float
    debt_to_ebitda: float; interest_coverage_ratio: float
    gross_margin: float; ebitda_margin: float; net_margin: float

@dataclass
class ComplianceFlag:
    flag_type: str; severity: str; is_active: bool; added_date: str; note: str

class RegistryClient(Protocol):
    async def get_company(self, company_id: str) -> CompanyProfile | None: ...
    async def get_financial_history(self, company_id: str, years: list[int] | None = None) -> list[FinancialYear]: ...
    async def get_compliance_flags(self, company_id: str, active_only: bool = False) -> list[ComplianceFlag]: ...
    async def get_loan_relationships(self, company_id: str) -> list[dict]: ...


class ApplicantRegistryClient:
    """
    READ-ONLY access to the Applicant Registry.
    Agents call these methods to get company profiles and historical data.
    Never write to this database from the event store system.
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_company(self, company_id: str) -> CompanyProfile | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT company_id, name, industry, naics, jurisdiction, legal_type, founded_year,
                       employee_count, risk_segment, trajectory, submission_channel, ip_region
                FROM applicant_registry.companies
                WHERE company_id = $1
                """,
                company_id,
            )
        if row is None:
            return None
        d = dict(row)
        # Backfill optional fields for older seeds
        d.setdefault("submission_channel", "UNKNOWN")
        d.setdefault("ip_region", "UNKNOWN")
        return CompanyProfile(**d)  # type: ignore[arg-type]

    async def get_financial_history(self, company_id: str,
                                     years: list[int] | None = None) -> list[FinancialYear]:
        query = """
            SELECT fiscal_year,
                   total_revenue, gross_profit, operating_income, ebitda, net_income,
                   total_assets, total_liabilities, total_equity,
                   long_term_debt, cash_and_equivalents,
                   current_assets, current_liabilities,
                   accounts_receivable, inventory,
                   debt_to_equity, current_ratio, debt_to_ebitda, interest_coverage_ratio,
                   gross_margin, ebitda_margin, net_margin
            FROM applicant_registry.financial_history
            WHERE company_id = $1
        """
        params: list[object] = [company_id]
        if years:
            query += " AND fiscal_year = ANY($2::int[])"
            params.append(list(map(int, years)))
        query += " ORDER BY fiscal_year ASC"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        return [FinancialYear(**dict(r)) for r in rows]  # type: ignore[arg-type]

    async def get_compliance_flags(self, company_id: str,
                                    active_only: bool = False) -> list[ComplianceFlag]:
        query = """
            SELECT flag_type, severity, is_active, added_date, note
            FROM applicant_registry.compliance_flags
            WHERE company_id = $1
        """
        params: list[object] = [company_id]
        if active_only:
            query += " AND is_active = TRUE"
        query += " ORDER BY added_date ASC"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        return [ComplianceFlag(**dict(r)) for r in rows]  # type: ignore[arg-type]

    async def get_loan_relationships(self, company_id: str) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT relationship_id, product_type, start_date, original_amount_usd,
                       current_balance_usd, default_occurred, last_payment_date,
                       relationship_status, notes
                FROM applicant_registry.loan_relationships
                WHERE company_id = $1
                ORDER BY start_date ASC
                """,
                company_id,
            )
        return [dict(r) for r in rows]
