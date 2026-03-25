"""
ledger/registry/file_client.py — Offline Applicant Registry client
===================================================================
Used by narrative tests and demo runs where a registry DB is not available.

Input format: `datagen/generate_all.py` writes JSONL exports under `data/registry/`.
This client loads those exports into memory and serves async methods.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ledger.registry.client import CompanyProfile, ComplianceFlag, FinancialYear, RegistryClient


def _load_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


@dataclass(slots=True)
class FileApplicantRegistryClient(RegistryClient):
    root_dir: Path

    def __post_init__(self) -> None:
        self.root_dir = Path(self.root_dir)
        companies = _load_jsonl(self.root_dir / "companies.jsonl")
        financials = _load_jsonl(self.root_dir / "financial_history.jsonl")
        flags = _load_jsonl(self.root_dir / "compliance_flags.jsonl")
        loans = _load_jsonl(self.root_dir / "loan_relationships.jsonl")

        self._companies: dict[str, CompanyProfile] = {}
        for c in companies:
            self._companies[str(c["company_id"])] = CompanyProfile(
                company_id=str(c["company_id"]),
                name=str(c["name"]),
                industry=str(c["industry"]),
                naics=str(c.get("naics") or "000000"),
                jurisdiction=str(c.get("jurisdiction") or "NA"),
                legal_type=str(c.get("legal_type") or "UNKNOWN"),
                founded_year=int(c.get("founded_year") or 2000),
                employee_count=int(c.get("employee_count") or 0),
                risk_segment=str(c.get("risk_segment") or "MEDIUM"),
                trajectory=str(c.get("trajectory") or "STABLE"),
                submission_channel=str(c.get("submission_channel") or "UNKNOWN"),
                ip_region=str(c.get("ip_region") or "UNKNOWN"),
            )

        self._financial_history: dict[str, list[FinancialYear]] = {}
        for r in financials:
            cid = str(r["company_id"])
            fy = FinancialYear(
                fiscal_year=int(r["fiscal_year"]),
                total_revenue=float(r.get("total_revenue") or 0.0),
                gross_profit=float(r.get("gross_profit") or 0.0),
                operating_income=float(r.get("operating_income") or 0.0),
                ebitda=float(r.get("ebitda") or 0.0),
                net_income=float(r.get("net_income") or 0.0),
                total_assets=float(r.get("total_assets") or 0.0),
                total_liabilities=float(r.get("total_liabilities") or 0.0),
                total_equity=float(r.get("total_equity") or 0.0),
                long_term_debt=float(r.get("long_term_debt") or 0.0),
                cash_and_equivalents=float(r.get("cash_and_equivalents") or 0.0),
                current_assets=float(r.get("current_assets") or 0.0),
                current_liabilities=float(r.get("current_liabilities") or 0.0),
                accounts_receivable=float(r.get("accounts_receivable") or 0.0),
                inventory=float(r.get("inventory") or 0.0),
                debt_to_equity=float(r.get("debt_to_equity") or 0.0),
                current_ratio=float(r.get("current_ratio") or 0.0),
                debt_to_ebitda=float(r.get("debt_to_ebitda") or 0.0),
                interest_coverage_ratio=float(r.get("interest_coverage_ratio") or 0.0),
                gross_margin=float(r.get("gross_margin") or 0.0),
                ebitda_margin=float(r.get("ebitda_margin") or 0.0),
                net_margin=float(r.get("net_margin") or 0.0),
            )
            self._financial_history.setdefault(cid, []).append(fy)
        for cid, ys in self._financial_history.items():
            ys.sort(key=lambda y: int(y.fiscal_year))

        self._flags: dict[str, list[ComplianceFlag]] = {}
        for r in flags:
            cid = str(r["company_id"])
            self._flags.setdefault(cid, []).append(
                ComplianceFlag(
                    flag_type=str(r.get("flag_type") or "UNKNOWN"),
                    severity=str(r.get("severity") or "LOW"),
                    is_active=bool(r.get("is_active") if "is_active" in r else True),
                    added_date=str(r.get("added_date") or "1970-01-01"),
                    note=str(r.get("note") or ""),
                )
            )

        self._loans: dict[str, list[dict]] = {}
        for r in loans:
            cid = str(r["company_id"])
            d = dict(r)
            d.pop("company_id", None)
            self._loans.setdefault(cid, []).append(d)

    async def get_company(self, company_id: str) -> CompanyProfile | None:
        return self._companies.get(company_id)

    async def get_financial_history(self, company_id: str, years: list[int] | None = None) -> list[FinancialYear]:
        hist = list(self._financial_history.get(company_id, []))
        if years:
            years_set = {int(y) for y in years}
            hist = [h for h in hist if int(h.fiscal_year) in years_set]
        return hist

    async def get_compliance_flags(self, company_id: str, active_only: bool = False) -> list[ComplianceFlag]:
        flags = list(self._flags.get(company_id, []))
        if active_only:
            flags = [f for f in flags if bool(f.is_active)]
        return flags

    async def get_loan_relationships(self, company_id: str) -> list[dict]:
        return list(self._loans.get(company_id, []))

