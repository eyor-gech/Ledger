"""
scripts/run_demo.py — Week Standard Demo (Phase 8)

Runs an application through:
  document_processing → credit_analysis → fraud_detection → compliance → decision_orchestrator → human_override

Default mode uses InMemoryEventStore (no Postgres required) and writes:
  - artifacts/api_cost_report.txt
  - artifacts/regulatory_package_NARR05.json
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from ledger.agents import (
    ComplianceAgent,
    CreditAnalysisAgent,
    DecisionOrchestratorAgent,
    DocumentProcessingAgent,
    FraudDetectionAgent,
)
#from ledger.agents.testing import FakeAnthropicClient
from anthropic import Anthropic
from dotenv import load_dotenv
from ledger.audit import verify_store_stream
from ledger.domain.handlers import HumanReviewCompletedCommand, SubmitApplicationCommand, handle_human_review_completed, handle_submit_application
from ledger.event_store import InMemoryEventStore
from ledger.schema.events import AgentType, DocumentFormat, DocumentType, DocumentUploaded, LoanPurpose


@dataclass(slots=True)
class DemoCompany:
    company_id: str
    name: str
    industry: str
    naics: str
    jurisdiction: str
    legal_type: str
    founded_year: int
    employee_count: int
    risk_segment: str
    trajectory: str
    submission_channel: str
    ip_region: str


class DemoRegistry:
    def __init__(self, *, company: DemoCompany, financial_history: list[dict], flags: list[dict], loans: list[dict]) -> None:
        self.company = company
        self.financial_history = financial_history
        self.flags = flags
        self.loans = loans

    async def get_company(self, company_id: str) -> Any:
        return self.company if company_id == self.company.company_id else None

    async def get_financial_history(self, company_id: str, years: list[int] | None = None) -> list[Any]:
        if company_id != self.company.company_id:
            return []
        hist = list(self.financial_history)
        if years:
            ys = set(map(int, years))
            hist = [h for h in hist if int(h.get("fiscal_year") or 0) in ys]
        return hist

    async def get_compliance_flags(self, company_id: str, active_only: bool = False) -> list[Any]:
        if company_id != self.company.company_id:
            return []
        if not active_only:
            return list(self.flags)
        return [f for f in self.flags if bool(f.get("is_active"))]

    async def get_loan_relationships(self, company_id: str) -> list[dict]:
        return list(self.loans) if company_id == self.company.company_id else []


def _write_demo_docs(root: Path, company_id: str) -> dict[str, str]:
    d = root / company_id
    d.mkdir(parents=True, exist_ok=True)
    (d / "application_proposal.pdf").write_bytes(b"%PDF-1.4\n%EOF\n")
    (d / "income_statement_2024.pdf").write_bytes(b"%PDF-1.4\n%EOF\n")
    (d / "balance_sheet_2024.pdf").write_bytes(b"%PDF-1.4\n%EOF\n")
    csv = d / "financial_summary.csv"
    # Deliberately high leverage to trigger orchestrator recommendation=DECLINE (overrideable).
    rows = {
        "total_revenue": 3_000_000.0,
        "gross_profit": 900_000.0,
        "operating_income": 50_000.0,
        "ebitda": 250_000.0,
        "net_income": -25_000.0,
        "total_assets": 2_000_000.0,
        "total_liabilities": 1_600_000.0,
        "total_equity": 400_000.0,
        "long_term_debt": 1_200_000.0,
        "cash_and_equivalents": 50_000.0,
        "current_assets": 400_000.0,
        "current_liabilities": 500_000.0,
        "accounts_receivable": 120_000.0,
        "inventory": 80_000.0,
        "debt_to_equity": 4.0,
        "current_ratio": 0.8,
        "debt_to_ebitda": 4.8,
        "gross_margin": 0.30,
        "ebitda_margin": 0.08,
        "net_margin": -0.01,
    }
    csv.write_text(
        "field,value,fiscal_year,currency\n" + "\n".join([f"{k},{v},2024,USD" for k, v in rows.items()]) + "\n",
        encoding="utf-8",
    )
    return {
        "proposal": str(d / "application_proposal.pdf"),
        "income": str(d / "income_statement_2024.pdf"),
        "balance": str(d / "balance_sheet_2024.pdf"),
    }


def _file_meta(path: str) -> tuple[str, int, str]:
    b = Path(path).read_bytes()
    return Path(path).name, len(b), hashlib.sha256(b).hexdigest()


async def _seed_application(store: InMemoryEventStore, *, application_id: str, company_id: str) -> None:
    await handle_submit_application(
        store,
        SubmitApplicationCommand(
            correlation_id=application_id,
            causation_id=f"demo:{application_id}:submit",
            application_id=application_id,
            applicant_id=company_id,
            requested_amount_usd=Decimal("750000"),
            loan_purpose=LoanPurpose.WORKING_CAPITAL,
            loan_term_months=36,
            submission_channel="demo",
            contact_email="demo@applicant.example",
            contact_name="Demo Applicant",
            required_document_types=[DocumentType.APPLICATION_PROPOSAL, DocumentType.INCOME_STATEMENT, DocumentType.BALANCE_SHEET],
        ),
    )


async def _append_uploads(store: InMemoryEventStore, *, application_id: str, docs: dict[str, str]) -> None:
    loan_stream = f"loan-{application_id}"
    v = await store.stream_version(loan_stream)
    up = []
    for doc_id, dt, key, fy in [
        ("doc-prop", DocumentType.APPLICATION_PROPOSAL, "proposal", None),
        ("doc-is", DocumentType.INCOME_STATEMENT, "income", 2024),
        ("doc-bs", DocumentType.BALANCE_SHEET, "balance", 2024),
    ]:
        fname, size, h = _file_meta(docs[key])
        up.append(
            DocumentUploaded(
                application_id=application_id,
                document_id=doc_id,
                document_type=dt,
                document_format=DocumentFormat.PDF,
                filename=fname,
                file_path=docs[key],
                file_size_bytes=size,
                file_hash=h,
                fiscal_year=fy,
                uploaded_at=datetime.now(timezone.utc),
                uploaded_by="demo",
            ).to_store_dict()
        )
    await store.append(loan_stream, up, expected_version=int(v), correlation_id=application_id, causation_id="demo:upload")


async def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--application-id", default="NARR05-DEMO")
    p.add_argument("--artifacts-dir", default="artifacts")
    args = p.parse_args()

    artifacts = Path(args.artifacts_dir)
    artifacts.mkdir(parents=True, exist_ok=True)
    docs_root = artifacts / "demo_documents"
    docs_root.mkdir(parents=True, exist_ok=True)

    application_id = str(args.application_id)
    company = DemoCompany(
        company_id="CO-DEMO-005",
        name="DemoCo",
        industry="tech",
        naics="000000",
        jurisdiction="CA",
        legal_type="LLC",
        founded_year=2018,
        employee_count=10,
        risk_segment="HIGH",
        trajectory="DECLINING",
        submission_channel="demo",
        ip_region="US-W",
    )
    registry = DemoRegistry(
        company=company,
        financial_history=[{"fiscal_year": 2024, "total_revenue": 3_000_000.0, "debt_to_ebitda": 4.8, "current_ratio": 0.8, "debt_to_equity": 4.0}],
        flags=[],
        loans=[],
    )

    store = InMemoryEventStore()
    docs = _write_demo_docs(docs_root, company.company_id)
    await _seed_application(store, application_id=application_id, company_id=company.company_id)
    await _append_uploads(store, application_id=application_id, docs=docs)

    load_dotenv()
    llm = Anthropic(
        api_key=os.getenv("ANTHROPIC_API_KEY"),  # Your sk-or-v1-bb7e...
        base_url="https://openrouter.ai/api/v1"  # OpenRouter endpoint
    )
    doc_agent = DocumentProcessingAgent(agent_id="doc-agent-1", agent_type=AgentType.DOCUMENT_PROCESSING, store=store, registry=registry, llm_client=llm, model_version="demo-model", docs_root=docs_root)
    credit_agent = CreditAnalysisAgent(agent_id="credit-agent-1", agent_type=AgentType.CREDIT_ANALYSIS, store=store, registry=registry, llm_client=llm, model_version="demo-model")
    fraud_agent = FraudDetectionAgent(agent_id="fraud-agent-1", agent_type=AgentType.FRAUD_DETECTION, store=store, registry=registry, llm_client=llm, model_version="demo-model")
    comp_agent = ComplianceAgent(agent_id="comp-agent-1", agent_type=AgentType.COMPLIANCE, store=store, registry=registry, llm_client=llm, model_version="demo-model")
    orch = DecisionOrchestratorAgent(agent_id="orch-1", agent_type=AgentType.DECISION_ORCHESTRATOR, store=store, registry=registry, llm_client=llm, model_version="demo-model")

    t0 = datetime.now(timezone.utc)
    await doc_agent.process_application(application_id)
    await credit_agent.process_application(application_id)
    await fraud_agent.process_application(application_id)
    await comp_agent.process_application(application_id)
    await orch.process_application(application_id)

    # Human override (NARR-05)
    await handle_human_review_completed(
        store,
        HumanReviewCompletedCommand(
            correlation_id=application_id,
            causation_id="demo:human",
            application_id=application_id,
            reviewer_id="LO-Sarah-Chen",
            override=True,
            original_recommendation="DECLINE",
            final_decision="APPROVE",
            override_reason="Strategic relationship; collateral acceptable.",
            approved_amount_usd=Decimal("750000"),
        ),
    )

    # Audit chain verification
    streams = [
        f"loan-{application_id}",
        f"docpkg-{application_id}",
        f"credit-{application_id}",
        f"fraud-{application_id}",
        f"compliance-{application_id}",
    ]
    ok_all = True
    for s in streams:
        ok, errs = await verify_store_stream(store, s)
        if not ok:
            ok_all = False
            print(f"[AUDIT] FAIL {s}: {errs[:1]}")
    print(f"[AUDIT] {'OK' if ok_all else 'FAIL'}")

    # Cost report: sum AgentSessionCompleted totals
    totals = {"sessions": 0, "cost_usd": 0.0, "tokens": 0}
    async for e in store.load_all(from_position=0):
        if e.get("event_type") == "AgentSessionCompleted":
            p = e.get("payload") or {}
            totals["sessions"] += 1
            totals["cost_usd"] += float(p.get("total_cost_usd") or 0.0)
            totals["tokens"] += int(p.get("total_tokens_used") or 0)
    (artifacts / "api_cost_report.txt").write_text(
        f"application_id={application_id}\n"
        f"sessions={totals['sessions']}\n"
        f"tokens={totals['tokens']}\n"
        f"estimated_cost_usd={totals['cost_usd']:.6f}\n"
        f"started_at={t0.isoformat()}\n"
        f"finished_at={datetime.now(timezone.utc).isoformat()}\n",
        encoding="utf-8",
    )

    # Regulatory package (NARR-05)
    loan = await store.load_stream(f"loan-{application_id}", from_position=0)
    compliance = await store.load_stream(f"compliance-{application_id}", from_position=0)
    pkg = {
        "application_id": application_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "decision": next((e.get("payload") for e in loan if e.get("event_type") == "DecisionGenerated"), None),
        "human_review": next((e.get("payload") for e in loan if e.get("event_type") == "HumanReviewCompleted"), None),
        "final_outcome": next((e.get("payload") for e in loan if e.get("event_type") == "ApplicationApproved"), None),
        "compliance": {
            "verdict": next((e.get("payload") for e in compliance if e.get("event_type") == "ComplianceCheckCompleted"), None),
            "rules": [e.get("payload") for e in compliance if e.get("event_type") in {"ComplianceRulePassed", "ComplianceRuleFailed", "ComplianceRuleNoted"}],
        },
    }
    (artifacts / "regulatory_package_NARR05.json").write_text(json.dumps(pkg, indent=2, sort_keys=True, default=str), encoding="utf-8")
    print(f"[OK] Wrote {artifacts / 'api_cost_report.txt'}")
    print(f"[OK] Wrote {artifacts / 'regulatory_package_NARR05.json'}")
    return 0 if ok_all else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
