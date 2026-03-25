"""
tests/test_narratives.py
========================
The 5 narrative scenario tests (Phase 7). These drive the real agent pipeline
against an InMemoryEventStore + in-memory registry + real filesystem docs.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from ledger.agents import (
    ComplianceAgent,
    CreditAnalysisAgent,
    DecisionOrchestratorAgent,
    DocumentProcessingAgent,
    FraudDetectionAgent,
)
from ledger.agents.testing import FakeAnthropicClient
from ledger.domain.handlers import HumanReviewCompletedCommand, SubmitApplicationCommand, handle_human_review_completed, handle_submit_application
from ledger.event_store import InMemoryEventStore
from ledger.schema.events import AgentType, DocumentFormat, DocumentType, DocumentUploaded, LoanPurpose


@dataclass(slots=True)
class _RegistryCompany:
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


class InMemoryRegistry:
    def __init__(self, *, company: _RegistryCompany, financial_history: list[dict], flags: list[dict], loans: list[dict]) -> None:
        self.company = company
        self.financial_history = list(financial_history)
        self.flags = list(flags)
        self.loans = list(loans)

    async def get_company(self, company_id: str) -> Any:
        if company_id != self.company.company_id:
            return None
        return self.company

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
        if company_id != self.company.company_id:
            return []
        return list(self.loans)


def _write_docs(tmp_path: Path, company_id: str, *, missing_ebitda: bool = False) -> dict[str, str]:
    d = tmp_path / company_id
    d.mkdir(parents=True, exist_ok=True)
    # Create dummy PDFs (agent reads CSV sidecar)
    for name in ["application_proposal.pdf", "income_statement_2024.pdf", "balance_sheet_2024.pdf"]:
        (d / name).write_bytes(b"%PDF-1.4\n%EOF\n")
    # Sidecar CSV used by DocumentProcessingAgent for deterministic extraction
    rows = {
        "fiscal_year": 2024,
        "total_revenue": 10_000_000.0,
        "gross_profit": 4_000_000.0,
        "operating_income": 1_200_000.0,
        "ebitda": 1_500_000.0,
        "net_income": 900_000.0,
        "total_assets": 8_000_000.0,
        "total_liabilities": 4_500_000.0,
        "total_equity": 3_500_000.0,
        "long_term_debt": 2_000_000.0,
        "cash_and_equivalents": 800_000.0,
        "current_assets": 2_500_000.0,
        "current_liabilities": 1_500_000.0,
        "accounts_receivable": 900_000.0,
        "inventory": 300_000.0,
        "debt_to_equity": 0.6,
        "current_ratio": 1.6,
        "debt_to_ebitda": 1.4,
        "interest_coverage_ratio": 3.2,
        "gross_margin": 0.4,
        "ebitda_margin": 0.15,
        "net_margin": 0.09,
    }
    if missing_ebitda:
        rows["ebitda"] = 0.0
    csv_path = d / "financial_summary.csv"
    csv_path.write_text("field,value,fiscal_year,currency\n" + "\n".join([f"{k},{v},2024,USD" for k, v in rows.items() if k != "fiscal_year"]) + "\n", encoding="utf-8")
    return {
        "proposal": str(d / "application_proposal.pdf"),
        "income": str(d / "income_statement_2024.pdf"),
        "balance": str(d / "balance_sheet_2024.pdf"),
    }


async def _seed_application(store: InMemoryEventStore, *, application_id: str, company_id: str, requested: Decimal) -> None:
    await handle_submit_application(
        store,
        SubmitApplicationCommand(
            correlation_id=application_id,
            causation_id=f"test:{application_id}:submit",
            application_id=application_id,
            applicant_id=company_id,
            requested_amount_usd=requested,
            loan_purpose=LoanPurpose.WORKING_CAPITAL,
            loan_term_months=36,
            submission_channel="test",
            contact_email="a@example.com",
            contact_name="Applicant",
            required_document_types=[DocumentType.APPLICATION_PROPOSAL, DocumentType.INCOME_STATEMENT, DocumentType.BALANCE_SHEET],
        ),
    )


async def _append_uploads(store: InMemoryEventStore, *, application_id: str, docs: dict[str, str]) -> None:
    import hashlib

    loan_stream = f"loan-{application_id}"
    v = await store.stream_version(loan_stream)
    def _hash(path: str) -> tuple[str, int, str]:
        b = Path(path).read_bytes()
        return Path(path).name, len(b), hashlib.sha256(b).hexdigest()
    uploads = [
        DocumentUploaded(
            application_id=application_id,
            document_id="doc-prop",
            document_type=DocumentType.APPLICATION_PROPOSAL,
            document_format=DocumentFormat.PDF,
            filename=_hash(docs["proposal"])[0],
            file_path=docs["proposal"],
            file_size_bytes=_hash(docs["proposal"])[1],
            file_hash=_hash(docs["proposal"])[2],
            uploaded_at=datetime.now(timezone.utc),
            uploaded_by="test",
        ).to_store_dict(),
        DocumentUploaded(
            application_id=application_id,
            document_id="doc-is",
            document_type=DocumentType.INCOME_STATEMENT,
            document_format=DocumentFormat.PDF,
            filename=_hash(docs["income"])[0],
            file_path=docs["income"],
            file_size_bytes=_hash(docs["income"])[1],
            file_hash=_hash(docs["income"])[2],
            fiscal_year=2024,
            uploaded_at=datetime.now(timezone.utc),
            uploaded_by="test",
        ).to_store_dict(),
        DocumentUploaded(
            application_id=application_id,
            document_id="doc-bs",
            document_type=DocumentType.BALANCE_SHEET,
            document_format=DocumentFormat.PDF,
            filename=_hash(docs["balance"])[0],
            file_path=docs["balance"],
            file_size_bytes=_hash(docs["balance"])[1],
            file_hash=_hash(docs["balance"])[2],
            fiscal_year=2024,
            uploaded_at=datetime.now(timezone.utc),
            uploaded_by="test",
        ).to_store_dict(),
    ]
    await store.append(loan_stream, uploads, expected_version=int(v), correlation_id=application_id, causation_id="test:upload")


@pytest.mark.asyncio
async def test_narr01_occ_collision_resolution(tmp_path: Path):
    """
    NARR-01: Two CreditAnalysisAgent instances run simultaneously.
    Expected: exactly one CreditAnalysisCompleted in credit stream (not two),
              both tasks return without error.
    """
    store = InMemoryEventStore()
    company_id = "CO-001"
    docs = _write_docs(tmp_path, company_id)
    registry = InMemoryRegistry(
        company=_RegistryCompany(
            company_id=company_id,
            name="Company",
            industry="tech",
            naics="000000",
            jurisdiction="CA",
            legal_type="LLC",
            founded_year=2018,
            employee_count=50,
            risk_segment="LOW",
            trajectory="STABLE",
            submission_channel="web",
            ip_region="US-W",
        ),
        financial_history=[{"fiscal_year": 2024, "total_revenue": 10_000_000.0, "debt_to_ebitda": 1.4, "current_ratio": 1.6, "debt_to_equity": 0.6}],
        flags=[],
        loans=[],
    )
    app_id = "APEX-N1"
    await _seed_application(store, application_id=app_id, company_id=company_id, requested=Decimal("500000"))
    await _append_uploads(store, application_id=app_id, docs=docs)

    doc_agent = DocumentProcessingAgent(agent_id="doc-agent-1", agent_type=AgentType.DOCUMENT_PROCESSING, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model", docs_root=tmp_path)
    await doc_agent.process_application(app_id)

    credit1 = CreditAnalysisAgent(agent_id="credit-1", agent_type=AgentType.CREDIT_ANALYSIS, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    credit2 = CreditAnalysisAgent(agent_id="credit-2", agent_type=AgentType.CREDIT_ANALYSIS, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")

    await asyncio.gather(credit1.process_application(app_id), credit2.process_application(app_id))
    credit_events = await store.load_stream(f"credit-{app_id}", from_position=0)
    assert sum(1 for e in credit_events if e["event_type"] == "CreditAnalysisCompleted") == 1


@pytest.mark.asyncio
async def test_narr02_full_pipeline_success(tmp_path: Path):
    """
    NARR-02: Full pipeline success path (no hard blocks).
    """
    store = InMemoryEventStore()
    company_id = "CO-002"
    docs = _write_docs(tmp_path, company_id)
    registry = InMemoryRegistry(
        company=_RegistryCompany(
            company_id=company_id,
            name="Company2",
            industry="manufacturing",
            naics="000000",
            jurisdiction="CA",
            legal_type="LLC",
            founded_year=2016,
            employee_count=120,
            risk_segment="LOW",
            trajectory="GROWTH",
            submission_channel="web",
            ip_region="US-W",
        ),
        financial_history=[{"fiscal_year": 2024, "total_revenue": 20_000_000.0, "debt_to_ebitda": 1.1, "current_ratio": 1.8, "debt_to_equity": 0.5}],
        flags=[],
        loans=[],
    )
    app_id = "APEX-N2"
    await _seed_application(store, application_id=app_id, company_id=company_id, requested=Decimal("750000"))
    await _append_uploads(store, application_id=app_id, docs=docs)

    doc_agent = DocumentProcessingAgent(agent_id="doc-agent-1", agent_type=AgentType.DOCUMENT_PROCESSING, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model", docs_root=tmp_path)
    credit_agent = CreditAnalysisAgent(agent_id="credit-agent-1", agent_type=AgentType.CREDIT_ANALYSIS, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    fraud_agent = FraudDetectionAgent(agent_id="fraud-agent-1", agent_type=AgentType.FRAUD_DETECTION, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    comp_agent = ComplianceAgent(agent_id="comp-agent-1", agent_type=AgentType.COMPLIANCE, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    orch = DecisionOrchestratorAgent(agent_id="orch-1", agent_type=AgentType.DECISION_ORCHESTRATOR, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")

    await doc_agent.process_application(app_id)
    await credit_agent.process_application(app_id)
    await fraud_agent.process_application(app_id)
    await comp_agent.process_application(app_id)
    await orch.process_application(app_id)

    loan_events = await store.load_stream(f"loan-{app_id}", from_position=0)
    assert any(e["event_type"] == "DecisionGenerated" for e in loan_events)


@pytest.mark.asyncio
async def test_narr03_gas_town_recovery(tmp_path: Path):
    """
    NARR-03: FraudDetectionAgent crashes mid-session and resumes without duplicating work.
    """
    store = InMemoryEventStore()
    company_id = "CO-003"
    docs = _write_docs(tmp_path, company_id)
    registry = InMemoryRegistry(
        company=_RegistryCompany(
            company_id=company_id,
            name="Company3",
            industry="tech",
            naics="000000",
            jurisdiction="CA",
            legal_type="LLC",
            founded_year=2017,
            employee_count=80,
            risk_segment="LOW",
            trajectory="STABLE",
            submission_channel="web",
            ip_region="US-W",
        ),
        financial_history=[{"fiscal_year": 2024, "total_revenue": 15_000_000.0, "debt_to_ebitda": 1.2, "current_ratio": 1.7, "debt_to_equity": 0.7}],
        flags=[],
        loans=[],
    )
    app_id = "APEX-N3"
    await _seed_application(store, application_id=app_id, company_id=company_id, requested=Decimal("400000"))
    await _append_uploads(store, application_id=app_id, docs=docs)

    doc_agent = DocumentProcessingAgent(agent_id="doc-agent-1", agent_type=AgentType.DOCUMENT_PROCESSING, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model", docs_root=tmp_path)
    credit_agent = CreditAnalysisAgent(agent_id="credit-agent-1", agent_type=AgentType.CREDIT_ANALYSIS, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    await doc_agent.process_application(app_id)
    await credit_agent.process_application(app_id)

    fraud_agent = FraudDetectionAgent(agent_id="fraud-agent-1", agent_type=AgentType.FRAUD_DETECTION, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    with pytest.raises(RuntimeError):
        await fraud_agent.process_application(app_id, simulate_crash_after_node="analyze_fraud_patterns")

    fraud_agent2 = FraudDetectionAgent(agent_id="fraud-agent-1", agent_type=AgentType.FRAUD_DETECTION, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    await fraud_agent2.process_application(app_id, resume_if_possible=True)

    fraud_events = await store.load_stream(f"fraud-{app_id}", from_position=0)
    assert sum(1 for e in fraud_events if e["event_type"] == "FraudScreeningCompleted") == 1

    # Validate recovery context source
    all_agent_events = [e async for e in store.load_all(from_position=0)]
    started = [e for e in all_agent_events if e["event_type"] == "AgentSessionStarted" and (e["payload"] or {}).get("agent_type") == "fraud_detection"]
    assert len(started) >= 2
    assert str(started[-1]["payload"]["context_source"]).startswith("prior_session_replay:")


@pytest.mark.asyncio
async def test_narr04_montana_hard_decline(tmp_path: Path):
    """
    NARR-04: Montana applicant triggers REG-003 HARD DECLINE.
    """
    store = InMemoryEventStore()
    company_id = "CO-MT"
    docs = _write_docs(tmp_path, company_id)
    registry = InMemoryRegistry(
        company=_RegistryCompany(
            company_id=company_id,
            name="MontanaCo",
            industry="agri",
            naics="000000",
            jurisdiction="MT",
            legal_type="LLC",
            founded_year=2010,
            employee_count=30,
            risk_segment="MEDIUM",
            trajectory="STABLE",
            submission_channel="web",
            ip_region="US-MT",
        ),
        financial_history=[{"fiscal_year": 2024, "total_revenue": 5_000_000.0, "debt_to_ebitda": 2.2, "current_ratio": 1.3, "debt_to_equity": 1.2}],
        flags=[],
        loans=[],
    )
    app_id = "APEX-N4"
    await _seed_application(store, application_id=app_id, company_id=company_id, requested=Decimal("300000"))
    await _append_uploads(store, application_id=app_id, docs=docs)

    doc_agent = DocumentProcessingAgent(agent_id="doc-agent-1", agent_type=AgentType.DOCUMENT_PROCESSING, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model", docs_root=tmp_path)
    credit_agent = CreditAnalysisAgent(agent_id="credit-agent-1", agent_type=AgentType.CREDIT_ANALYSIS, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    fraud_agent = FraudDetectionAgent(agent_id="fraud-agent-1", agent_type=AgentType.FRAUD_DETECTION, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    comp_agent = ComplianceAgent(agent_id="comp-agent-1", agent_type=AgentType.COMPLIANCE, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")

    await doc_agent.process_application(app_id)
    await credit_agent.process_application(app_id)
    await fraud_agent.process_application(app_id)
    await comp_agent.process_application(app_id)

    comp_events = await store.load_stream(f"compliance-{app_id}", from_position=0)
    assert any(e["event_type"] == "ComplianceRuleFailed" and (e["payload"] or {}).get("rule_id") == "REG-003" and bool((e["payload"] or {}).get("is_hard_block")) for e in comp_events)

    loan_events = await store.load_stream(f"loan-{app_id}", from_position=0)
    assert not any(e["event_type"] == "DecisionGenerated" for e in loan_events)
    declined = next(e for e in loan_events if e["event_type"] == "ApplicationDeclined")
    assert bool((declined["payload"] or {}).get("adverse_action_notice_required")) is True


@pytest.mark.asyncio
async def test_narr05_human_override(tmp_path: Path):
    """
    NARR-05: Orchestrator recommends DECLINE; human overrides to APPROVE.
    """
    store = InMemoryEventStore()
    company_id = "CO-005"
    docs = _write_docs(tmp_path, company_id)
    registry = InMemoryRegistry(
        company=_RegistryCompany(
            company_id=company_id,
            name="Company5",
            industry="tech",
            naics="000000",
            jurisdiction="CA",
            legal_type="LLC",
            founded_year=2018,
            employee_count=10,
            risk_segment="HIGH",
            trajectory="DECLINING",
            submission_channel="web",
            ip_region="US-W",
        ),
        financial_history=[{"fiscal_year": 2024, "total_revenue": 3_000_000.0, "debt_to_ebitda": 4.8, "current_ratio": 1.0, "debt_to_equity": 2.4}],
        flags=[],
        loans=[],
    )
    app_id = "APEX-N5"
    await _seed_application(store, application_id=app_id, company_id=company_id, requested=Decimal("750000"))
    await _append_uploads(store, application_id=app_id, docs=docs)

    doc_agent = DocumentProcessingAgent(agent_id="doc-agent-1", agent_type=AgentType.DOCUMENT_PROCESSING, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model", docs_root=tmp_path)
    credit_agent = CreditAnalysisAgent(agent_id="credit-agent-1", agent_type=AgentType.CREDIT_ANALYSIS, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    fraud_agent = FraudDetectionAgent(agent_id="fraud-agent-1", agent_type=AgentType.FRAUD_DETECTION, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    comp_agent = ComplianceAgent(agent_id="comp-agent-1", agent_type=AgentType.COMPLIANCE, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")
    orch = DecisionOrchestratorAgent(agent_id="orch-1", agent_type=AgentType.DECISION_ORCHESTRATOR, store=store, registry=registry, llm_client=FakeAnthropicClient(), model_version="test-model")

    await doc_agent.process_application(app_id)
    await credit_agent.process_application(app_id)
    await fraud_agent.process_application(app_id)
    await comp_agent.process_application(app_id)
    await orch.process_application(app_id)

    loan_events = await store.load_stream(f"loan-{app_id}", from_position=0)
    decision = next(e for e in loan_events if e["event_type"] == "DecisionGenerated")
    assert (decision["payload"] or {}).get("recommendation") == "DECLINE"
    assert any(e["event_type"] == "HumanReviewRequested" for e in loan_events)

    await handle_human_review_completed(
        store,
        HumanReviewCompletedCommand(
            correlation_id=app_id,
            causation_id="test:human",
            application_id=app_id,
            reviewer_id="LO-Sarah-Chen",
            override=True,
            original_recommendation="DECLINE",
            final_decision="APPROVE",
            override_reason="Strong collateral and strategic relationship",
            approved_amount_usd=Decimal("750000"),
        ),
    )

    loan_events2 = await store.load_stream(f"loan-{app_id}", from_position=0)
    assert any(e["event_type"] == "HumanReviewCompleted" and bool((e["payload"] or {}).get("override")) for e in loan_events2)
    approved = next(e for e in loan_events2 if e["event_type"] == "ApplicationApproved")
    assert Decimal(str((approved["payload"] or {}).get("approved_amount_usd"))) == Decimal("750000")
