"""
tests/phase1/test_event_models.py

Validates strict Pydantic v2 event model behavior (missing fields, extra fields).
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from ledger.schema.events import (
    ApplicationSubmitted,
    FinancialFacts,
    LoanPurpose,
    StoredEvent,
)


def test_event_model_missing_required_field_rejected():
    with pytest.raises(Exception):
        ApplicationSubmitted(
            application_id="A",
            applicant_id="C",
            requested_amount_usd=Decimal("1"),
            loan_purpose=LoanPurpose.WORKING_CAPITAL,
            loan_term_months=12,
            submission_channel="web",
            # contact_email missing
            contact_name="X",
            submitted_at=datetime.now(timezone.utc),
            application_reference="A",
        )


def test_event_model_extra_field_rejected():
    with pytest.raises(Exception):
        ApplicationSubmitted(
            application_id="A",
            applicant_id="C",
            requested_amount_usd=Decimal("1"),
            loan_purpose=LoanPurpose.WORKING_CAPITAL,
            loan_term_months=12,
            submission_channel="web",
            contact_email="x@y.com",
            contact_name="X",
            submitted_at=datetime.now(timezone.utc),
            application_reference="A",
            extra_field="nope",  # type: ignore[call-arg]
        )


def test_financial_facts_accepts_seed_fact_keys():
    facts = FinancialFacts(
        total_revenue=Decimal("10"),
        gross_profit=Decimal("1"),
        operating_expenses=Decimal("1"),
        operating_income=Decimal("1"),
        ebitda=Decimal("1"),
        interest_expense=Decimal("1"),
        income_before_tax=Decimal("1"),
        tax_expense=Decimal("1"),
        net_income=Decimal("1"),
        total_assets=Decimal("1"),
        total_liabilities=Decimal("1"),
        total_equity=Decimal("1"),
        cash_and_equivalents=Decimal("1"),
        current_assets=Decimal("1"),
        current_liabilities=Decimal("1"),
        long_term_debt=Decimal("1"),
        accounts_receivable=Decimal("1"),
        inventory=Decimal("1"),
        operating_cash_flow=Decimal("1"),
        investing_cash_flow=Decimal("1"),
        financing_cash_flow=Decimal("1"),
        free_cash_flow=Decimal("1"),
        debt_to_equity=1.0,
        current_ratio=1.0,
        debt_to_ebitda=1.0,
        interest_coverage=1.0,
        gross_margin=0.2,
        net_margin=0.1,
        fiscal_year_end="2024-12-31",
        currency="USD",
        gaap_compliant=True,
        field_confidence={},
        page_references={},
        extraction_notes=[],
        balance_sheet_balances=True,
        balance_discrepancy_usd=Decimal("0"),
    )
    assert facts.total_revenue == Decimal("10")


def test_stored_event_is_separate_type():
    row = {
        "global_position": 1,
        "event_id": "00000000-0000-0000-0000-000000000001",
        "stream_id": "s",
        "stream_position": 0,
        "event_type": "X",
        "event_version": 1,
        "payload": {"a": 1},
        "metadata": {},
        "recorded_at": datetime.now(timezone.utc),
        "prev_hash": None,
        "event_hash": None,
    }
    stored = StoredEvent.from_row(row)
    assert isinstance(stored, StoredEvent)
    assert stored.stream_position == 0

