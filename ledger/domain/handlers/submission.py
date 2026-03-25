"""
ledger/domain/handlers/submission.py

Command handler pattern (Phase 5 tools):
  1) load aggregates (or verify stream emptiness for new)
  2) validate via aggregate guards only
  3) produce events only
  4) append with aggregate-tracked version + correlation/causation ids
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from ledger.schema.events import (
    ApplicationSubmitted,
    DocumentUploadRequested,
    DocumentType,
    LoanPurpose,
    PackageCreated,
)


@dataclass(frozen=True, slots=True)
class SubmitApplicationCommand:
    correlation_id: str
    causation_id: str
    application_id: str
    applicant_id: str
    requested_amount_usd: Decimal
    loan_purpose: LoanPurpose
    loan_term_months: int
    submission_channel: str
    contact_email: str
    contact_name: str
    required_document_types: list[DocumentType]


async def handle_submit_application(store: Any, cmd: SubmitApplicationCommand) -> None:
    loan_stream = f"loan-{cmd.application_id}"
    docpkg_stream = f"docpkg-{cmd.application_id}"

    # 1) load (new stream: validate emptiness)
    if await store.stream_version(loan_stream) != -1:
        raise ValueError(f"stream already exists: {loan_stream}")
    if await store.stream_version(docpkg_stream) != -1:
        raise ValueError(f"stream already exists: {docpkg_stream}")

    # 2) validate (guard-only): new stream submission has no aggregate state yet
    if cmd.requested_amount_usd <= 0:
        raise ValueError("requested_amount_usd must be > 0")

    # 3) events only
    submitted = ApplicationSubmitted(
        application_id=cmd.application_id,
        applicant_id=cmd.applicant_id,
        requested_amount_usd=cmd.requested_amount_usd,
        loan_purpose=cmd.loan_purpose,
        loan_term_months=int(cmd.loan_term_months),
        submission_channel=str(cmd.submission_channel),
        contact_email=str(cmd.contact_email),
        contact_name=str(cmd.contact_name),
        submitted_at=datetime.now(timezone.utc),
        application_reference=cmd.application_id,
    ).to_store_dict()
    docs = DocumentUploadRequested(
        application_id=cmd.application_id,
        required_document_types=list(cmd.required_document_types),
        deadline=datetime.now(timezone.utc).replace(microsecond=0),
        requested_by="system",
    ).to_store_dict()
    pkg = PackageCreated(
        package_id=cmd.application_id,
        application_id=cmd.application_id,
        required_documents=list(cmd.required_document_types),
        created_at=datetime.now(timezone.utc),
    ).to_store_dict()

    # 4) append
    await store.append(
        stream_id=loan_stream,
        events=[submitted, docs],
        expected_version=-1,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
    await store.append(
        stream_id=docpkg_stream,
        events=[pkg],
        expected_version=-1,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )

