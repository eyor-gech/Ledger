"""
ledger/domain/aggregates/document_package.py

DocumentPackage aggregate (Phase 1):
- Tracks document intake and extraction lifecycle from docpkg stream.
- Provides deterministic "ready for analysis" reconstruction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from ledger.domain.errors import InvariantViolation
from ledger.schema.events import BaseEvent, DocumentType, StoredEvent, deserialize_event


@dataclass(slots=True)
class DocumentPackage:
    package_id: str
    application_id: str | None = None
    created_at: datetime | None = None
    required_documents: set[DocumentType] = field(default_factory=set)
    documents_added: dict[str, DocumentType] = field(default_factory=dict)  # document_id -> type
    validated: set[str] = field(default_factory=set)  # document_id
    rejected: dict[str, str] = field(default_factory=dict)  # document_id -> reason
    extracted: set[str] = field(default_factory=set)  # document_id
    quality_flags: dict[str, list[str]] = field(default_factory=dict)  # document_id -> anomalies
    ready_for_analysis: bool = False
    version: int = -1

    def apply(self, event: BaseEvent) -> None:
        handler = getattr(self, f"on_{event.event_type}", None)
        if handler is None:
            return
        handler(event)
        self.version += 1

    def apply_stored(self, stored: StoredEvent) -> None:
        event = deserialize_event(stored.event_type, stored.payload)
        handler = getattr(self, f"on_{event.event_type}", None)
        if handler is None:
            self.version = stored.stream_position
            return
        handler(event)
        self.version = stored.stream_position

    def on_PackageCreated(self, event: BaseEvent) -> None:
        p = event.to_payload()
        self.application_id = str(p["application_id"])
        created_at = p["created_at"]
        self.created_at = created_at if isinstance(created_at, datetime) else datetime.fromisoformat(str(created_at))
        self.required_documents = {DocumentType(str(d)) for d in (p.get("required_documents") or [])}

    def on_DocumentAdded(self, event: BaseEvent) -> None:
        p = event.to_payload()
        self.documents_added[str(p["document_id"])] = DocumentType(str(p["document_type"]))

    def on_DocumentFormatValidated(self, event: BaseEvent) -> None:
        p = event.to_payload()
        self.validated.add(str(p["document_id"]))

    def on_DocumentFormatRejected(self, event: BaseEvent) -> None:
        p = event.to_payload()
        doc_id = str(p["document_id"])
        self.rejected[doc_id] = str(p["rejection_reason"])

    def on_ExtractionCompleted(self, event: BaseEvent) -> None:
        p = event.to_payload()
        self.extracted.add(str(p["document_id"]))

    def on_QualityAssessmentCompleted(self, event: BaseEvent) -> None:
        p = event.to_payload()
        doc_id = str(p["document_id"])
        anomalies = list(p.get("anomalies") or [])
        critical_missing = list(p.get("critical_missing_fields") or [])
        self.quality_flags[doc_id] = [*anomalies, *[f"missing:{x}" for x in critical_missing]]

    def on_PackageReadyForAnalysis(self, event: BaseEvent) -> None:
        p = event.to_payload()
        self.ready_for_analysis = True
        docs_processed = int(p.get("documents_processed") or 0)
        if docs_processed < 0:
            raise InvariantViolation("documents_processed must be >= 0")

    @classmethod
    def rebuild(cls, events: Iterable[BaseEvent] | Iterable[StoredEvent]) -> "DocumentPackage":
        events_list = list(events)
        if not events_list:
            raise ValueError("cannot rebuild DocumentPackage from empty event list")
        if isinstance(events_list[0], StoredEvent):
            first = deserialize_event(events_list[0].event_type, events_list[0].payload).to_payload()
            pkg_id = str(first.get("package_id") or "")
            if not pkg_id:
                raise ValueError("first docpkg event must include package_id")
            agg = cls(package_id=pkg_id)
            for se in events_list:
                agg.apply_stored(se)
            return agg
        first = events_list[0].to_payload()  # type: ignore[union-attr]
        pkg_id = str(first.get("package_id") or "")
        if not pkg_id:
            raise ValueError("first docpkg event must include package_id")
        agg = cls(package_id=pkg_id)
        for e in events_list:  # type: ignore[assignment]
            agg.apply(e)  # type: ignore[arg-type]
        return agg
