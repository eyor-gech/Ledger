"""
ledger/upcasters.py — UpcasterRegistry
=======================================
COMPLETION STATUS: STUB — implement upcast() for two event versions.

Upcasters transform old event versions to the current version ON READ.
They NEVER write to the events table. Immutability is non-negotiable.

IMPLEMENT:
  CreditAnalysisCompleted v1 → v2: add model_versions={} if absent
  DecisionGenerated v1 → v2: add model_versions={} if absent

RULE: if event_version == current version, return unchanged.
      if event_version < current version, apply the chain of upcasters.
"""
from __future__ import annotations

class UpcasterRegistry:
    """Apply on load_stream() — never on append()."""
    def upcast(self, event: dict) -> dict:
        et = event.get("event_type"); ver = event.get("event_version", 1)
        if et == "CreditAnalysisCompleted" and int(ver) < 2:
            e = dict(event)
            p = dict(e.get("payload", {}))
            # v2 fields (must be present with defaults)
            p.setdefault("regulatory_basis", [])
            p.setdefault("model_versions", {})
            e["payload"] = p
            e["event_version"] = 2
            return e
        if et == "DecisionGenerated" and int(ver) < 2:
            e = dict(event)
            p = dict(e.get("payload", {}))
            p.setdefault("model_versions", {})
            e["payload"] = p
            e["event_version"] = 2
            return e
        return event
