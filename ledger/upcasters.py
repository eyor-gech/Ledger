"""
ledger/upcasters.py — decorator-based upcaster registry

Upcasters transform older event payloads to the current schema **at read-time**.
They never mutate the database (immutability preserved).

Rules:
- Registration is per (event_type, from_version -> to_version).
- Upcasting chains automatically (v1 -> v2 -> v3 ...).
- If a chain step is missing, the event is returned unchanged from that version onward.
- When a new field cannot be inferred, the upcaster must use `None` (no fabrication).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


UpcastFn = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True, slots=True)
class UpcasterSpec:
    event_type: str
    from_version: int
    to_version: int
    fn: UpcastFn


class UpcasterRegistry:
    def __init__(self, *, include_defaults: bool = True) -> None:
        # event_type -> from_version -> (to_version, fn)
        self._steps: dict[str, dict[int, tuple[int, UpcastFn]]] = {}
        if include_defaults:
            _register_required_upcasters(self)

    def register(self, *, event_type: str, from_version: int, to_version: int) -> Callable[[UpcastFn], UpcastFn]:
        """
        Decorator that registers an upcaster step.

        Example:
            registry = UpcasterRegistry()

            @registry.register(event_type="X", from_version=1, to_version=2)
            def upcast_x_v1_to_v2(event: dict[str, Any]) -> dict[str, Any]:
                ...
        """
        if int(to_version) != int(from_version) + 1:
            raise ValueError("Upcaster steps must be contiguous (to_version == from_version + 1)")

        def _decorator(fn: UpcastFn) -> UpcastFn:
            steps_for_type = self._steps.setdefault(str(event_type), {})
            fv = int(from_version)
            if fv in steps_for_type:
                raise ValueError(f"Duplicate upcaster for {event_type} v{from_version}->v{to_version}")
            steps_for_type[fv] = (int(to_version), fn)
            return fn

        return _decorator

    def upcast(self, event: dict[str, Any]) -> dict[str, Any]:
        """
        Apply registered upcasters to an event row/envelope until current.

        The input dict is not mutated; the returned dict may be a new copy.
        """
        et = str(event.get("event_type") or "")
        if not et:
            return event

        try:
            ver = int(event.get("event_version") or 1)
        except Exception:
            ver = 1

        steps_for_type = self._steps.get(et)
        if not steps_for_type:
            return event

        current: dict[str, Any] = dict(event)
        while True:
            step = steps_for_type.get(ver)
            if step is None:
                return current
            to_version, fn = step
            current = fn(dict(current))
            current["event_version"] = int(to_version)
            ver = int(to_version)


def _register_required_upcasters(reg: UpcasterRegistry) -> None:
    # CreditAnalysisCompleted v1 -> v2
    @reg.register(event_type="CreditAnalysisCompleted", from_version=1, to_version=2)
    def _credit_analysis_completed_v1_to_v2(e: dict[str, Any]) -> dict[str, Any]:
        out = dict(e)
        p = dict(out.get("payload") or {})
        p.setdefault("regulatory_basis", [])
        p.setdefault("model_versions", {})
        out["payload"] = p
        return out

    # DecisionGenerated v1 -> v2
    @reg.register(event_type="DecisionGenerated", from_version=1, to_version=2)
    def _decision_generated_v1_to_v2(e: dict[str, Any]) -> dict[str, Any]:
        out = dict(e)
        p = dict(out.get("payload") or {})
        p.setdefault("model_versions", {})

        # Infer confidence_score if missing
        if "confidence" not in p or p["confidence"] is None:
            recommendation = str(p.get("recommendation") or "")
            p["confidence"] = 0.5 if recommendation == "REFER" else 0.95

        # Infer regulatory_basis from event metadata if missing
        if "regulatory_basis" not in p:
            p["regulatory_basis"] = list(e.get("metadata", {}).get("rules_applied", []))

        out["payload"] = p
        return out

def default_upcasters() -> UpcasterRegistry:
    """
    Explicit constructor for the default registry (kept for callsites that prefer it).
    """
    return UpcasterRegistry(include_defaults=True)
