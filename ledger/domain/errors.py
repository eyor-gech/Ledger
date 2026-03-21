"""
ledger/domain/errors.py

Domain-layer exception types (Phase 1).
"""

from __future__ import annotations


class DomainError(Exception):
    """Base type for all domain-layer errors."""


class IllegalStateTransition(DomainError):
    def __init__(self, aggregate: str, current: str | None, target: str):
        self.aggregate = aggregate
        self.current = current
        self.target = target
        super().__init__(f"{aggregate}: illegal transition {current!r} -> {target!r}")


class InvariantViolation(DomainError):
    pass


class ModelVersionMismatch(DomainError):
    def __init__(self, expected: str, actual: str):
        self.expected = expected
        self.actual = actual
        super().__init__(f"Model version mismatch: expected={expected!r} actual={actual!r}")

