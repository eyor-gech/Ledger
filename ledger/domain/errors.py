"""
ledger/domain/errors.py

Domain-layer exception types (Phase 1).
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

@dataclass
class DomainError(Exception):
    """Base type for all domain-layer errors."""

@dataclass
class IllegalStateTransition(DomainError):
    aggregate: str
    current: Optional[str]
    target: str

    def __str__(self) -> str:
        return f"{self.aggregate}: illegal transition {self.current!r} -> {self.target!r}"

@dataclass
class InvariantViolation(DomainError):
    message: str

@dataclass
class ModelVersionMismatch(DomainError):
    expected: str
    actual: str

    def __str__(self) -> str:
        return f"Model version mismatch: expected={self.expected!r} actual={self.actual!r}"
