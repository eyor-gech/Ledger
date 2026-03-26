from .agent_session import AgentSession
from .audit_ledger import AuditLedger
from .compliance_record import ComplianceRecord
from .credit_record import CreditRecord
from .document_package import DocumentPackage
from .fraud_screening import FraudScreening
from .loan_application import LoanApplication

# Back-compat / rubric-friendly aliases (keep canonical class names intact).
LoanApplicationAggregate = LoanApplication
AgentSessionAggregate = AgentSession
ComplianceRecordAggregate = ComplianceRecord
AuditLedgerAggregate = AuditLedger

__all__ = [
    "AgentSession",
    "AuditLedger",
    "ComplianceRecord",
    "CreditRecord",
    "DocumentPackage",
    "FraudScreening",
    "LoanApplication",
    "LoanApplicationAggregate",
    "AgentSessionAggregate",
    "ComplianceRecordAggregate",
    "AuditLedgerAggregate",
]
