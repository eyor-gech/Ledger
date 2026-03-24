from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from langgraph.graph import END, StateGraph

from ledger.agents.base_agent import BaseApexAgent, RecoveryInfo
from ledger.schema.events import (
    ApplicationDeclined,
    ComplianceCheckCompleted,
    ComplianceCheckInitiated,
    ComplianceRuleFailed,
    ComplianceRuleNoted,
    ComplianceRulePassed,
    DecisionRequested,
)


RULESET_VERSION = "2026-Q1-v1"


class ComplianceAgent(BaseApexAgent):
    agent_node_order = {
        "validate_inputs": 1,
        "open_aggregate_record": 2,
        "load_external_data": 3,
        "evaluate_rules": 4,
        "write_output": 5,
    }

    def build_graph(self) -> Any:
        g = StateGraph(dict)
        g.add_node("validate_inputs", self._node_validate_inputs)
        g.add_node("open_aggregate_record", self._node_open_compliance_stream)
        g.add_node("load_external_data", self._node_load_external_data)
        g.add_node("evaluate_rules", self._node_evaluate_rules)
        g.add_node("write_output", self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs", "open_aggregate_record")
        g.add_edge("open_aggregate_record", "load_external_data")
        g.add_edge("load_external_data", "evaluate_rules")
        g.add_edge("evaluate_rules", "write_output")
        g.add_edge("write_output", END)
        return g.compile()

    def _skip(self, state: dict, node_name: str) -> bool:
        recovery: RecoveryInfo | None = state.get("recovery")
        if recovery is None:
            return False
        return int(recovery.last_successful_node_sequence) >= int(self.agent_node_order[node_name])

    async def _node_validate_inputs(self, state: dict) -> dict:
        if self._skip(state, "validate_inputs"):
            return state
        t0 = time.time()
        app_id = state["application_id"]
        fraud_events = await self.store.load_stream(f"fraud-{app_id}", from_position=0)
        if not any(e.get("event_type") == "FraudScreeningCompleted" for e in fraud_events):
            raise ValueError("FraudScreeningCompleted required before compliance")
        await self.record_input_validated(["application_id", "fraud_complete"], int((time.time() - t0) * 1000))
        await self.record_node_execution("validate_inputs", ["fraud_stream"], ["ready"], int((time.time() - t0) * 1000))
        return state

    async def _node_open_compliance_stream(self, state: dict) -> dict:
        if self._skip(state, "open_aggregate_record"):
            return state
        t0 = time.time()
        app_id = state["application_id"]
        comp_stream = f"compliance-{app_id}"
        existing = await self.store.load_stream(comp_stream, from_position=0)
        if not any(e.get("event_type") == "ComplianceCheckInitiated" for e in existing):
            await self.append_events_with_occ_retry(
                comp_stream,
                [
                    ComplianceCheckInitiated(
                        application_id=app_id,
                        session_id=self.session_id,
                        regulation_set_version=RULESET_VERSION.split("-v", 1)[0],
                        rules_to_evaluate=[f"REG-00{i}" for i in range(1, 7)],
                        initiated_at=datetime.now(timezone.utc),
                    ).to_store_dict()
                ],
                correlation_id=app_id,
                causation_id=self.session_id,
                idempotency_key=f"{app_id}:ComplianceCheckInitiated",
            )
        await self.record_node_execution("open_aggregate_record", ["application_id"], ["compliance_stream_ready"], int((time.time() - t0) * 1000))
        return state

    async def _node_load_external_data(self, state: dict) -> dict:
        if self._skip(state, "load_external_data"):
            return state
        t0 = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}", from_position=0)
        submitted = next((e for e in loan_events if e.get("event_type") == "ApplicationSubmitted"), None)
        applicant_id = str((submitted or {}).get("payload", {}).get("applicant_id") or "")
        profile = await self.registry.get_company(applicant_id) if applicant_id else None
        flags = await self.registry.get_compliance_flags(applicant_id, active_only=True) if applicant_id else []
        await self.record_tool_call(
            "load_company_profile_and_flags",
            {"company_id": applicant_id},
            {"flags": len(flags), "jurisdiction": getattr(profile, "jurisdiction", None) if profile else None},
            int((time.time() - t0) * 1000),
        )
        await self.record_node_execution("load_external_data", ["registry"], ["company_profile", "active_flags"], int((time.time() - t0) * 1000))
        return {
            **state,
            "company_profile": self._as_dict(profile) if profile else None,
            "active_flags": [f if isinstance(f, dict) else self._as_dict(f) for f in flags],
        }

    async def _node_evaluate_rules(self, state: dict) -> dict:
        if self._skip(state, "evaluate_rules"):
            return state
        t0 = time.time()
        profile = dict(state.get("company_profile") or {})
        active_flags = list(state.get("active_flags") or [])
        jurisdiction = str(profile.get("jurisdiction") or "NA")
        legal_type = str(profile.get("legal_type") or "UNKNOWN")

        # Rules mirror datagen/event_simulator for narrative compatibility.
        rules: list[tuple[str, str, bool, bool, str]] = []
        # (id, name, passes, hard_block, reason)
        aml_ok = not any(f.get("flag_type") == "AML_WATCH" and f.get("is_active") for f in active_flags if isinstance(f, dict))
        rules.append(("REG-001", "Bank Secrecy Act (BSA)", aml_ok, False, "AML watchlist flag active"))
        ofac_ok = not any(f.get("flag_type") == "SANCTIONS_REVIEW" and f.get("is_active") for f in active_flags if isinstance(f, dict))
        rules.append(("REG-002", "OFAC Sanctions", ofac_ok, True, "OFAC sanctions review required"))
        mt_ok = jurisdiction != "MT"
        rules.append(("REG-003", "Jurisdiction Eligibility", mt_ok, True, "Montana applications are ineligible (REG-003)"))
        legal_ok = not (legal_type == "Sole Proprietor")
        rules.append(("REG-004", "Legal Entity Eligibility", legal_ok, False, "Sole Proprietor requires additional review"))
        founded_year = int(profile.get("founded_year") or 2024)
        op_ok = (2024 - founded_year) >= 2
        rules.append(("REG-005", "Operating History", op_ok, True, "Operating history < 2 years"))
        rules.append(("REG-006", "CRA Consideration", True, False, "CRA note"))

        await self.record_node_execution("evaluate_rules", ["company_profile", "active_flags"], ["rule_results"], int((time.time() - t0) * 1000))
        return {**state, "rule_results": rules}

    async def _node_write_output(self, state: dict) -> dict:
        if self._skip(state, "write_output"):
            return state
        t0 = time.time()
        app_id = state["application_id"]
        comp_stream = f"compliance-{app_id}"
        loan_stream = f"loan-{app_id}"

        existing = await self.store.load_stream(comp_stream, from_position=0)
        if any(e.get("event_type") == "ComplianceCheckCompleted" for e in existing):
            await self.record_output_written([], "Compliance already completed; no-op.")
            await self.record_node_execution("write_output", ["compliance_stream"], ["noop"], int((time.time() - t0) * 1000))
            return {**state, "next_agent_triggered": "decision_orchestrator"}

        rules = list(state.get("rule_results") or [])
        passed = failed = noted = 0
        hard_block = False
        failed_ids: list[str] = []

        events: list[dict[str, Any]] = []
        for rule_id, name, passes, is_hard, reason in rules:
            eh = self._sha(f"{rule_id}:{app_id}")
            if passes:
                if rule_id == "REG-006":
                    events.append(
                        ComplianceRuleNoted(
                            application_id=app_id,
                            session_id=self.session_id,
                            rule_id=rule_id,
                            rule_name=name,
                            note_type="CRA_CONSIDERATION",
                            note_text="Applicant jurisdiction qualifies for CRA consideration.",
                            evaluated_at=datetime.now(timezone.utc),
                        ).to_store_dict()
                    )
                    noted += 1
                else:
                    events.append(
                        ComplianceRulePassed(
                            application_id=app_id,
                            session_id=self.session_id,
                            rule_id=rule_id,
                            rule_name=name,
                            rule_version=RULESET_VERSION,
                            evidence_hash=eh,
                            evaluation_notes=f"{name}: Clear.",
                            evaluated_at=datetime.now(timezone.utc),
                        ).to_store_dict()
                    )
                    passed += 1
            else:
                events.append(
                    ComplianceRuleFailed(
                        application_id=app_id,
                        session_id=self.session_id,
                        rule_id=rule_id,
                        rule_name=name,
                        rule_version=RULESET_VERSION,
                        failure_reason=reason,
                        is_hard_block=bool(is_hard),
                        remediation_available=not bool(is_hard),
                        remediation_description=None if is_hard else "Manual compliance review required.",
                        evidence_hash=eh,
                        evaluated_at=datetime.now(timezone.utc),
                    ).to_store_dict()
                )
                failed += 1
                failed_ids.append(rule_id)
                if is_hard:
                    hard_block = True
                    break

        verdict = "BLOCKED" if hard_block else "CLEAR" if failed == 0 else "CONDITIONAL"
        completed_event = (
            ComplianceCheckCompleted(
                application_id=app_id,
                session_id=self.session_id,
                rules_evaluated=passed + failed + noted,
                rules_passed=passed,
                rules_failed=failed,
                rules_noted=noted,
                has_hard_block=hard_block,
                overall_verdict=verdict,
                completed_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )
        events.append(completed_event)

        await self.append_events_with_occ_retry(
            comp_stream,
            events,
            correlation_id=app_id,
            causation_id=self.session_id,
            idempotency_key=f"{app_id}:ComplianceCheckCompleted",
        )
        # Mirror completion to loan stream for deterministic LoanApplication replay.
        await self.append_events_with_occ_retry(
            loan_stream,
            [completed_event],
            correlation_id=app_id,
            causation_id=self.session_id,
            idempotency_key=f"{app_id}:Loan:ComplianceCheckCompleted",
        )

        if hard_block:
            await self.append_events_with_occ_retry(
                loan_stream,
                [
                    ApplicationDeclined(
                        application_id=app_id,
                        decline_reasons=[f"Compliance hard block: {failed_ids[0] if failed_ids else 'UNKNOWN'}"],
                        declined_by="compliance-agent",
                        adverse_action_notice_required=True,
                        adverse_action_codes=["COMPLIANCE_BLOCK"],
                        declined_at=datetime.now(timezone.utc),
                    ).to_store_dict()
                ],
                correlation_id=app_id,
                causation_id=self.session_id,
                idempotency_key=f"{app_id}:DeclinedCompliance",
            )
            next_agent = None
        else:
            await self.append_events_with_occ_retry(
                loan_stream,
                [
                    DecisionRequested(
                        application_id=app_id,
                        requested_at=datetime.now(timezone.utc),
                        all_analyses_complete=True,
                        triggered_by_event_id=self._sha(self.session_id),
                    ).to_store_dict()
                ],
                correlation_id=app_id,
                causation_id=self.session_id,
                idempotency_key=f"{app_id}:DecisionRequested",
            )
            next_agent = "decision_orchestrator"

        events_written = [{"stream_id": comp_stream, "event_type": "ComplianceCheckCompleted"}]
        events_written.append({"stream_id": loan_stream, "event_type": "ApplicationDeclined" if hard_block else "DecisionRequested"})
        await self.record_output_written(events_written, f"Compliance: {verdict} (hard_block={hard_block}).")
        await self.record_node_execution("write_output", ["rule_results"], ["events_written"], int((time.time() - t0) * 1000))
        return {**state, "next_agent_triggered": next_agent, "events_written": events_written, "hard_block": hard_block}
