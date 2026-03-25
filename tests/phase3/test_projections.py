import pytest

from ledger.event_store import InMemoryEventStore
from ledger.projections.in_memory import (
    InMemoryProjections,
    apply_agent_trace,
    apply_application_summary,
    apply_compliance_audit,
    run_in_memory_projection,
)


@pytest.mark.asyncio
async def test_projections_checkpoint_and_idempotency():
    store = InMemoryEventStore()
    app_id = "APEX-TEST-0001"

    await store.append(
        f"loan-{app_id}",
        [
            {
                "event_type": "ApplicationSubmitted",
                "event_version": 1,
                "payload": {"application_id": app_id, "applicant_id": "CO-1", "requested_amount_usd": 100000, "loan_purpose": "working_capital"},
            }
        ],
        expected_version=-1,
    )
    await store.append(
        f"loan-{app_id}",
        [
            {"event_type": "DocumentUploadRequested", "event_version": 1, "payload": {"application_id": app_id, "required_document_types": ["income_statement"], "deadline": "2026-01-01T00:00:00+00:00", "requested_by": "system"}}
        ],
        expected_version=0,
    )
    await store.append(
        f"compliance-{app_id}",
        [
            {"event_type": "ComplianceRuleFailed", "event_version": 1, "payload": {"application_id": app_id, "session_id": "S1", "rule_id": "REG-003", "rule_name": "Jurisdiction Eligibility", "rule_version": "2026-Q1-v1", "failure_reason": "MT", "is_hard_block": True, "remediation_available": False, "evidence_hash": "x", "evaluated_at": "2026-01-01T00:00:00+00:00"}}
        ],
        expected_version=-1,
    )

    mem = InMemoryProjections()
    await run_in_memory_projection(store=store, projection_name="application_summary", apply_fn=apply_application_summary, mem=mem)
    await run_in_memory_projection(store=store, projection_name="compliance_audit", apply_fn=apply_compliance_audit, mem=mem)

    assert mem.application_summary[app_id]["state"] == "DOCUMENTS_PENDING"
    assert "REG-003" in mem.compliance_audit[app_id]["failed_rule_ids"]
    cp1 = mem.checkpoint("application_summary")

    # Running again should be idempotent and should not move checkpoint.
    await run_in_memory_projection(store=store, projection_name="application_summary", apply_fn=apply_application_summary, mem=mem)
    assert mem.checkpoint("application_summary") == cp1


@pytest.mark.asyncio
async def test_agent_trace_projection_collects_nodes_and_tools():
    store = InMemoryEventStore()
    app_id = "APEX-TRACE-0001"
    session_id = "sess-doc-123"
    agent_stream = f"agent-document_processing-{session_id}"

    await store.append(
        agent_stream,
        [{"event_type": "AgentSessionStarted", "event_version": 1, "payload": {"session_id": session_id, "agent_type": "document_processing", "agent_id": "a1", "application_id": app_id, "model_version": "m", "langgraph_graph_version": "1", "context_source": "fresh", "context_token_count": 0, "started_at": "2026-01-01T00:00:00+00:00"}}],
        expected_version=-1,
    )
    await store.append(
        agent_stream,
        [{"event_type": "AgentToolCalled", "event_version": 1, "payload": {"session_id": session_id, "agent_type": "document_processing", "tool_name": "load", "tool_input_summary": "i", "tool_output_summary": "o", "tool_duration_ms": 10, "called_at": "2026-01-01T00:00:01+00:00"}}],
        expected_version=0,
    )
    await store.append(
        agent_stream,
        [{"event_type": "AgentNodeExecuted", "event_version": 1, "payload": {"session_id": session_id, "agent_type": "document_processing", "node_name": "validate_inputs", "node_sequence": 1, "input_keys": ["a"], "output_keys": ["b"], "llm_called": False, "duration_ms": 5, "executed_at": "2026-01-01T00:00:02+00:00"}}],
        expected_version=1,
    )
    await store.append(
        agent_stream,
        [{"event_type": "AgentSessionCompleted", "event_version": 1, "payload": {"session_id": session_id, "agent_type": "document_processing", "application_id": app_id, "total_nodes_executed": 1, "total_llm_calls": 0, "total_tokens_used": 0, "total_cost_usd": 0.0, "total_duration_ms": 20, "next_agent_triggered": None, "completed_at": "2026-01-01T00:00:03+00:00"}}],
        expected_version=2,
    )

    mem = InMemoryProjections()
    await run_in_memory_projection(store=store, projection_name="agent_trace", apply_fn=apply_agent_trace, mem=mem)

    assert session_id in mem.agent_trace
    row = mem.agent_trace[session_id]
    assert len(row["nodes_executed"]) == 1
    assert len(row["tool_calls"]) == 1
    assert row["total_nodes_executed"] == 1

