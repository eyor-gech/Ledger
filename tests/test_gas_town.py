from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ledger.agents.gas_town import reconstruct_agent_context
from ledger.event_store import InMemoryEventStore


@pytest.mark.asyncio
async def test_reconstruct_agent_context_preserves_last_three_events_and_pending_work():
    store = InMemoryEventStore()
    app_id = "APEX-GAS-0001"
    session_id = "sess-doc-gt-1"
    stream_id = f"agent-document_processing-{session_id}"
    now = datetime.now(timezone.utc).isoformat()

    await store.append(
        stream_id,
        [
            {
                "event_type": "AgentSessionStarted",
                "event_version": 1,
                "payload": {
                    "session_id": session_id,
                    "agent_type": "document_processing",
                    "agent_id": "a1",
                    "application_id": app_id,
                    "model_version": "m",
                    "langgraph_graph_version": "1",
                    "context_source": "fresh",
                    "context_token_count": 0,
                    "started_at": now,
                },
            },
            {
                "event_type": "AgentContextLoaded",
                "event_version": 1,
                "payload": {
                    "session_id": session_id,
                    "agent_type": "document_processing",
                    "application_id": app_id,
                    "model_version": "m",
                    "context_source": "fresh",
                    "context_hash": "h",
                    "loaded_at": now,
                },
            },
            {
                "event_type": "AgentNodeExecuted",
                "event_version": 1,
                "payload": {
                    "session_id": session_id,
                    "agent_type": "document_processing",
                    "node_name": "validate_inputs",
                    "node_sequence": 1,
                    "input_keys": ["application_id"],
                    "output_keys": ["loan_events"],
                    "llm_called": False,
                    "duration_ms": 5,
                    "executed_at": now,
                },
            },
            {
                "event_type": "AgentToolCalled",
                "event_version": 1,
                "payload": {
                    "session_id": session_id,
                    "agent_type": "document_processing",
                    "tool_name": "load_uploaded_documents",
                    "tool_input_summary": "i",
                    "tool_output_summary": "o",
                    "tool_duration_ms": 10,
                    "called_at": now,
                },
            },
            {
                "event_type": "AgentSessionFailed",
                "event_version": 1,
                "payload": {
                    "session_id": session_id,
                    "agent_type": "document_processing",
                    "application_id": app_id,
                    "error_type": "Crash",
                    "error_message": "boom",
                    "last_successful_node": "validate_inputs",
                    "recoverable": True,
                    "failed_at": now,
                },
            },
        ],
        expected_version=-1,
    )

    rec = await reconstruct_agent_context(store, agent_stream_id=stream_id)
    assert rec.last_event_position == 4
    assert rec.session_health_status in {"FAILED", "NEEDS_RECONCILIATION"}
    assert "AgentNodeExecuted" in rec.context_text
    assert "AgentToolCalled" in rec.context_text
    assert "AgentSessionFailed" in rec.context_text
    assert "write_output" in rec.pending_work
