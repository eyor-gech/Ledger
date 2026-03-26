from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ledger.domain.aggregates.agent_session import AgentSession
from ledger.schema.events import StoredEvent


STRICT_NODE_SEQUENCE: list[str] = [
    "validate_inputs",
    "open_aggregate_record",
    "load_external_data",
    # domain nodes are agent-specific; we treat them as dynamic
    "write_output",
]


@dataclass(frozen=True, slots=True)
class AgentContextReconstruction:
    context_text: str
    last_event_position: int
    pending_work: list[str]
    session_health_status: str  # HEALTHY|RUNNING|FAILED|NEEDS_RECONCILIATION|RECOVERED|UNKNOWN


def _as_iso(v: Any) -> str:
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)


def _verbatim_event_line(e: dict[str, Any]) -> str:
    body = {
        "stream_position": int(e.get("stream_position") or -1),
        "event_type": str(e.get("event_type") or ""),
        "recorded_at": _as_iso(e.get("recorded_at")) if e.get("recorded_at") else None,
        "payload": e.get("payload") or {},
    }
    return json.dumps(body, sort_keys=True, ensure_ascii=False, default=_as_iso)


async def reconstruct_agent_context(store: Any, *, agent_stream_id: str) -> AgentContextReconstruction:
    """
    Gas Town Recovery primitive.

    Reconstructs an LLM-friendly context summary for an agent session stream:
    - Preserves the last 3 events verbatim.
    - Preserves any FAILED/ERROR-ish events verbatim (even if older).
    - Summarizes older events.
    - Produces pending work based on strict node sequence + executed nodes.
    """
    events = await store.load_stream(agent_stream_id, from_position=0)
    if not events:
        return AgentContextReconstruction(
            context_text=f"No events found for {agent_stream_id}.",
            last_event_position=-1,
            pending_work=[],
            session_health_status="UNKNOWN",
        )

    last_pos = int(events[-1].get("stream_position") or -1)

    # Identify verbatim keep set.
    last_three = events[-3:]
    error_like = [
        e
        for e in events
        if any(
            token in str(e.get("event_type") or "")
            for token in ("Failed", "ValidationFailed", "Error")
        )
    ]
    verbatim = {id(e) for e in last_three} | {id(e) for e in error_like}

    executed_nodes: list[str] = []
    tool_calls = 0
    for e in events:
        if str(e.get("event_type")) == "AgentNodeExecuted":
            node_name = str((e.get("payload") or {}).get("node_name") or "")
            if node_name:
                executed_nodes.append(node_name)
        if str(e.get("event_type")) == "AgentToolCalled":
            tool_calls += 1

    # Attempt aggregate reconstruction for additional signals.
    health = "RUNNING"
    try:
        stored = [StoredEvent.from_row(r) for r in events]
        session = AgentSession.rebuild(stored)  # type: ignore[arg-type]
        if session.status == "COMPLETED":
            health = "HEALTHY"
        elif session.status == "FAILED":
            # If we failed without a clear last node, we need reconciliation.
            health = "NEEDS_RECONCILIATION" if not session.recovery_point else "FAILED"
        elif session.status == "RECOVERED":
            health = "RECOVERED"
        else:
            health = "RUNNING"
    except Exception:
        health = "NEEDS_RECONCILIATION"

    # Pending work.
    pending: list[str] = []
    for node in STRICT_NODE_SEQUENCE:
        if node not in executed_nodes:
            pending.append(node)

    # If we've already executed the canonical early nodes, keep agent-specific work as implicit.
    if executed_nodes and "write_output" not in executed_nodes and "write_output" not in pending:
        pending.append("write_output")

    older = events[: max(0, len(events) - 3)]
    summary = (
        f"Agent stream: {agent_stream_id}\n"
        f"Events: {len(events)} (last_position={last_pos})\n"
        f"Nodes executed: {len(executed_nodes)}\n"
        f"Tool calls: {tool_calls}\n"
        f"Session health: {health}\n"
        f"Older events summarized: {len(older)}\n"
    )

    verbatim_lines = []
    for e in events:
        if id(e) in verbatim:
            verbatim_lines.append(_verbatim_event_line(e))

    context = summary + "\nVerbatim recent/error events:\n" + "\n".join(verbatim_lines[-12:])

    return AgentContextReconstruction(
        context_text=context,
        last_event_position=last_pos,
        pending_work=pending,
        session_health_status=health,
    )

