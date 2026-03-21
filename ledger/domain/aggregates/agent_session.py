"""
ledger/domain/aggregates/agent_session.py

AgentSession aggregate (Phase 1):
- Rebuilds deterministic session state from agent stream.
- Enforces monotonic node sequence and terminal session states.
- Provides "Gas Town" recovery primitives (recoverable failure + recovery marker).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from ledger.domain.errors import InvariantViolation, ModelVersionMismatch
from ledger.schema.events import AgentType, BaseEvent, StoredEvent, deserialize_event


@dataclass(slots=True)
class AgentSession:
    session_id: str
    agent_type: AgentType
    application_id: str | None = None
    agent_id: str | None = None
    model_version: str | None = None
    langgraph_graph_version: str | None = None
    context_source: str | None = None
    context_loaded: bool = False
    context_hash: str | None = None
    started_at: datetime | None = None
    status: str = "NEW"  # NEW|RUNNING|COMPLETED|FAILED|RECOVERED
    last_node_sequence: int = 0
    nodes_executed: list[str] = field(default_factory=list)
    tool_calls: int = 0
    llm_calls: int = 0
    total_tokens_used: int = 0
    total_cost_usd: float = 0.0
    output_events_written: list[dict] = field(default_factory=list)
    recovered_from_session_id: str | None = None
    recovery_point: str | None = None
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

    def guard_model_version(self, model_version: str) -> None:
        if self.model_version is None:
            raise InvariantViolation("AgentSession has no model_version yet")
        if self.model_version != model_version:
            raise ModelVersionMismatch(self.model_version, model_version)

    def guard_can_write_output(self, model_version: str) -> None:
        if self.status != "RUNNING":
            raise InvariantViolation(f"AgentSession not RUNNING (status={self.status})")
        if not self.context_loaded:
            raise InvariantViolation("AgentSession has not loaded context (AgentContextLoaded missing)")
        self.guard_model_version(model_version)

    def on_AgentSessionStarted(self, event: BaseEvent) -> None:
        p = event.to_payload()
        if self.status != "NEW":
            raise InvariantViolation("session already started")
        self.status = "RUNNING"
        self.application_id = str(p["application_id"])
        self.agent_id = str(p["agent_id"])
        self.model_version = str(p["model_version"])
        self.langgraph_graph_version = str(p["langgraph_graph_version"])
        self.context_source = str(p["context_source"])
        self.started_at = p["started_at"] if isinstance(p["started_at"], datetime) else datetime.fromisoformat(str(p["started_at"]))

    def on_AgentContextLoaded(self, event: BaseEvent) -> None:
        p = event.to_payload()
        if self.status != "RUNNING":
            raise InvariantViolation("context load requires RUNNING session")
        mv = str(p["model_version"])
        if self.model_version is not None and self.model_version != mv:
            raise ModelVersionMismatch(self.model_version, mv)
        self.context_loaded = True
        self.context_source = str(p["context_source"])
        self.context_hash = str(p["context_hash"])

    def on_AgentInputValidated(self, event: BaseEvent) -> None:
        if self.status != "RUNNING":
            raise InvariantViolation("input validation requires RUNNING session")

    def on_AgentInputValidationFailed(self, event: BaseEvent) -> None:
        if self.status != "RUNNING":
            raise InvariantViolation("validation failure requires RUNNING session")
        self.status = "FAILED"

    def on_AgentNodeExecuted(self, event: BaseEvent) -> None:
        if self.status != "RUNNING":
            raise InvariantViolation("node execution requires RUNNING session")
        p = event.to_payload()
        seq = int(p["node_sequence"])
        if seq != self.last_node_sequence + 1:
            raise InvariantViolation(
                f"node_sequence must be contiguous: expected {self.last_node_sequence + 1}, got {seq}"
            )
        self.last_node_sequence = seq
        self.nodes_executed.append(str(p["node_name"]))
        if bool(p.get("llm_called")):
            self.llm_calls += 1
            self.total_tokens_used += int(p.get("llm_tokens_input") or 0) + int(p.get("llm_tokens_output") or 0)
            self.total_cost_usd += float(p.get("llm_cost_usd") or 0.0)

    def on_AgentToolCalled(self, event: BaseEvent) -> None:
        if self.status != "RUNNING":
            raise InvariantViolation("tool call requires RUNNING session")
        self.tool_calls += 1

    def on_AgentOutputWritten(self, event: BaseEvent) -> None:
        if self.status != "RUNNING":
            raise InvariantViolation("output write requires RUNNING session")
        p = event.to_payload()
        self.output_events_written.extend(list(p.get("events_written") or []))

    def on_AgentSessionCompleted(self, event: BaseEvent) -> None:
        if self.status != "RUNNING":
            raise InvariantViolation("completion requires RUNNING session")
        p = event.to_payload()
        self.status = "COMPLETED"
        self.total_tokens_used = int(p.get("total_tokens_used") or self.total_tokens_used)
        self.total_cost_usd = float(p.get("total_cost_usd") or self.total_cost_usd)

    def on_AgentSessionFailed(self, event: BaseEvent) -> None:
        if self.status != "RUNNING":
            raise InvariantViolation("failure requires RUNNING session")
        p = event.to_payload()
        self.status = "FAILED"
        self.recovery_point = str(p.get("last_successful_node") or "") or None

    def on_AgentSessionRecovered(self, event: BaseEvent) -> None:
        if self.status not in {"RUNNING", "FAILED", "NEW"}:
            raise InvariantViolation("recovery requires non-terminal session")
        p = event.to_payload()
        self.status = "RECOVERED"
        self.recovered_from_session_id = str(p["recovered_from_session_id"])
        self.recovery_point = str(p["recovery_point"])

    @classmethod
    def rebuild(cls, events: Iterable[BaseEvent]) -> "AgentSession":
        events_list = list(events)
        if not events_list:
            raise ValueError("cannot rebuild AgentSession from empty event list")

        if isinstance(events_list[0], StoredEvent):
            first = deserialize_event(events_list[0].event_type, events_list[0].payload).to_payload()
            sid = str(first.get("session_id") or "")
            at = first.get("agent_type")
            if not sid or at is None:
                raise ValueError("first agent event must include session_id and agent_type")
            agent_type = at if isinstance(at, AgentType) else AgentType(str(at))
            agg = cls(session_id=sid, agent_type=agent_type)
            for se in events_list:
                agg.apply_stored(se)
            return agg

        first = events_list[0].to_payload()
        sid = str(first.get("session_id") or "")
        at = first.get("agent_type")
        if not sid or at is None:
            raise ValueError("first agent event must include session_id and agent_type")
        agent_type = at if isinstance(at, AgentType) else AgentType(str(at))
        agg = cls(session_id=sid, agent_type=agent_type)
        for e in events_list:
            agg.apply(e)
        return agg
