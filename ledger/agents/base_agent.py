"""
ledger/agents/base_agent.py — Replay-safe LangGraph agent base
==============================================================
Phase 3 requirements:
- Async-first execution with deterministic node sequence
- Replay-safe logging (AgentNodeExecuted / AgentToolCalled / AgentOutputWritten)
- OCC append with retry and idempotency keys
- Gas Town recovery: resume from last AgentNodeExecuted of a failed session
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
import dataclasses
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import uuid4

from langgraph.graph import END, StateGraph

from ledger.event_store import OptimisticConcurrencyError
from ledger.schema.events import (
    AgentContextLoaded,
    AgentInputValidated,
    AgentNodeExecuted,
    AgentOutputWritten,
    AgentSessionCompleted,
    AgentSessionFailed,
    AgentSessionRecovered,
    AgentSessionStarted,
    AgentToolCalled,
    AgentType,
)

LANGGRAPH_GRAPH_VERSION = "1.0.0"
MAX_OCC_RETRIES = 5


@dataclass(frozen=True, slots=True)
class RecoveryInfo:
    prior_session_id: str
    prior_session_stream: str
    last_successful_node_sequence: int
    last_successful_node_name: str | None


class BaseApexAgent(ABC):
    """
    Base class for all Apex agents.

    Strict node contract (per-agent graph must follow this sequence):
      validate_inputs → open_aggregate_record → load_external_data → [domain nodes] → write_output
    """

    def __init__(
        self,
        *,
        agent_id: str,
        agent_type: AgentType,
        store: Any,
        registry: Any,
        llm_client: Any,
        model_version: str,
    ) -> None:
        self.agent_id = str(agent_id)
        self.agent_type = agent_type
        self.store = store
        self.registry = registry
        self.client = llm_client
        self.model_version = str(model_version)

        self.session_id: str | None = None
        self.application_id: str | None = None
        self._session_stream: str | None = None

        self._graph: Any | None = None
        self._seq: int = 0
        self._t0: float = 0.0
        self._llm_calls: int = 0
        self._tokens: int = 0
        self._cost_usd: float = 0.0
        self._last_node_name: str | None = None

        self._recovery: RecoveryInfo | None = None
        self._simulate_crash_after_node: str | None = None

    @abstractmethod
    def build_graph(self) -> Any:
        raise NotImplementedError

    def _ensure_graph(self) -> Any:
        if self._graph is None:
            self._graph = self.build_graph()
        return self._graph

    async def process_application(
        self,
        application_id: str,
        *,
        resume_if_possible: bool = True,
        simulate_crash_after_node: str | None = None,
    ) -> dict[str, Any]:
        self._ensure_graph()
        self.application_id = str(application_id)
        self._simulate_crash_after_node = simulate_crash_after_node

        self._seq = 0
        self._t0 = time.time()
        self._llm_calls = 0
        self._tokens = 0
        self._cost_usd = 0.0
        self._last_node_name = None

        if resume_if_possible:
            self._recovery = await self._find_recovery_info(self.application_id)
        else:
            self._recovery = None

        self.session_id = self._new_session_id()
        self._session_stream = f"agent-{self.agent_type.value}-{self.session_id}"

        await self._start_session()
        try:
            result = await self._graph.ainvoke(self._initial_state())
            await self._complete_session(next_agent_triggered=result.get("next_agent_triggered"))
            return dict(result)
        except Exception as exc:
            await self._fail_session(type(exc).__name__, str(exc))
            raise

    def _new_session_id(self) -> str:
        return f"sess-{self.agent_type.value[:3]}-{uuid4().hex[:10]}"

    def _initial_state(self) -> dict[str, Any]:
        if self.application_id is None or self.session_id is None:
            raise RuntimeError("process_application not started")
        return {
            "application_id": self.application_id,
            "session_id": self.session_id,
            "agent_id": self.agent_id,
            "recovery": self._recovery,
            "next_agent_triggered": None,
            "errors": [],
        }

    async def _start_session(self) -> None:
        if self.application_id is None or self.session_id is None or self._session_stream is None:
            raise RuntimeError("session not initialized")

        context_source = "fresh"
        recovered_from = None
        if self._recovery is not None:
            context_source = (
                f"prior_session_replay:{self._recovery.prior_session_id}:"
                f"node_seq={self._recovery.last_successful_node_sequence}"
            )
            recovered_from = self._recovery.prior_session_id

        await self._append_session_event(
            AgentSessionStarted(
                session_id=self.session_id,
                agent_type=self.agent_type,
                agent_id=self.agent_id,
                application_id=self.application_id,
                model_version=self.model_version,
                langgraph_graph_version=LANGGRAPH_GRAPH_VERSION,
                context_source=context_source,
                context_token_count=0,
                started_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

        if recovered_from is not None:
            await self._append_session_event(
                AgentSessionRecovered(
                    session_id=self.session_id,
                    agent_type=self.agent_type,
                    application_id=self.application_id,
                    recovered_from_session_id=recovered_from,
                    recovery_point=(
                        f"AgentNodeExecuted:{self._recovery.last_successful_node_sequence if self._recovery else 0}:"
                        f"{self._recovery.last_successful_node_name if self._recovery else ''}"
                    ),
                    recovered_at=datetime.now(timezone.utc),
                ).to_store_dict()
            )

        await self._append_session_event(
            AgentContextLoaded(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=self.application_id,
                model_version=self.model_version,
                context_source=context_source,
                context_hash=self._sha(
                    {
                        "application_id": self.application_id,
                        "agent_type": self.agent_type.value,
                        "recovered_from": recovered_from,
                    }
                ),
                loaded_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

    async def _complete_session(self, *, next_agent_triggered: str | None) -> None:
        if self.application_id is None or self.session_id is None:
            raise RuntimeError("session not initialized")
        ms = int((time.time() - self._t0) * 1000)
        await self._append_session_event(
            AgentSessionCompleted(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=self.application_id,
                total_nodes_executed=self._seq,
                total_llm_calls=self._llm_calls,
                total_tokens_used=self._tokens,
                total_cost_usd=round(self._cost_usd, 6),
                total_duration_ms=ms,
                next_agent_triggered=next_agent_triggered,
                completed_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

    async def _fail_session(self, error_type: str, error_message: str) -> None:
        if self.application_id is None or self.session_id is None:
            return
        await self._append_session_event(
            AgentSessionFailed(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=self.application_id,
                error_type=str(error_type),
                error_message=str(error_message)[:1000],
                last_successful_node=self._last_node_name,
                recoverable=True,
                failed_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

    # ──────────────────────────────────────────────────────────────────
    # Replay-safe recording
    # ──────────────────────────────────────────────────────────────────

    async def record_input_validated(self, inputs_validated: Iterable[str], duration_ms: int) -> None:
        if self.application_id is None or self.session_id is None:
            raise RuntimeError("session not initialized")
        await self._append_session_event(
            AgentInputValidated(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=self.application_id,
                inputs_validated=list(inputs_validated),
                validation_duration_ms=int(duration_ms),
                validated_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

    async def record_node_execution(
        self,
        node_name: str,
        input_keys: Iterable[str],
        output_keys: Iterable[str],
        duration_ms: int,
        *,
        llm_tokens_input: int | None = None,
        llm_tokens_output: int | None = None,
        llm_cost_usd: float | None = None,
    ) -> None:
        if self.application_id is None or self.session_id is None:
            raise RuntimeError("session not initialized")

        self._seq += 1
        self._last_node_name = str(node_name)
        if llm_tokens_input is not None or llm_tokens_output is not None:
            self._llm_calls += 1
            self._tokens += int(llm_tokens_input or 0) + int(llm_tokens_output or 0)
        if llm_cost_usd is not None:
            self._cost_usd += float(llm_cost_usd)

        await self._append_session_event(
            AgentNodeExecuted(
                session_id=self.session_id,
                agent_type=self.agent_type,
                node_name=str(node_name),
                node_sequence=int(self._seq),
                input_keys=list(input_keys),
                output_keys=list(output_keys),
                llm_called=llm_tokens_input is not None,
                llm_tokens_input=int(llm_tokens_input) if llm_tokens_input is not None else None,
                llm_tokens_output=int(llm_tokens_output) if llm_tokens_output is not None else None,
                llm_cost_usd=float(llm_cost_usd) if llm_cost_usd is not None else None,
                duration_ms=int(duration_ms),
                executed_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

        if self._simulate_crash_after_node and str(node_name) == self._simulate_crash_after_node:
            raise RuntimeError(f"Simulated crash after node: {node_name}")

    async def record_tool_call(self, tool_name: str, tool_input: Any, tool_output: Any, duration_ms: int) -> None:
        if self.application_id is None or self.session_id is None:
            raise RuntimeError("session not initialized")
        await self._append_session_event(
            AgentToolCalled(
                session_id=self.session_id,
                agent_type=self.agent_type,
                tool_name=str(tool_name),
                tool_input_summary=self._safe_summary(tool_input),
                tool_output_summary=self._safe_summary(tool_output),
                tool_duration_ms=int(duration_ms),
                called_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

    async def record_output_written(self, events_written: list[dict[str, Any]], output_summary: str) -> None:
        if self.application_id is None or self.session_id is None:
            raise RuntimeError("session not initialized")
        await self._append_session_event(
            AgentOutputWritten(
                session_id=self.session_id,
                agent_type=self.agent_type,
                application_id=self.application_id,
                events_written=list(events_written),
                output_summary=str(output_summary),
                written_at=datetime.now(timezone.utc),
            ).to_store_dict()
        )

    # ──────────────────────────────────────────────────────────────────
    # OCC append with idempotency
    # ──────────────────────────────────────────────────────────────────

    async def append_events_with_occ_retry(
        self,
        stream_id: str,
        event_dicts: list[dict[str, Any]],
        *,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        idempotency_key: str | None = None,
        extra_metadata: dict[str, Any] | None = None,
    ) -> list[int]:
        metadata = dict(extra_metadata or {})
        if idempotency_key:
            metadata["idempotency_key"] = str(idempotency_key)

        for attempt in range(MAX_OCC_RETRIES):
            expected = await self.store.stream_version(stream_id)
            # Strong idempotency: if key already exists in the stream tail, treat as success.
            if idempotency_key:
                tail = await self.store.load_stream(stream_id, from_position=max(0, int(expected) - 50))
                if any((e.get("metadata") or {}).get("idempotency_key") == idempotency_key for e in tail):
                    return []
            try:
                return await self.store.append(
                    stream_id=stream_id,
                    events=event_dicts,
                    expected_version=expected,
                    correlation_id=correlation_id,
                    causation_id=causation_id,
                    metadata=metadata,
                )
            except OptimisticConcurrencyError as exc:
                if idempotency_key:
                    tail = await self.store.load_stream(stream_id, from_position=max(0, exc.actual - 25))
                    if any((e.get("metadata") or {}).get("idempotency_key") == idempotency_key for e in tail):
                        return []
                if attempt < MAX_OCC_RETRIES - 1:
                    await asyncio.sleep(0.05 * (2**attempt))
                    continue
                raise

        raise RuntimeError("append_events_with_occ_retry exceeded retries")

    async def _append_session_event(self, event_dict: dict[str, Any]) -> None:
        if self._session_stream is None:
            raise RuntimeError("session stream not initialized")
        idem = f"{self.session_id}:{event_dict.get('event_type')}:{event_dict.get('payload', {}).get('node_sequence', '')}"
        await self.append_events_with_occ_retry(
            self._session_stream,
            [event_dict],
            correlation_id=self.application_id,
            causation_id=self.session_id,
            idempotency_key=idem,
        )

    # ──────────────────────────────────────────────────────────────────
    # LLM helper
    # ──────────────────────────────────────────────────────────────────

    async def call_llm_json(self, *, system: str, user: str, max_tokens: int = 800) -> tuple[dict[str, Any], int, int, float]:
        last_exc: BaseException | None = None
        for attempt in range(3):
            try:
                resp = await self.client.messages.create(
                    model=self.model_version,
                    max_tokens=int(max_tokens),
                    system=str(system),
                    messages=[{"role": "user", "content": str(user)}],
                )
                text = self._extract_text(resp)
                usage = getattr(resp, "usage", None)
                tok_in = int(getattr(usage, "input_tokens", 0) or 0)
                tok_out = int(getattr(usage, "output_tokens", 0) or 0)
                est_cost = round(tok_in / 1e6 * 3.0 + tok_out / 1e6 * 15.0, 6)
                payload = self._parse_json_object(text)
                return payload, tok_in, tok_out, est_cost
            except Exception as exc:
                last_exc = exc
                if attempt < 2:
                    await asyncio.sleep(0.2 * (2**attempt))
                    continue
                raise RuntimeError(
                    f"LLM call failed after retries (agent_type={self.agent_type.value}, model={self.model_version})"
                ) from exc
        raise RuntimeError("LLM call failed") from last_exc

    # ──────────────────────────────────────────────────────────────────
    # Recovery scanning
    # ──────────────────────────────────────────────────────────────────

    async def _find_recovery_info(self, application_id: str) -> RecoveryInfo | None:
        """
        Finds the latest FAILED session for (application_id, agent_type) and returns
        the last successful AgentNodeExecuted info so a new run can resume.

        Implementation note: this scans events in global order; it is fine for tests/demo.
        Production would maintain an agent_session projection for O(1) lookup.
        """
        last_failed_session: str | None = None
        last_failed_stream: str | None = None
        last_node_seq: int = 0
        last_node_name: str | None = None

        async for e in self.store.load_all(from_position=0, batch_size=500):
            if e.get("event_type") == "AgentSessionStarted":
                p = e.get("payload") or {}
                if str(p.get("application_id")) == application_id and str(p.get("agent_type")) == self.agent_type.value:
                    sid = str(p.get("session_id"))
                    last_failed_session = sid
                    last_failed_stream = f"agent-{self.agent_type.value}-{sid}"
                    last_node_seq = 0
                    last_node_name = None
            elif e.get("event_type") == "AgentNodeExecuted" and last_failed_session:
                p = e.get("payload") or {}
                if str(p.get("session_id")) == last_failed_session and str(p.get("agent_type")) == self.agent_type.value:
                    last_node_seq = int(p.get("node_sequence") or 0)
                    last_node_name = str(p.get("node_name") or "") or None
            elif e.get("event_type") == "AgentSessionCompleted" and last_failed_session:
                p = e.get("payload") or {}
                if str(p.get("session_id")) == last_failed_session and str(p.get("agent_type")) == self.agent_type.value:
                    last_failed_session = None
                    last_failed_stream = None
                    last_node_seq = 0
                    last_node_name = None
            elif e.get("event_type") == "AgentSessionFailed" and last_failed_session:
                p = e.get("payload") or {}
                if str(p.get("session_id")) == last_failed_session and str(p.get("agent_type")) == self.agent_type.value:
                    if last_failed_stream is None:
                        last_failed_stream = f"agent-{self.agent_type.value}-{last_failed_session}"
                    return RecoveryInfo(
                        prior_session_id=last_failed_session,
                        prior_session_stream=last_failed_stream,
                        last_successful_node_sequence=last_node_seq,
                        last_successful_node_name=last_node_name,
                    )
        return None

    # ──────────────────────────────────────────────────────────────────
    # Pure helpers
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _sha(obj: Any) -> str:
        return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()

    @staticmethod
    def _as_dict(obj: Any) -> dict[str, Any]:
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return dict(obj)
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        d = getattr(obj, "__dict__", None)
        if isinstance(d, dict):
            return dict(d)
        slots = getattr(obj, "__slots__", None)
        if slots:
            out: dict[str, Any] = {}
            for s in list(slots):
                if hasattr(obj, s):
                    out[str(s)] = getattr(obj, s)
            return out
        return {"value": str(obj)}

    @staticmethod
    def _safe_summary(obj: Any, max_len: int = 800) -> str:
        try:
            s = json.dumps(obj, default=str, ensure_ascii=False)
        except Exception:
            s = str(obj)
        return s if len(s) <= max_len else s[: max_len - 3] + "..."

    @staticmethod
    def _extract_text(resp: Any) -> str:
        parts: list[str] = []
        for block in getattr(resp, "content", []) or []:
            t = getattr(block, "text", None)
            if t:
                parts.append(str(t))
        if parts:
            return "\n".join(parts)
        return str(getattr(resp, "content", "") or "")

    @staticmethod
    def _parse_json_object(text: str) -> dict[str, Any]:
        # Defensive: pull out the first {...} JSON object from the response.
        import re

        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            raise ValueError("LLM response did not contain JSON object")
        return json.loads(m.group())
