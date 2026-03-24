# Domain Notes

## Core Invariants
- **Strict lifecycle transitions** are enforced by `LoanApplication` during event replay.
- **Confidence floor**: `DecisionGenerated.confidence < 0.60` implies `recommendation == "REFER"`.
- **Compliance gate**: approval requires a non-blocking compliance verdict (`CLEAR`/`CONDITIONAL`) and `has_hard_block = false`.
- **REG-003 (Montana)**: any applicant with `jurisdiction == "MT"` triggers a **hard block** and the application is declined by compliance.
- **Model version locking**: `AgentSession` locks `model_version` at `AgentSessionStarted` and guards output writes against mismatches.
- **Full causal chain**: agent-produced domain writes use `correlation_id = application_id` and `causation_id = session_id`.

## Agent Node Contract (Replay-Safe)
All agents follow the strict sequence:
`validate_inputs → open_aggregate_record → load_external_data → [domain nodes] → write_output`

Replay-safe logging is written to per-session streams:
`agent-{agent_type}-{session_id}` with:
- `AgentSessionStarted`, `AgentContextLoaded`
- `AgentInputValidated`
- `AgentToolCalled` (registry/doc loads; deterministic summaries)
- `AgentNodeExecuted` (durations + optional LLM usage)
- `AgentOutputWritten`, `AgentSessionCompleted` (or `AgentSessionFailed`)

## Gas Town Recovery
- If a prior session failed for the same `(application_id, agent_type)`, a new session starts with
  `context_source = "prior_session_replay:{prior_session_id}:node_seq=..."` and emits `AgentSessionRecovered`.
- Recovery resumes from the last `AgentNodeExecuted` by skipping already-completed nodes.
- Nodes that produce data required later must durably record it (e.g., Fraud agent writes a `fraud_assessment` tool-call envelope).

## Projection Guarantees
- Projections are **idempotent** and **checkpointed** via `projection_checkpoints.global_position`.
- Events are processed in **global_position** order to maintain sequential consistency.
- The event store emits `NOTIFY ledger_events` per append transaction to trigger realtime projection catch-up.

