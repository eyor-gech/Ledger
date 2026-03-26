# DOMAIN_NOTES — The Ledger (Event Sourcing + Multi-Agent Decisioning)

## 1) EDA vs ES: Callbacks (LangChain traces) capturing events = EDA or ES? Redesign with The Ledger: exact architecture changes + gains?
**Callbacks/traces** (LangChain/LangGraph tracing, logs, spans) are usually **EDA/observability**: they describe *what happened*, but they are not *the authoritative state transition log* unless you enforce:
- append-only persistence
- optimistic concurrency on stream head
- deterministic replay rules
- schema + invariants at aggregate boundaries

**The Ledger redesign** makes agent traces **first-class event streams**:
- Replace “trace sink” with `EventStore.append()` to `agent-{agent_type}-{session_id}` streams.
- Replace “state = DB row” with “state = replay of `loan-{application_id}` + related streams”.
- Replace “best-effort logs” with hash-chained, versioned event envelopes.

**Gains**
- **Auditability**: `correlation_id` and `causation_id` allow cross-stream causal reconstruction.
- **Replayability**: `AgentSession` rebuild enforces contiguous `node_sequence`.
- **Determinism**: projections rebuild from `events.global_position` (sequential consistency).
- **Integrity**: tampering becomes detectable via `event_hash` + integrity attestations (`AuditIntegrityCheckRun`).

## 2) Aggregates: 4 aggregates in loan scenario. One rejected boundary + coupling problem prevented?
**Implemented aggregates (write model)**
- `LoanApplication` (`loan-{application_id}`): lifecycle state machine + decision invariants.
- `ComplianceRecord` (`compliance-{application_id}`): rule outcomes + verdict.
- `AgentSession` (`agent-{agent_type}-{session_id}`): replay-safe agent execution timeline.
- `AuditLedger` (`audit-{entity_id}`): integrity check attestations.

**Rejected boundary (example): merging `ComplianceRecord` into `LoanApplication`**
- Coupling problem: compliance is evaluated by a separate agent (separate stream, separate OCC contention domain).
- If merged, concurrent appends from compliance and decision nodes would contend on `loan-{id}` much more often.
- Keeping compliance separate prevents a class of “unrelated writer” OCC failures and reduces retry pressure.

## 3) Concurrency: Two agents append_events(expected_version=3) same loan. Trace event store sequence, loser gets what, next steps?
**Sequence (Postgres EventStore, per-stream `SELECT ... FOR UPDATE`)**
1. Both agents read `event_streams.current_version == 3`.
2. Agent A enters `append()` transaction, locks `event_streams` row, verifies `expected_version == 3`, writes event(s), updates `current_version` to `4`, commits.
3. Agent B enters, locks row after A commits, sees `current_version == 4`, raises:
   `OptimisticConcurrencyError(stream_id, expected=3, actual=4)`.

**Loser behavior**
- Receives a typed `OptimisticConcurrencyError` with structured fields.

**Next steps**
- Reload latest state (replay stream from the winner’s new head).
- Re-run aggregate guards (no in-handler business conditionals).
- Re-append with updated `expected_version`, within a bounded retry budget.

## 4) Projection Lag: 200ms lag, officer sees stale credit limit post-disbursement. System response + UI communication?
**System response**
- Projections are async; read models may be stale. The source of truth remains the event log.
- On stale reads, UI should:
  - display “as of global_position / updated_at”
  - offer “refresh” (or long-poll) until the projection catches up
  - allow a “read-through” fallback to rebuild from events for critical screens

**UI contract**
- Expose `ledger://health` (per-projection lag in ms) so clients can communicate freshness explicitly.

## 5) Upcasting: CreditDecisionMade 2024→2026 schema. Write Python upcaster + historical inference strategy.
**Upcaster pattern (decorator registry)**
```python
from ledger.upcasters import UpcasterRegistry

reg = UpcasterRegistry()

@reg.register(event_type="DecisionGenerated", from_version=1, to_version=2)
def decision_generated_v1_to_v2(e: dict) -> dict:
    out = dict(e)
    p = dict(out.get("payload") or {})
    # New field added in 2026: model_versions
    # Inference strategy: do not fabricate; default to {} (unknown).
    p.setdefault("model_versions", {})
    out["payload"] = p
    return out
```

**Historical inference strategy**
- Only infer if the source is unambiguous (e.g., a legacy `model_version` field maps to a single `model_versions["decision"]`).
- Otherwise set to `None` or `{}` and let downstream logic treat it as unknown.

## 6) Marten Daemon: Python distributed projections equiv. Coordination primitive + failure guarded?
**Coordination primitive**
- Marten uses Postgres advisory locks + durable checkpoints.
- The Ledger uses:
  - `projection_checkpoints` (durable cursor)
  - `LISTEN/NOTIFY ledger_events` (realtime hint)
  - sequential consistency via `events.global_position`

**Failure mode and guard**
- Projection failures are retried per (projection,event).
- After retry budget, the daemon records into `projection_failures` and **advances the checkpoint** (skip) to avoid crashing the process.
- Operationally, skipped events are discoverable and can be replayed after a fix by resetting the checkpoint.

