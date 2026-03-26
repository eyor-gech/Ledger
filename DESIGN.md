# DESIGN — The Ledger (Production-Grade Event Sourcing in Python/Async)

## 1) Aggregate Boundaries: Why ComplianceRecord separate from LoanApplication? Merge coupling → specific concurrent write failure?
**Why separate**
- `LoanApplication` (`loan-{application_id}`) owns the **lifecycle state machine** and final decision invariants.
- `ComplianceRecord` (`compliance-{application_id}`) owns **rule evaluation facts** and the compliance verdict.

This separation keeps each aggregate’s invariants cohesive and minimizes OCC contention:
- Compliance writes tend to be **bursty** (many rule outcomes).
- Loan lifecycle writes are **stage transitions** and final decisions.

**Concrete “merge failure” if Compliance were merged into `loan-{id}`**
- Compliance agent appends N rule outcome events while decision orchestrator appends `DecisionGenerated`.
- With a single merged stream:
  - both writers contend on the same `event_streams` row lock,
  - the decision writer frequently loses with `OptimisticConcurrencyError`,
  - the system burns retry budget and increases tail latency.

Keeping compliance separate allows:
- compliance to progress independently,
- decision orchestration to read compliance facts via projection or replay without being blocked by compliance’s higher write volume.

## 2) Projection Strategy: Per projection: Inline vs Async + SLO. ComplianceAuditView: snapshot trigger (event/time/manual) + invalidation logic?
**Projection mode**
- **Inline projections** (inside `append()` transaction) provide “read-your-writes” but increase write latency and coupling.
- **Async projections** (daemon) preserve write throughput and isolate failures at the cost of lag.

This repo uses **async projections**:
- triggers via `NOTIFY ledger_events`,
- sequential consistency by processing `events.global_position` in order,
- durable checkpoints (`projection_checkpoints`).

**SLO**
- Target projection lag: `< 500ms` for interactive screens under moderate load.
- `ledger://health` exposes per-projection lag in ms so clients can show freshness.

**ComplianceAuditView snapshots**
- `ComplianceAuditView.get_compliance_at(application_id, timestamp)` reconstructs from the compliance stream “as-of” time (millisecond precision).
- `ComplianceAuditView.rebuild_from_scratch()` uses a **shadow table + atomic swap**:
  - build `compliance_audit_rebuild_*` from `events`,
  - swap tables in a short transaction,
  - old table serves reads until swap (no downtime).

**Invalidation logic**
- Any new compliance-related event (e.g., `ComplianceRuleFailed`, `ComplianceCheckCompleted`) invalidates cached compliance state for that `application_id` (projection updates are idempotent by global_position).

## 3) Concurrency Analysis: Peak (100 apps, 4 agents): Expected OptimisticConcurrencyErrors/min on loan-{id} streams? Retry strategy + max budget?
**Mechanism**
- Per stream, `EventStore.append()` uses `SELECT ... FOR UPDATE` on `event_streams` and checks `expected_version` strictly.
- Losers get `OptimisticConcurrencyError(stream_id, expected, actual)` and must reload + retry.

**Estimate (explicitly approximate)**
Assume:
- For a given application, 4 agents can attempt to append to `loan-{id}` concurrently (e.g., orchestrator + compliance + fraud + credit) during transitions.
- The critical section for a stream lock is ~`20ms` (transaction + inserts).
- Each agent attempts one write per ~`2s` during active processing.

Approx collision rate per application (rough):
- 4 writers × 0.5 writes/s each = 2 writes/s total.
- Probability of overlap in a 20ms window ≈ `rate * window` = `2 * 0.02 = 0.04` (4%).
- Expected OCC failures per second per app ≈ overlaps × competing writers ≈ ~`0.04 * (some factor ~1)` → ~0.04/s.
- Over a minute of active contention: ~`2.4` OCC failures/app/min.

Peak 100 applications (worst-case all contending) would be ~`240` OCC failures/min. In practice, contention is far lower because:
- agents are orchestrated sequentially most of the time,
- many writes are to non-loan streams (`credit-*`, `compliance-*`, `agent-*`).

**Retry strategy**
- Retry only on **transient** DB errors inside `EventStore.append()` (deadlocks/serialization).
- For OCC mismatches:
  - reload aggregate(s),
  - re-run guards,
  - retry append with updated `expected_version`.

**Budget**
- For command handlers: max 3 OCC retries (bounded latency).
- For agent node writes: `BaseApexAgent.append_events_with_occ_retry()` uses bounded exponential backoff.

## 4) Upcasting Inference: Per inferred field: error rate + downstream impact. Null vs inference: when?
**Principles**
- Upcasters run **only on read**; raw DB payload remains immutable.
- When inference is ambiguous: set `None` (or empty containers for “unknown set”) and let downstream treat it as unknown.

**Examples**
- `DecisionGenerated v1 -> v2`: add `model_versions`.
  - Inference: if v1 had a single `model_version`, mapping it to a namespaced dict is ambiguous.
  - Strategy: `model_versions={}` (unknown), error rate minimized; downstream treats missing as “no claim”.
- `CreditAnalysisCompleted v1 -> v2`: add `regulatory_basis` and `model_versions`.
  - Strategy: `regulatory_basis=[]`, `model_versions={}` to preserve correctness without fabrication.

**Downstream impact**
- Analytics: can safely group “unknown model_versions” separately.
- Compliance reporting: never fabricates justification fields.

## 5) EventStoreDB Mapping: PG schema → ES concepts (streams, $all sub, persistent subs). What ES provides your impl works harder for?
**Mapping**
- Streams: `events.stream_id` + `events.stream_position`
- `$all`: `events.global_position` (total order)
- Stream head: `event_streams.current_version`
- Metadata: `events.metadata` (includes `correlation_id`, `causation_id`, idempotency keys)
- Subscriptions:
  - implemented as `LISTEN/NOTIFY` + checkpointed polling
  - (no native persistent subscription group semantics)

**What ESDB provides that we implement manually**
- Subscription coordination + competing consumers: we rely on checkpoints + (optional) external coordination.
- Server-side projections: we maintain our own projector code + retry/skip policy.
- Stream truncation/metadata streams: we provide `archive_stream` and stream metadata manually.

## 6) Rethink One Decision: Single biggest architectural redo with another day. Distinguish built vs best.
**Built (pragmatic)**
- Projection daemon runs as a single-process async loop with retry + skip-after-retries.
- Agent session lookup for recovery can scan `$all` in demo/test mode.

**Best (with another day)**
- Add a dedicated **agent_session index projection** (session_id → stream_id, last_node_sequence, status).
  - Eliminates `$all` scans for Gas Town recovery and MCP session resources.
  - Reduces worst-case recovery lookup from O(total events) to O(1).
  - Improves performance predictability under large histories.

