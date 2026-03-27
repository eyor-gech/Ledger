[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/eyor-gech/Ledger)
# The Ledger — Event-Sourced, Multi-Agent Loan Decisioning

An immutable AI “flight recorder” built on **Event Sourcing + CQRS + DDD**:
- PostgreSQL is the **single source of truth** (append-only `events` log).
- Every agent action is replayable (`agent-{agent_type}-{session_id}` streams).
- Every state is temporally queryable (`recorded_at`, `global_position`).
- Every event is cryptographically verifiable (SHA-256 hash chain + integrity attestations).

## Requirements
- Python `3.12+`
- PostgreSQL `16+`

## Setup
```bash
# 1) Install deps (recommended: venv)
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 2) Start Postgres (example: Docker)
docker run -d --name apex-ledger-pg ^
  -e POSTGRES_PASSWORD=apex ^
  -e POSTGRES_DB=apex_ledger ^
  -p 5432:5432 postgres:16

# 3) Configure env
copy .env.example .env
# Edit .env:
#   DATABASE_URL=postgresql://postgres:apex@localhost/apex_ledger
#   TEST_DB_URL=postgresql://postgres:apex@localhost/apex_ledger_test
#   ANTHROPIC_API_KEY=... (optional; tests use a fake client if missing)

# 4) Initialize schema (idempotent)
.\.venv\Scripts\python.exe init_db.py --db-url %DATABASE_URL%

# 5) Generate seed data (registry + documents + optional seed events)
.\.venv\Scripts\python.exe datagen\generate_all.py --db-url %DATABASE_URL%
```

## Run tests (all phases)
```bash
.\.venv\Scripts\python.exe -m pytest -q
```

## Run the demo (Week-standard)
```bash
.\.venv\Scripts\python.exe scripts\run_demo.py --application-id NARR05-DEMO
```

Artifacts are written under `artifacts\`:
- `artifacts\api_cost_report.txt`
- `artifacts\regulatory_package_NARR05.json`

## Run projections (Phase 4)
Two options exist:

1) One-shot catch-up (good for CI / rebuilds):
```python
import asyncio, asyncpg, os
from ledger.event_store import EventStore
from ledger.projections.runner import ProjectionSpec, run_once
from ledger.projections.projectors import project_application_summary, project_compliance_audit, project_agent_trace, project_agent_performance_ledger

async def main():
  url = os.environ["DATABASE_URL"]
  store = EventStore(url); await store.connect()
  pool = await asyncpg.create_pool(url)
  projections = [
    ProjectionSpec("application_summary", project_application_summary),
    ProjectionSpec("compliance_audit", project_compliance_audit),
    ProjectionSpec("agent_trace", project_agent_trace),
    ProjectionSpec("agent_performance_ledger", project_agent_performance_ledger),
  ]
  await run_once(store=store, pool=pool, projections=projections)
  await pool.close(); await store.close()

asyncio.run(main())
```

2) Daemon (LISTEN/NOTIFY + retries + skip-after-retries):
Use `ledger/projections/daemon.py::ProjectionDaemon` in your service host.

## MCP server (Phase 5)
Start the MCP server (stdio transport):
```bash
.\.venv\Scripts\python.exe scripts\run_mcp.py
```

### Tools
- `submit_application`
- `start_agent_session`
- `record_credit_analysis`
- `record_fraud_screening`
- `record_compliance_check`
- `generate_decision`
- `record_human_review`
- `run_integrity_check`

All tools require `correlation_id` and `causation_id` for full causal chain reconstruction.

### Resources
- `ledger://applications/{application_id}`
- `ledger://applications/{application_id}/compliance`
- `ledger://agent_sessions/{session_id}`
- `ledger://audit/{entity_id}`
- `ledger://audit/{application_id}/temporal?as_of=...`
- `ledger://projections`
- `ledger://health`

## Query examples
```sql
-- Read a stream (ordered)
SELECT stream_position, event_type, event_version, recorded_at, payload
FROM events
WHERE stream_id = 'loan-APEX-0001'
ORDER BY stream_position ASC;

-- Read $all in global order (bounded)
SELECT global_position, stream_id, event_type, recorded_at
FROM events
WHERE global_position >= 0
ORDER BY global_position ASC
LIMIT 500;

-- Projection checkpoints
SELECT projection_name, global_position, updated_at
FROM projection_checkpoints
ORDER BY projection_name ASC;
```

