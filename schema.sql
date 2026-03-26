-- schema.sql — The Ledger (Phase 0)
-- PostgreSQL is the single source of truth.
-- Append-only events + outbox + snapshots + projection checkpoints.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ─────────────────────────────────────────────────────────────────────────────
-- EVENT STREAMS
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS event_streams (
    stream_id        TEXT PRIMARY KEY,
    aggregate_type   TEXT NOT NULL,
    current_version  INTEGER NOT NULL DEFAULT -1, -- last stream_position, -1 means empty
    last_event_hash  BYTEA NULL,                  -- per-stream hash chain head
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    archived_at      TIMESTAMPTZ NULL
);

-- ─────────────────────────────────────────────────────────────────────────────
-- EVENTS (LOG-AS-TRUTH)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events (
    global_position  BIGSERIAL PRIMARY KEY,       -- total order across all streams
    event_id         UUID NOT NULL DEFAULT gen_random_uuid(),
    stream_id        TEXT NOT NULL REFERENCES event_streams(stream_id),
    stream_position  INTEGER NOT NULL,            -- 0-based position within stream
    event_type       TEXT NOT NULL,
    event_version    INTEGER NOT NULL DEFAULT 1,
    payload          JSONB NOT NULL,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    prev_hash        BYTEA NULL,
    event_hash       BYTEA NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_events_event_id
    ON events(event_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_events_stream_pos
    ON events(stream_id, stream_position);

CREATE INDEX IF NOT EXISTS ix_events_stream_id_pos
    ON events(stream_id, stream_position);

CREATE INDEX IF NOT EXISTS ix_events_global_position
    ON events(global_position);

CREATE INDEX IF NOT EXISTS ix_events_event_type
    ON events(event_type);

CREATE INDEX IF NOT EXISTS ix_events_recorded_at
    ON events(recorded_at);

CREATE INDEX IF NOT EXISTS ix_events_event_type_recorded_at
    ON events(event_type, recorded_at);

-- ─────────────────────────────────────────────────────────────────────────────
-- OUTBOX (IN-TRANSACTION WITH EVENTS)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS outbox (
    outbox_id       BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    stream_id       TEXT NOT NULL,
    stream_position INTEGER NOT NULL,
    topic           TEXT NOT NULL,
    payload         JSONB NOT NULL,
    headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT NOT NULL DEFAULT 'PENDING', -- PENDING|PUBLISHED|FAILED
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    locked_at       TIMESTAMPTZ NULL,
    locked_by       TEXT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at    TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS ix_outbox_status_created
    ON outbox(status, created_at);

CREATE INDEX IF NOT EXISTS ix_outbox_status_next_attempt
    ON outbox(status, next_attempt_at, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS ux_outbox_stream_topic
    ON outbox(stream_id, stream_position, topic);

-- ─────────────────────────────────────────────────────────────────────────────
-- SNAPSHOTS (OPTIONAL, READ OPTIMIZATION)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS snapshots (
    stream_id       TEXT PRIMARY KEY REFERENCES event_streams(stream_id) ON DELETE CASCADE,
    stream_version  INTEGER NOT NULL,
    snapshot        JSONB NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- PROJECTION CHECKPOINTS (GLOBAL POSITION)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    global_position BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- APPLICANT REGISTRY (READ-ONLY, SEE ledger/registry/client.py)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS applicant_registry;

CREATE TABLE IF NOT EXISTS applicant_registry.companies (
    company_id          TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    industry            TEXT NOT NULL,
    naics               TEXT NOT NULL,
    jurisdiction        TEXT NOT NULL,
    legal_type          TEXT NOT NULL,
    founded_year        INTEGER NOT NULL,
    employee_count      INTEGER NOT NULL,
    risk_segment        TEXT NOT NULL,
    trajectory          TEXT NOT NULL,
    submission_channel  TEXT NOT NULL DEFAULT 'UNKNOWN',
    ip_region           TEXT NOT NULL DEFAULT 'UNKNOWN',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS ix_registry_companies_jurisdiction
    ON applicant_registry.companies(jurisdiction);

CREATE INDEX IF NOT EXISTS ix_registry_companies_risk_segment
    ON applicant_registry.companies(risk_segment);

CREATE TABLE IF NOT EXISTS applicant_registry.financial_history (
    company_id               TEXT NOT NULL REFERENCES applicant_registry.companies(company_id) ON DELETE CASCADE,
    fiscal_year              INTEGER NOT NULL,
    total_revenue            NUMERIC(15,2) NOT NULL,
    gross_profit             NUMERIC(15,2) NOT NULL,
    operating_income         NUMERIC(15,2) NOT NULL,
    ebitda                   NUMERIC(15,2) NOT NULL,
    net_income               NUMERIC(15,2) NOT NULL,
    total_assets             NUMERIC(15,2) NOT NULL,
    total_liabilities         NUMERIC(15,2) NOT NULL,
    total_equity             NUMERIC(15,2) NOT NULL,
    long_term_debt           NUMERIC(15,2) NOT NULL,
    cash_and_equivalents     NUMERIC(15,2) NOT NULL,
    current_assets           NUMERIC(15,2) NOT NULL,
    current_liabilities      NUMERIC(15,2) NOT NULL,
    accounts_receivable      NUMERIC(15,2) NOT NULL,
    inventory                NUMERIC(15,2) NOT NULL,
    debt_to_equity           NUMERIC(10,4) NOT NULL,
    current_ratio            NUMERIC(10,4) NOT NULL,
    debt_to_ebitda           NUMERIC(10,4) NOT NULL,
    interest_coverage_ratio  NUMERIC(10,4) NOT NULL,
    gross_margin             NUMERIC(10,4) NOT NULL,
    ebitda_margin            NUMERIC(10,4) NOT NULL,
    net_margin               NUMERIC(10,4) NOT NULL,
    PRIMARY KEY (company_id, fiscal_year)
);

CREATE INDEX IF NOT EXISTS ix_registry_financial_history_company_year
    ON applicant_registry.financial_history(company_id, fiscal_year);

CREATE TABLE IF NOT EXISTS applicant_registry.compliance_flags (
    company_id  TEXT NOT NULL REFERENCES applicant_registry.companies(company_id) ON DELETE CASCADE,
    flag_type   TEXT NOT NULL,
    severity    TEXT NOT NULL,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    added_date  DATE NOT NULL,
    note        TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (company_id, flag_type, added_date)
);

CREATE INDEX IF NOT EXISTS ix_registry_compliance_flags_active
    ON applicant_registry.compliance_flags(company_id, is_active, severity);

CREATE TABLE IF NOT EXISTS applicant_registry.loan_relationships (
    company_id             TEXT NOT NULL REFERENCES applicant_registry.companies(company_id) ON DELETE CASCADE,
    relationship_id        TEXT NOT NULL,
    product_type           TEXT NOT NULL,
    start_date             DATE NOT NULL,
    original_amount_usd    NUMERIC(15,2) NOT NULL,
    current_balance_usd    NUMERIC(15,2) NOT NULL,
    default_occurred       BOOLEAN NOT NULL DEFAULT FALSE,
    last_payment_date      DATE NULL,
    relationship_status    TEXT NOT NULL DEFAULT 'CLOSED',
    notes                  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (company_id, relationship_id)
);

CREATE INDEX IF NOT EXISTS ix_registry_loan_relationships_company
    ON applicant_registry.loan_relationships(company_id, start_date);

-- ─────────────────────────────────────────────────────────────────────────────
-- PROJECTION READ MODELS (PHASE 4)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS application_summary (
    application_id            TEXT PRIMARY KEY,
    applicant_id              TEXT NULL,
    state                     TEXT NULL,
    requested_amount_usd      NUMERIC(15,2) NULL,
    loan_purpose              TEXT NULL,
    last_decision_recommendation TEXT NULL,
    last_decision_confidence  DOUBLE PRECISION NULL,
    last_global_position      BIGINT NOT NULL DEFAULT 0,
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS ix_application_summary_state
    ON application_summary(state, updated_at);

CREATE TABLE IF NOT EXISTS compliance_audit (
    application_id       TEXT PRIMARY KEY,
    overall_verdict      TEXT NULL,
    has_hard_block       BOOLEAN NOT NULL DEFAULT FALSE,
    rules_passed         INTEGER NOT NULL DEFAULT 0,
    rules_failed         INTEGER NOT NULL DEFAULT 0,
    rules_noted          INTEGER NOT NULL DEFAULT 0,
    failed_rule_ids      TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    last_global_position BIGINT NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS ix_compliance_audit_verdict
    ON compliance_audit(overall_verdict, updated_at);

CREATE TABLE IF NOT EXISTS agent_trace (
    session_id           TEXT PRIMARY KEY,
    agent_type           TEXT NOT NULL,
    application_id       TEXT NOT NULL,
    context_source       TEXT NULL,
    model_version        TEXT NULL,
    started_at           TIMESTAMPTZ NULL,
    completed_at         TIMESTAMPTZ NULL,
    failed_at            TIMESTAMPTZ NULL,
    recovered_from       TEXT NULL,
    total_nodes_executed INTEGER NOT NULL DEFAULT 0,
    total_llm_calls      INTEGER NOT NULL DEFAULT 0,
    total_tokens_used    INTEGER NOT NULL DEFAULT 0,
    total_cost_usd       NUMERIC(18,6) NOT NULL DEFAULT 0,
    total_duration_ms    INTEGER NOT NULL DEFAULT 0,
    nodes_executed       JSONB NOT NULL DEFAULT '[]'::jsonb,
    tool_calls           JSONB NOT NULL DEFAULT '[]'::jsonb,
    last_global_position BIGINT NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS ix_agent_trace_application
    ON agent_trace(application_id, agent_type, started_at);

-- Aggregate performance ledger (rollups for SLO monitoring / cost reporting)
CREATE TABLE IF NOT EXISTS agent_performance_ledger (
    agent_type           TEXT NOT NULL,
    model_version        TEXT NOT NULL,
    sessions_completed   INTEGER NOT NULL DEFAULT 0,
    sessions_failed      INTEGER NOT NULL DEFAULT 0,
    total_tokens_used    BIGINT NOT NULL DEFAULT 0,
    total_cost_usd       NUMERIC(18,6) NOT NULL DEFAULT 0,
    total_duration_ms    BIGINT NOT NULL DEFAULT 0,
    last_global_position BIGINT NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (agent_type, model_version)
);

CREATE INDEX IF NOT EXISTS ix_agent_performance_updated
    ON agent_performance_ledger(updated_at);

-- Projection failure tracking (fault tolerance / skip-after-retries)
CREATE TABLE IF NOT EXISTS projection_failures (
    projection_name  TEXT NOT NULL,
    global_position  BIGINT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT NULL,
    first_failed_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_failed_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection_name, global_position)
);

CREATE INDEX IF NOT EXISTS ix_projection_failures_recent
    ON projection_failures(projection_name, last_failed_at);

COMMIT;
