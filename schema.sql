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

COMMIT;
