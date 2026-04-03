-- PostgreSQL initialization script for the streaming feature store.
-- This script is executed automatically by the postgres container on first start.
-- See docs/design/week1_kafka_postgres_docker_setup.md §2.6 for schema rationale.

-- ---------------------------------------------------------------------------
-- raw_events: landing table for all Kafka-ingested events
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_events (
    event_id        UUID            PRIMARY KEY,
    event_type      VARCHAR(50)     NOT NULL,
    user_id         VARCHAR(64)     NOT NULL,
    session_id      VARCHAR(64)     NOT NULL,
    event_timestamp TIMESTAMPTZ     NOT NULL,
    properties      JSONB           NOT NULL DEFAULT '{}',
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Index for time-range scans (offline feature computation in Week 4)
CREATE INDEX IF NOT EXISTS idx_raw_events_timestamp
    ON raw_events (event_timestamp);

-- Index for per-user queries (point-in-time joins in Week 4)
CREATE INDEX IF NOT EXISTS idx_raw_events_user_id
    ON raw_events (user_id);

-- Index for filtering by event type
CREATE INDEX IF NOT EXISTS idx_raw_events_event_type
    ON raw_events (event_type);

-- Composite index for the most common query pattern: user + time range
CREATE INDEX IF NOT EXISTS idx_raw_events_user_timestamp
    ON raw_events (user_id, event_timestamp);
