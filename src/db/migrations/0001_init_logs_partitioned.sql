-- migrations/0001_init_logs_partitioned.sql

-- Partitioned parent table (weekly ranges by timestamp)
CREATE TABLE logs (
    id BIGSERIAL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    level VARCHAR(10) NOT NULL,
    service VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (id, "timestamp")
) PARTITION BY RANGE ("timestamp");

-- Composite index for keyset pagination (created on parent,
-- Postgres propagates it to every partition automatically)
CREATE INDEX idx_logs_timestamp_id ON logs ("timestamp" DESC, id DESC);

-- GIN index on attributes, using jsonb_path_ops (lighter, equality-only)
CREATE INDEX idx_logs_attributes_gin ON logs USING GIN (attributes jsonb_path_ops);

-- Helpful secondary indexes for exact-match filters (service, level)
CREATE INDEX idx_logs_service ON logs (service);
CREATE INDEX idx_logs_level ON logs (level);

-- Example initial partition (one week) — the app/migration logic
-- will need to create future weekly partitions ahead of time
CREATE TABLE logs_2026_w32 PARTITION OF logs
    FOR VALUES FROM ('2026-08-03T00:00:00Z') TO ('2026-08-10T00:00:00Z');