-- 0001_init_logs_partitioned.sql
-- Applied by the runner in src/db/migrate.ts (not by drizzle-kit).
-- Must be idempotent: it is re-executed on every container start.

-- Partitioned parent table (weekly ranges by timestamp)
CREATE TABLE IF NOT EXISTS logs (
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
CREATE INDEX IF NOT EXISTS idx_logs_timestamp_id ON logs ("timestamp" DESC, id DESC);

-- GIN index on attributes, using jsonb_path_ops (lighter, equality-only)
CREATE INDEX IF NOT EXISTS idx_logs_attributes_gin ON logs USING GIN (attributes jsonb_path_ops);

-- Helpful secondary indexes for exact-match filters (service, level)
CREATE INDEX IF NOT EXISTS idx_logs_service ON logs (service);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs (level);

-- Creates the weekly partition covering `target` if it does not already exist.
-- Weeks are ISO weeks starting Monday 00:00 UTC. Called at startup to provision
-- the current and upcoming weeks so inserts never hit a missing partition.
CREATE OR REPLACE FUNCTION ensure_logs_partition(target TIMESTAMPTZ)
RETURNS TEXT AS $$
DECLARE
    week_start DATE;
    week_end   DATE;
    part_name  TEXT;
BEGIN
    week_start := date_trunc('week', target AT TIME ZONE 'UTC')::DATE;
    week_end   := week_start + 7;
    part_name  := format('logs_%s_w%s',
                         to_char(week_start, 'IYYY'),
                         to_char(week_start, 'IW'));

    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = part_name
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF logs FOR VALUES FROM (%L) TO (%L)',
            part_name, week_start, week_end
        );
    END IF;

    RETURN part_name;
END;
$$ LANGUAGE plpgsql;

-- A default partition catches any row outside the provisioned weekly ranges,
-- so an insert can never fail with "no partition of relation logs found".
CREATE TABLE IF NOT EXISTS logs_default PARTITION OF logs DEFAULT;
