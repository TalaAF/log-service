-- 0002_retention_pg_cron.sql
-- Applied by the runner in src/db/migrate.ts (not by drizzle-kit).
-- Must be idempotent: it is re-executed on every container start.
--
-- Retention runs inside Postgres via pg_cron rather than in the app process:
-- the database container has the larger resource budget, and deletion should
-- not depend on the app being up.

CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Single-row table holding the active retention window. index.ts upserts this
-- from RETENTION_DAYS on every boot, so changing the env var and restarting
-- changes retention without touching code or re-scheduling the cron job.
CREATE TABLE IF NOT EXISTS retention_config (
    id             BOOLEAN PRIMARY KEY DEFAULT TRUE,
    retention_days INTEGER NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT retention_config_singleton CHECK (id),
    CONSTRAINT retention_config_days_positive CHECK (retention_days > 0)
);

-- Seed a default so the job is safe to run before the app has ever booted.
INSERT INTO retention_config (id, retention_days)
VALUES (TRUE, 30)
ON CONFLICT (id) DO NOTHING;

-- Enforces the retention window and returns a human-readable summary of what
-- it did. Reads retention_days from retention_config on every run.
--
-- Two strategies, by design:
--   * a partition whose upper bound is at or before the cutoff is entirely
--     expired -> DROP TABLE, which is near-instant and takes no long lock on
--     the parent;
--   * the single partition straddling the cutoff keeps some live rows, so it
--     gets a plain DELETE of just the expired ones. That is bounded to one
--     week of data, not the whole table.
--
-- Partitions newer than the cutoff, and the DEFAULT partition, are never
-- touched. DEFAULT is excluded explicitly: it has no upper bound, so it can
-- hold rows of any age (including future ones) and dropping it would lose
-- data the retention window is supposed to keep.
CREATE OR REPLACE FUNCTION enforce_log_retention()
RETURNS TEXT AS $$
DECLARE
    days           INTEGER;
    cutoff         TIMESTAMPTZ;
    part           RECORD;
    dropped        TEXT[] := '{}';
    deleted_rows   BIGINT := 0;
    trimmed        TEXT   := NULL;
    affected       BIGINT;
BEGIN
    SELECT retention_days INTO days FROM retention_config WHERE id;
    IF days IS NULL THEN
        RETURN 'retention_config is empty; nothing to do';
    END IF;

    cutoff := now() - make_interval(days => days);

    FOR part IN
        SELECT c.oid,
               c.relname,
               -- Range bounds are read from the catalog rather than parsed
               -- out of the partition name, so an oddly-named partition can
               -- never be misjudged. split_part avoids regex-escaping noise.
               btrim(
                   split_part(pg_get_expr(c.relpartbound, c.oid), 'TO (', 2),
                   '''() '
               )::TIMESTAMPTZ AS upper_bound
        FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = 'logs'::regclass
          AND c.relkind = 'r'
          -- never the DEFAULT partition
          AND pg_get_expr(c.relpartbound, c.oid) <> 'DEFAULT'
        ORDER BY c.relname
    LOOP
        IF part.upper_bound IS NULL THEN
            CONTINUE;
        END IF;

        IF part.upper_bound <= cutoff THEN
            -- Fully expired: every row in it predates the cutoff.
            EXECUTE format('DROP TABLE %I', part.relname);
            dropped := dropped || part.relname;
        ELSE
            -- Straddles the cutoff if its lower bound is older. Deleting
            -- directly from the partition keeps the scan off sibling
            -- partitions entirely.
            EXECUTE format(
                'DELETE FROM %I WHERE "timestamp" < $1', part.relname
            ) USING cutoff;
            GET DIAGNOSTICS affected = ROW_COUNT;
            IF affected > 0 THEN
                deleted_rows := deleted_rows + affected;
                trimmed := part.relname;
            END IF;
        END IF;
    END LOOP;

    RETURN format(
        'retention: cutoff=%s days=%s dropped=[%s] deleted_rows=%s trimmed=%s',
        cutoff, days, array_to_string(dropped, ','), deleted_rows,
        COALESCE(trimmed, 'none')
    );
END;
$$ LANGUAGE plpgsql;

-- Schedule daily at 03:15 UTC. Unscheduling first keeps this idempotent: the
-- migration only runs once, but re-running it must not create a duplicate job.
SELECT cron.unschedule('log-retention')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'log-retention');

SELECT cron.schedule('log-retention', '15 3 * * *', 'SELECT enforce_log_retention()');
