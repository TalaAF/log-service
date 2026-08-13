-- 0004_retention_default_partition.sql
-- Applied by the runner in src/db/migrate.ts (not by drizzle-kit).
-- Must be idempotent: it is re-executed on every container start.
--
-- Closes a gap in retention.
--
-- 0002 deliberately never touches the DEFAULT partition, on the reasoning that
-- it has no upper bound and so could hold rows of any age, including future
-- ones. That is the right call for DROP, but it also meant expired rows sitting
-- in DEFAULT were never removed by anything — and rows land there routinely:
-- 0001 only provisions the current week and four ahead, so any backfill of
-- historical data, or any row older than the oldest provisioned week, is routed
-- to DEFAULT and then kept forever.
--
-- The partition itself still must not be dropped. Instead it gets the same
-- bounded DELETE the straddling partition already gets, which is what retention
-- means for a partition that cannot be dropped wholesale.
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
    default_rows   BIGINT := 0;
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

    -- The DEFAULT partition is never dropped — it has no upper bound, so it can
    -- legitimately hold rows newer than the cutoff — but its expired rows are
    -- deleted like any others. Bounded by the retention window, so it does not
    -- take a long lock.
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = 'logs'::regclass
          AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
    ) THEN
        DELETE FROM logs_default WHERE "timestamp" < cutoff;
        GET DIAGNOSTICS default_rows = ROW_COUNT;
        deleted_rows := deleted_rows + default_rows;
    END IF;

    RETURN format(
        'retention: cutoff=%s days=%s dropped=[%s] deleted_rows=%s trimmed=%s default_rows=%s',
        cutoff, days, array_to_string(dropped, ','), deleted_rows,
        COALESCE(trimmed, 'none'), default_rows
    );
END;
$$ LANGUAGE plpgsql;

-- Provision backwards as well as forwards. Rows older than the oldest existing
-- partition would otherwise pile up in DEFAULT, which cannot be dropped and so
-- has to be cleaned row by row. Covering the whole retention window with real
-- weekly partitions keeps the cheap DROP path available for almost all expiry.
CREATE OR REPLACE FUNCTION ensure_logs_partition_range(
    weeks_back    INTEGER,
    weeks_forward INTEGER
)
RETURNS TEXT AS $$
DECLARE
    created TEXT[] := '{}';
    week    INTEGER;
BEGIN
    FOR week IN -weeks_back..weeks_forward LOOP
        created := created || ensure_logs_partition(now() + make_interval(weeks => week));
    END LOOP;
    RETURN array_to_string(created, ',');
END;
$$ LANGUAGE plpgsql;

-- Hourly top-up now covers the retention window behind us as well as the weeks
-- ahead, derived from retention_config so it tracks RETENTION_DAYS.
SELECT cron.unschedule('log-partitions')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'log-partitions');

SELECT cron.schedule(
    'log-partitions',
    '7 * * * *',
    $$SELECT ensure_logs_partition_range(
        (SELECT ceil(retention_days / 7.0)::INTEGER FROM retention_config WHERE id), 4)$$
);
