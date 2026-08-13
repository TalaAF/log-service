-- 0003_write_path.sql
-- Applied by the runner in src/db/migrate.ts (not by drizzle-kit).
-- Must be idempotent: it is re-executed on every container start.
--
-- Trims the ingest path down to the indexes queries actually use.
--
-- Measured on a partition holding ~900k rows, 0001 left five indexes totalling
-- 102 MB against a 152 MB heap, and every INSERT had to maintain all five:
--
--     logs_..._timestamp_id_idx   48 MB   <- keyset pagination + range scans
--     logs_..._pkey               27 MB   <- nothing reads it
--     logs_..._attributes_idx     11 MB   <- attr filters
--     logs_..._level_idx        8104 kB   <- never usable
--     logs_..._service_idx      7504 kB   <- never usable
--
-- level holds four distinct values and service a handful, across millions of
-- rows, so a filter on either matches a large fraction of the table and the
-- planner correctly ignores the index in favour of a scan. They cost write
-- throughput on every row and return nothing at read time: every query in this
-- API is time-bounded, so the composite timestamp index plus a filter already
-- serves (timestamp, service) and (timestamp, level).
DROP INDEX IF EXISTS idx_logs_service;
DROP INDEX IF EXISTS idx_logs_level;

-- The primary key is only a uniqueness assertion. Nothing queries by id:
-- pagination is keyed on (timestamp DESC, id DESC), which the composite index
-- covers, and id comes from a sequence, so uniqueness holds without an index to
-- enforce it. Dropping it removes a second full btree insert per row.
ALTER TABLE logs DROP CONSTRAINT IF EXISTS logs_pkey;

-- GIN's pending list is what makes bulk insertion into a GIN index affordable:
-- new entries are appended to an unsorted list and merged into the tree in bulk
-- later, rather than every row paying its own tree descent. fastupdate is on by
-- default but is stated explicitly so it survives a rebuild, and raising the
-- pending list from the 4 MB default spends memory this container has spare to
-- get far fewer merges under sustained ingest.
--
-- The setting has to be applied per partition. A partitioned index has no
-- storage of its own, so ALTER INDEX on the parent is accepted but stores
-- nothing and new partitions inherit nothing from it — which is why
-- ensure_logs_partition() below applies it at creation time, and the backfill
-- afterwards covers partitions that already exist.
CREATE OR REPLACE FUNCTION apply_logs_index_options(part_name TEXT)
RETURNS VOID AS $$
DECLARE
    child_index TEXT;
BEGIN
    FOR child_index IN
        SELECT i.indexrelid::regclass::TEXT
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_am am ON am.oid = c.relam
        WHERE i.indrelid = part_name::regclass
          AND am.amname = 'gin'
    LOOP
        EXECUTE format(
            'ALTER INDEX %s SET (fastupdate = on, gin_pending_list_limit = 16384)',
            child_index
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Redefined from 0001 to apply the GIN storage options to the index Postgres
-- clones onto each new partition. Otherwise the tuning only ever reaches the
-- partitions that happened to exist when this migration ran.
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
        PERFORM apply_logs_index_options(part_name);
    END IF;

    RETURN part_name;
END;
$$ LANGUAGE plpgsql;

-- Backfill: partitions that already existed before the function was redefined.
DO $$
DECLARE
    part TEXT;
BEGIN
    FOR part IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = 'logs'::regclass AND c.relkind = 'r'
    LOOP
        PERFORM apply_logs_index_options(part);
    END LOOP;
END $$;

-- Query profiling. Preloaded via shared_preload_libraries in docker-compose.yml;
-- this makes the views available so the write and read paths can be measured
-- rather than guessed at.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Partition provisioning previously happened only at application boot, so a
-- deployment left running past its provisioned horizon would start routing rows
-- into the DEFAULT partition. pg_cron already owns retention, so it owns this
-- too: hourly, top up the current week plus four ahead.
SELECT cron.unschedule('log-partitions')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'log-partitions');

SELECT cron.schedule(
    'log-partitions',
    '7 * * * *',
    $$SELECT ensure_logs_partition(now() + (n || ' weeks')::interval) FROM generate_series(0, 4) AS n$$
);
