-- 0005_rollups.sql
-- Applied by the runner in src/db/migrate.ts (not by drizzle-kit).
-- Must be idempotent: it is re-executed on every container start.
--
-- Pre-aggregated minute rollups, so the common aggregate query stops scanning
-- the raw table.
--
-- The problem this addresses is a feedback loop, not a slow query. GET
-- /logs/aggregate over the active window reads every matching raw row, so its
-- cost grows with the dataset while the CPU budget does not. Measured on this
-- container at 4 aggregates/sec against ~2M rows, ingestion did not merely
-- slow down, it stopped: 15,000 logs/s fell to 315, aggregate p95 reached
-- 7.1 s and GET /logs p95 4.1 s, because each aggregate held Postgres's single
-- CPU long enough that the closed-loop clients had no capacity left to POST
-- with.
--
-- Rollups break the loop by making the common aggregate cost proportional to
-- elapsed time rather than to stored row count.

-- ---------------------------------------------------------------- the rollup
--
-- One row per (minute, service, level). 1 minute is the smallest bucket the API
-- offers, so every other supported width (5m, 1h, 1d) is a whole multiple of it
-- and is derived with SUM() over these rows. Storing the same aggregate at four
-- granularities would quadruple the write cost to save an addition.
--
-- Volume is services x levels rows per minute -- tens of rows a minute against
-- hundreds of thousands of raw rows a minute -- so this table is optimised for
-- reading, not for insertion.
CREATE TABLE IF NOT EXISTS log_rollups (
    bucket_start TIMESTAMPTZ  NOT NULL,
    service      VARCHAR(255) NOT NULL,
    level        VARCHAR(10)  NOT NULL,
    entry_count  BIGINT       NOT NULL
);

-- The uniqueness constraint and the read index are deliberately one index.
-- ON CONFLICT needs a unique index on (bucket_start, service, level), and the
-- read path wants entry_count without a heap fetch. INCLUDE gives both from a
-- single btree, so the covering payload adds no index to maintain -- which is
-- what distinguishes it from the standalone covering index that was tested and
-- rejected on the raw table, where it would have been a fifth index on the
-- hottest write path in the system.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'log_rollups_pkey') THEN
        CREATE UNIQUE INDEX log_rollups_pkey
            ON log_rollups (bucket_start, service, level) INCLUDE (entry_count);
        ALTER TABLE log_rollups
            ADD CONSTRAINT log_rollups_pkey PRIMARY KEY USING INDEX log_rollups_pkey;
    END IF;
END $$;

-- ------------------------------------------------------------------- config
CREATE TABLE IF NOT EXISTS rollup_config (
    id              BOOLEAN PRIMARY KEY DEFAULT TRUE,
    -- How far behind now() the rollup/raw boundary is allowed to sit. This now
    -- covers only clock skew between the client and the database: the delay
    -- between stamping a log and committing it is handled exactly, and without
    -- guesswork, by the ingest floor in the application. See safe_before below.
    lag_seconds     INTEGER NOT NULL DEFAULT 10,
    -- Largest number of ids one refresh will fold. Bounds the cost of a single
    -- run when the watermark is a long way behind -- first deployment onto a
    -- populated database, or a restart after downtime -- so the backlog is
    -- worked off over successive runs instead of in one long transaction that
    -- monopolises the CPU.
    max_fold_rows   BIGINT  NOT NULL DEFAULT 1000000,
    -- Rollup rows are a tiny fraction of the size of the raw rows behind them,
    -- so they could be kept far longer. Defaulting to the raw window keeps
    -- aggregate results identical to what the raw implementation would return:
    -- a longer window would let aggregates outlive the logs they describe,
    -- which is a change in meaning and so has to be opted into explicitly.
    retention_days  INTEGER NOT NULL DEFAULT 30,
    CONSTRAINT rollup_config_singleton CHECK (id),
    CONSTRAINT rollup_config_lag_nonneg CHECK (lag_seconds >= 0),
    CONSTRAINT rollup_config_fold_positive CHECK (max_fold_rows > 0),
    CONSTRAINT rollup_config_retention_positive CHECK (retention_days > 0)
);

INSERT INTO rollup_config (id) VALUES (TRUE) ON CONFLICT (id) DO NOTHING;

-- -------------------------------------------------------------------- state
--
-- watermark_id is the contract between the refresh job and the query path:
--
--     every row with id < watermark_id is already counted in log_rollups
--
-- and safe_before is that invariant restated in terms the query path can use:
--
--     every row with "timestamp" < safe_before has id < watermark_id.
--
-- Together they are what make it sound to answer [.., safe_before) from the
-- rollup and [safe_before, ..) from the raw table without double counting a row
-- or missing one.
CREATE TABLE IF NOT EXISTS rollup_state (
    id               BOOLEAN PRIMARY KEY DEFAULT TRUE,
    watermark_id     BIGINT      NOT NULL DEFAULT 1,
    safe_before      TIMESTAMPTZ,
    last_refresh_at  TIMESTAMPTZ,
    last_rows        BIGINT           NOT NULL DEFAULT 0,
    last_duration_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    refreshes        BIGINT           NOT NULL DEFAULT 0,
    skipped          BIGINT           NOT NULL DEFAULT 0,
    -- True when the last run hit max_fold_rows, i.e. the rollup is still
    -- catching up. safe_before is frozen while it is set.
    behind           BOOLEAN          NOT NULL DEFAULT FALSE,
    CONSTRAINT rollup_state_singleton CHECK (id)
);

INSERT INTO rollup_state (id) VALUES (TRUE) ON CONFLICT (id) DO NOTHING;

-- --------------------------------------------------------- id range access
--
-- The refresh folds an id range, not a time range, and that choice is what
-- makes it exact for out-of-order logs. A log backdated by an hour still gets a
-- fresh id, so it is folded into its true minute bucket on the next run. A time
-- window would instead have to guess how late a log might arrive and would
-- silently lose anything later than the guess.
--
-- BRIN rather than btree for the supporting index. id comes from a sequence and
-- the table is append-only, so id is almost perfectly correlated with physical
-- order -- the case BRIN exists for. The index is a handful of pages per
-- partition, against the ~27 MB per 900k rows that the btree primary key cost
-- before 0003_write_path.sql dropped it, so the per-row write cost is close to
-- unmeasurable while a range lookup still skips every page range that cannot
-- hold the ids being folded.
--
-- autosummarize keeps new page ranges summarised as they fill rather than
-- waiting for a vacuum. refresh_log_rollups() also summarises explicitly, so a
-- freshly created benchmark database does not spend its first refreshes reading
-- pages BRIN has not learned about yet.
CREATE INDEX IF NOT EXISTS idx_logs_id_brin ON logs USING brin (id);

-- Applies per-partition index storage options. Extends the 0003 version, which
-- covered only the GIN pending list, with the BRIN autosummarize setting. Both
-- have to be set on the leaf index: a partitioned index has no storage of its
-- own, and new partitions inherit nothing from it.
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

    FOR child_index IN
        SELECT i.indexrelid::regclass::TEXT
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_am am ON am.oid = c.relam
        WHERE i.indrelid = part_name::regclass
          AND am.amname = 'brin'
    LOOP
        EXECUTE format('ALTER INDEX %s SET (autosummarize = on)', child_index);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Backfill for partitions that already exist.
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

-- Summarises the page ranges BRIN has not indexed yet, across every partition.
-- Only unsummarised ranges are read, so the cost is proportional to the rows
-- added since the previous call rather than to the size of the table.
CREATE OR REPLACE FUNCTION summarize_logs_brin()
RETURNS VOID AS $$
DECLARE
    child_index TEXT;
BEGIN
    FOR child_index IN
        SELECT i.indexrelid::regclass::TEXT
        FROM pg_index i
        JOIN pg_class c    ON c.oid = i.indexrelid
        JOIN pg_am am      ON am.oid = c.relam
        JOIN pg_inherits h ON h.inhrelid = i.indrelid
        WHERE h.inhparent = 'logs'::regclass
          AND am.amname = 'brin'
    LOOP
        PERFORM brin_summarize_new_values(child_index::regclass);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ------------------------------------------------------------------ refresh
--
-- Folds every raw row that has appeared since the last run into log_rollups and
-- advances the watermark. Idempotent by construction: it consumes disjoint,
-- monotonically increasing id ranges, and the count update and the watermark
-- advance commit together, so a run that fails or is retried cannot double
-- count and a run that executes twice has nothing to do the second time.
CREATE OR REPLACE FUNCTION refresh_log_rollups()
RETURNS TEXT AS $$
DECLARE
    cfg          rollup_config%ROWTYPE;
    h_old        BIGINT;
    h_new        BIGINT;
    seq_last     BIGINT;
    candidate    BIGINT;
    taken_at     TIMESTAMPTZ;
    settle_xmax  XID8;
    settled      BOOLEAN := FALSE;
    caught_up    BOOLEAN;
    folded       BIGINT := 0;
    started      TIMESTAMPTZ := clock_timestamp();
    attempt      INTEGER;
BEGIN
    -- One refresh at a time. pg_cron fires on a fixed schedule and will happily
    -- start a second run while the first is still going; on a single CPU,
    -- overlapping runs contending for the same rollup rows make the backlog
    -- worse rather than better. A transaction-scoped lock is released
    -- automatically however the run ends.
    IF NOT pg_try_advisory_xact_lock(7761421) THEN
        UPDATE rollup_state SET skipped = skipped + 1 WHERE id;
        RETURN 'skipped: another refresh holds the lock';
    END IF;

    SELECT * INTO cfg FROM rollup_config WHERE id;
    SELECT watermark_id INTO h_old FROM rollup_state WHERE id;

    seq_last := pg_sequence_last_value('logs_id_seq'::regclass);
    taken_at := clock_timestamp();

    IF seq_last IS NULL THEN
        -- The sequence has never been drawn from: no rows have ever been
        -- written, so there is nothing to fold and nothing to be stale.
        UPDATE rollup_state
        SET last_refresh_at  = clock_timestamp(),
            last_rows        = 0,
            last_duration_ms = 0,
            refreshes        = refreshes + 1,
            behind           = FALSE
        WHERE id;
        RETURN 'nothing ingested yet';
    END IF;

    -- A sequence hands out ids before the transaction that took them commits,
    -- so seq_last can sit above ids belonging to a COPY that is still in
    -- flight. Folding straight to it would step over those rows and, because
    -- the watermark only moves forwards, lose them permanently.
    --
    -- Waiting until the current snapshot's xmin has passed the xmax recorded
    -- just now proves every transaction that could be holding such an id has
    -- since committed or aborted. Ingest transactions are single COPY
    -- statements lasting a few milliseconds, so this settles almost at once;
    -- read-only queries never take an xid and so cannot hold it up. If it does
    -- not settle inside the budget the run simply does not advance, which
    -- leaves the previous watermark -- and the previous, more conservative
    -- boundary -- in force.
    settle_xmax := pg_snapshot_xmax(pg_current_snapshot());
    FOR attempt IN 1..50 LOOP
        IF pg_snapshot_xmin(pg_current_snapshot()) >= settle_xmax THEN
            settled := TRUE;
            EXIT;
        END IF;
        PERFORM pg_sleep(0.01);
    END LOOP;

    IF NOT settled THEN
        UPDATE rollup_state
        SET last_refresh_at = clock_timestamp(),
            skipped         = skipped + 1
        WHERE id;
        RETURN 'skipped: in-flight writes did not settle';
    END IF;

    candidate := seq_last + 1;
    h_new     := LEAST(candidate, h_old + cfg.max_fold_rows);
    caught_up := (h_new = candidate);

    IF h_new > h_old THEN
        -- Keep BRIN's summaries current before using them, so the range scan
        -- below can skip page ranges instead of falling back to reading every
        -- page the planner cannot rule out.
        PERFORM summarize_logs_brin();

        WITH folded_rows AS MATERIALIZED (
            SELECT date_bin('1 minute', "timestamp", TIMESTAMPTZ 'epoch') AS bucket_start,
                   service,
                   level,
                   count(*) AS n
            FROM logs
            WHERE id >= h_old AND id < h_new
            GROUP BY 1, 2, 3
        ), upserted AS (
            INSERT INTO log_rollups AS r (bucket_start, service, level, entry_count)
            SELECT bucket_start, service, level, n FROM folded_rows
            ON CONFLICT (bucket_start, service, level)
            -- Addition, not replacement. Safe here precisely because the id
            -- ranges consumed by successive runs are disjoint and never
            -- revisited, so no row can be added to a bucket twice -- the
            -- condition that makes incremental accumulation legitimate.
            DO UPDATE SET entry_count = r.entry_count + EXCLUDED.entry_count
            RETURNING 1
        )
        SELECT COALESCE(sum(n), 0) INTO folded FROM folded_rows;
    END IF;

    UPDATE rollup_state
    SET watermark_id     = h_new,
        -- safe_before only moves when the fold is fully caught up. While a
        -- backlog is being worked off, rows below the boundary may still be
        -- unfolded, so the boundary stays where it was and the query path keeps
        -- reading those minutes from the raw table -- slower, but never wrong.
        --
        -- taken_at is when the watermark's id was read. Every row now folded
        -- was inserted before then, so a row that is *not* folded was inserted
        -- at or after it, and can only carry a timestamp below the boundary if
        -- it was backdated by more than lag_seconds. Aligning down to a whole
        -- minute keeps the boundary on a rollup bucket edge, so no bucket is
        -- ever split between the two sources.
        safe_before      = CASE
                             WHEN caught_up
                               THEN GREATEST(
                                      safe_before,
                                      date_bin(
                                        '1 minute',
                                        taken_at - make_interval(secs => cfg.lag_seconds),
                                        TIMESTAMPTZ 'epoch'))
                             ELSE safe_before
                           END,
        last_refresh_at  = clock_timestamp(),
        last_rows        = folded,
        last_duration_ms = EXTRACT(EPOCH FROM clock_timestamp() - started) * 1000,
        refreshes        = refreshes + 1,
        behind           = NOT caught_up
    WHERE id;

    RETURN format('folded %s rows, watermark %s -> %s%s',
                  folded, h_old, h_new,
                  CASE WHEN caught_up THEN '' ELSE ' (catching up)' END);
END;
$$ LANGUAGE plpgsql;

-- Every 10 seconds. The interval sets how quickly the boundary can follow now()
-- and therefore how much raw data a hybrid aggregate has to read: at 10s the
-- hot tail stays between lag_seconds and lag_seconds + 60s regardless of how
-- large the table grows. Each run reads only the rows added since the previous
-- one, so a shorter interval does not mean more total work -- only more, and
-- smaller, transactions.
SELECT cron.unschedule('log-rollup-refresh')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'log-rollup-refresh');

SELECT cron.schedule('log-rollup-refresh', '10 seconds', 'SELECT refresh_log_rollups()');

-- ---------------------------------------------------------------- retention
--
-- Rollup retention rides along with raw retention rather than running as a
-- separate job, so the two can never drift apart and leave aggregates
-- describing logs that have been deleted.
CREATE OR REPLACE FUNCTION enforce_rollup_retention()
RETURNS TEXT AS $$
DECLARE
    days       INTEGER;
    cutoff     TIMESTAMPTZ;
    edge       TIMESTAMPTZ;
    removed    BIGINT;
BEGIN
    SELECT retention_days INTO days FROM rollup_config WHERE id;
    IF days IS NULL THEN
        RETURN 'rollup_config is empty; nothing to do';
    END IF;

    -- Blocks rather than gives up: a refresh run lasts milliseconds, and
    -- skipping retention for a whole day to avoid a short wait would be a poor
    -- trade. Holding it also stops a concurrent fold adding rows to the
    -- straddling bucket while it is being recomputed below.
    PERFORM pg_advisory_xact_lock(7761421);

    cutoff := now() - make_interval(days => days);
    edge   := date_bin('1 minute', cutoff, TIMESTAMPTZ 'epoch');

    -- Bounded by the primary key's leading column, so this is an index range
    -- delete over the expired prefix, not a scan of the table.
    DELETE FROM log_rollups WHERE bucket_start < edge;
    GET DIAGNOSTICS removed = ROW_COUNT;

    -- The minute straddling the cutoff is a special case: raw retention deletes
    -- the rows inside it that predate the cutoff, which would leave its rollup
    -- counting rows that no longer exist. It is one minute of data, so it is
    -- simply recomputed from what survives. Replacement, not addition -- this
    -- is a correction of a bucket, not the arrival of new rows.
    DELETE FROM log_rollups WHERE bucket_start = edge;

    INSERT INTO log_rollups (bucket_start, service, level, entry_count)
    SELECT date_bin('1 minute', "timestamp", TIMESTAMPTZ 'epoch'), service, level, count(*)
    FROM logs
    WHERE "timestamp" >= edge AND "timestamp" < edge + INTERVAL '1 minute'
    GROUP BY 1, 2, 3
    ON CONFLICT (bucket_start, service, level)
    DO UPDATE SET entry_count = EXCLUDED.entry_count;

    RETURN format('rollup retention: cutoff=%s days=%s deleted=%s', cutoff, days, removed);
END;
$$ LANGUAGE plpgsql;

-- Ten minutes after raw retention, so the rollup edge is recomputed against
-- rows that have already been deleted rather than ones about to be.
SELECT cron.unschedule('log-rollup-retention')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'log-rollup-retention');

SELECT cron.schedule(
    'log-rollup-retention',
    '25 3 * * *',
    $$SELECT enforce_rollup_retention()$$
);
