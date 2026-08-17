-- 0006_rollup_granularity.sql
-- Applied by the runner in src/db/migrate.ts (not by drizzle-kit).
-- Must be idempotent: it is re-executed on every container start.
--
-- Shrinks the raw hot tail by storing rollups at a finer granularity.
--
-- 0005 stored one row per minute, on the reasoning that a minute is the
-- smallest bucket the API exposes. That is true and it is why the rollup can
-- answer every supported bucket width, but it also set a floor under how close
-- the rollup/raw boundary could get to now(): the boundary has to land on a
-- stored bucket edge, so up to a whole minute of the newest data was always
-- read from the raw table on top of the configured lag.
--
-- At 15,000 logs/s that was tolerable. Under the ramp it was not. Measured at
-- 15k -> 22.5k -> 30k -> 45k logs/s with aggregates running concurrently:
--
--     offered 15,000/s   achieved 15,001/s
--     offered 22,500/s   achieved 22,503/s
--     offered 30,000/s   achieved  6,870/s     <- collapse
--     aggregate p95 16.0 s
--
-- The hot tail is bounded in time but not in rows, so its cost scales with the
-- ingest rate: a 20-80 second tail is 600k-2.4M rows at 30k/s. Aggregates then
-- take long enough to hold Postgres's single core, ingestion starves, and the
-- feedback loop the rollup was built to break reopens at a higher rate.
--
-- Ten seconds divides evenly into every supported bucket width (60, 300, 3600,
-- 86400), so derived buckets stay exact sums of stored rows, and it cuts the
-- alignment term from <=60s to <=10s. The table grows six-fold and remains
-- negligible: tens of rows per minute against hundreds of thousands of raw ones.

ALTER TABLE rollup_config
    ADD COLUMN IF NOT EXISTS bucket_seconds INTEGER NOT NULL DEFAULT 60;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'rollup_config_bucket_valid'
    ) THEN
        -- Every API bucket width must be a whole multiple of the stored width,
        -- otherwise a derived bucket would be a partial sum. 1, 2, 5, 10, 15,
        -- 20, 30 and 60 all divide 60 exactly.
        ALTER TABLE rollup_config
            ADD CONSTRAINT rollup_config_bucket_valid
            CHECK (bucket_seconds > 0 AND 60 % bucket_seconds = 0);
    END IF;
END $$;

-- Rows stored at one granularity cannot be summed together with rows stored at
-- another, so a change of granularity means discarding what is there and
-- refolding from the raw table. Resetting the watermark is what triggers that:
-- the refresh walks forward from id 1 again, in max_fold_rows chunks, and
-- safe_before stays frozen until it has caught up, so aggregates read raw rows
-- in the meantime -- slower, never wrong.
DO $$
DECLARE
    current_seconds INTEGER;
    target_seconds  CONSTANT INTEGER := 10;
BEGIN
    SELECT bucket_seconds INTO current_seconds FROM rollup_config WHERE id;

    IF current_seconds IS DISTINCT FROM target_seconds THEN
        UPDATE rollup_config SET bucket_seconds = target_seconds WHERE id;
        TRUNCATE log_rollups;
        UPDATE rollup_state
        SET watermark_id = 1,
            safe_before  = NULL,
            behind       = TRUE
        WHERE id;
    END IF;
END $$;

-- Redefined from 0005 to bin at the configured width rather than a hard-coded
-- minute. Everything else about the fold is unchanged: it consumes disjoint,
-- monotonically increasing id ranges, so counts accumulate safely, and the
-- count update and the watermark advance still commit together.
CREATE OR REPLACE FUNCTION refresh_log_rollups()
RETURNS TEXT AS $$
DECLARE
    cfg          rollup_config%ROWTYPE;
    h_old        BIGINT;
    h_new        BIGINT;
    seq_last     BIGINT;
    candidate    BIGINT;
    taken_at     TIMESTAMPTZ;
    settle_snapshot PG_SNAPSHOT;
    in_flight_xids XID8[];
    settled      BOOLEAN := FALSE;
    caught_up    BOOLEAN;
    folded       BIGINT := 0;
    started      TIMESTAMPTZ := clock_timestamp();
    attempt      INTEGER;
    bucket_width INTERVAL;
BEGIN
    IF NOT pg_try_advisory_xact_lock(7761421) THEN
        UPDATE rollup_state SET skipped = skipped + 1 WHERE id;
        RETURN 'skipped: another refresh holds the lock';
    END IF;

    SELECT * INTO cfg FROM rollup_config WHERE id;
    SELECT watermark_id INTO h_old FROM rollup_state WHERE id;

    bucket_width := make_interval(secs => cfg.bucket_seconds);

    seq_last := pg_sequence_last_value('logs_id_seq'::regclass);
    taken_at := clock_timestamp();

    IF seq_last IS NULL THEN
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
    -- so seq_last can sit above ids belonging to a COPY still in flight.
    -- Capture the exact transactions that were in flight when seq_last was
    -- read, then wait for those XIDs to finish. Re-reading
    -- pg_current_snapshot() inside this function does not work: the function is
    -- one top-level statement, so its transaction snapshot does not advance and
    -- a run that initially sees a COPY can only time out and skip. Transactions
    -- that start after this snapshot can only allocate ids above seq_last and
    -- therefore do not need to delay this fold.
    settle_snapshot := pg_current_snapshot();
    SELECT COALESCE(array_agg(active_xid), ARRAY[]::XID8[])
    INTO in_flight_xids
    FROM pg_snapshot_xip(settle_snapshot) AS active(active_xid);

    FOR attempt IN 1..500 LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM unnest(in_flight_xids) AS active(active_xid)
            WHERE pg_xact_status(active_xid) = 'in progress'
        ) THEN
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
        PERFORM summarize_logs_brin();

        WITH folded_rows AS MATERIALIZED (
            SELECT date_bin(bucket_width, "timestamp", TIMESTAMPTZ 'epoch') AS bucket_start,
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
            DO UPDATE SET entry_count = r.entry_count + EXCLUDED.entry_count
            RETURNING 1
        )
        SELECT COALESCE(sum(n), 0) INTO folded FROM folded_rows;
    END IF;

    UPDATE rollup_state
    SET watermark_id     = h_new,
        -- Aligned down to a stored bucket edge, which is now ten seconds rather
        -- than a minute. That alignment is the whole point of this migration:
        -- it is what decides how much of the newest data an aggregate still has
        -- to read from the raw table.
        safe_before      = CASE
                             WHEN caught_up
                               THEN GREATEST(
                                      safe_before,
                                      date_bin(
                                        bucket_width,
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

-- Retention's straddling-bucket recompute has to use the same width, otherwise
-- it would replace a ten-second bucket's count with a minute's worth of rows.
CREATE OR REPLACE FUNCTION enforce_rollup_retention()
RETURNS TEXT AS $$
DECLARE
    days         INTEGER;
    width        INTERVAL;
    cutoff       TIMESTAMPTZ;
    edge         TIMESTAMPTZ;
    removed      BIGINT;
BEGIN
    SELECT retention_days, make_interval(secs => bucket_seconds)
    INTO days, width
    FROM rollup_config WHERE id;

    IF days IS NULL THEN
        RETURN 'rollup_config is empty; nothing to do';
    END IF;

    PERFORM pg_advisory_xact_lock(7761421);

    cutoff := now() - make_interval(days => days);
    edge   := date_bin(width, cutoff, TIMESTAMPTZ 'epoch');

    DELETE FROM log_rollups WHERE bucket_start < edge;
    GET DIAGNOSTICS removed = ROW_COUNT;

    DELETE FROM log_rollups WHERE bucket_start = edge;

    INSERT INTO log_rollups (bucket_start, service, level, entry_count)
    SELECT date_bin(width, "timestamp", TIMESTAMPTZ 'epoch'), service, level, count(*)
    FROM logs
    WHERE "timestamp" >= edge AND "timestamp" < edge + width
    GROUP BY 1, 2, 3
    ON CONFLICT (bucket_start, service, level)
    DO UPDATE SET entry_count = EXCLUDED.entry_count;

    RETURN format('rollup retention: cutoff=%s days=%s deleted=%s', cutoff, days, removed);
END;
$$ LANGUAGE plpgsql;

-- Every 5 seconds rather than 10. The interval is the other half of how close
-- the boundary can get to now(): the watermark can only advance when a run
-- executes, so halving the interval halves that term too. Each run still reads
-- only the rows added since the previous one, so this is the same total work
-- split into smaller transactions -- measured at 168 ms for 150k rows, well
-- under 4% of the core at 30k logs/s.
SELECT cron.unschedule('log-rollup-refresh')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'log-rollup-refresh');

SELECT cron.schedule('log-rollup-refresh', '5 seconds', 'SELECT refresh_log_rollups()');
