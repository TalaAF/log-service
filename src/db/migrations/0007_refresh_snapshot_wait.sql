-- 0007_refresh_snapshot_wait.sql
-- Upgrade existing databases to the transaction-settling logic introduced in
-- 0006. Migrations are applied once by filename, so changing 0006 alone only
-- affects a fresh volume.

CREATE OR REPLACE FUNCTION refresh_log_rollups()
RETURNS TEXT AS $$
DECLARE
    cfg             rollup_config%ROWTYPE;
    h_old           BIGINT;
    h_new           BIGINT;
    seq_last        BIGINT;
    candidate       BIGINT;
    taken_at        TIMESTAMPTZ;
    settle_snapshot PG_SNAPSHOT;
    in_flight_xids  XID8[];
    settled         BOOLEAN := FALSE;
    caught_up       BOOLEAN;
    folded          BIGINT := 0;
    started         TIMESTAMPTZ := clock_timestamp();
    attempt         INTEGER;
    bucket_width    INTERVAL;
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

    -- A sequence allocates ids before their transaction commits. Capture the
    -- exact XIDs active when seq_last was read and wait for those transactions
    -- to finish. Re-reading pg_current_snapshot() here cannot show progress:
    -- this function is one top-level statement and keeps its statement
    -- snapshot. Newer transactions only allocate ids above seq_last and do not
    -- need to delay this fold.
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
