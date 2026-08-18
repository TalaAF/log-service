-- 0010_attr_sidecar.sql
-- Applied by the runner in src/db/migrate.ts (not by drizzle-kit).
-- Must be idempotent: it is re-executed on every container start.
--
-- Moves attribute-index maintenance off the ingest critical path.
--
-- The GIN index on logs.attributes is what makes an arbitrary
-- `attr.<key>=<value>` filter answerable without reading the whole range, and
-- the API contract requires that filter to work for any key. It is also the
-- single most expensive thing COPY does: measured on this container, removing
-- it took the mixed load scenario from ~14.4k to ~14.94k logs/s and cut WAL by
-- more than half. Both facts are true at once, so the index cannot simply be
-- dropped and cannot simply be kept.
--
-- What is actually required is that the *query* be answerable, not that the
-- index be maintained inside the writing transaction. So the entry point moves
-- to a derived sidecar:
--
--     logs              durable source of truth, no attribute index
--     log_attr_tokens   (id, timestamp, hashed key/value tokens) + GIN
--     attr_index_state  watermark W: every id < W is present in the sidecar
--
-- and the query path owns the two id ranges separately:
--
--     id <  W   answered through the sidecar, then rechecked against the
--               original JSONB, because a 64-bit token is a prefilter and
--               nothing more
--     id >= W   answered by reading logs directly with the exact predicate
--
-- The ranges are disjoint and cover everything, so no row is counted twice or
-- missed, whatever the indexer is doing. A row is queryable the instant it is
-- committed — the fallback covers it — which is why the indexer is allowed to
-- be slow, to fall behind, and to skip work entirely when the database has
-- something better to do. That is the whole point: index lag is not query lag.

-- ------------------------------------------------------------------- tokens
--
-- One token per (key, value) pair, hashed rather than stored as text. Hashing
-- makes every token the same fixed width whatever the attribute holds, which
-- keeps the GIN entries small and uniform; a canonical-text representation was
-- measured against this one and was worse on both index size and write cost.
--
-- The value side is exactly `attributes ->> key`, so a token matches precisely
-- when the API's `->>`-based predicate does. Seeding the value's hash with the
-- key's hash rather than concatenating the two makes the pair unambiguous
-- without inventing a separator that an attribute key could itself contain.
--
-- Collisions are permitted by construction. Every read path re-applies
-- `attributes ->> key = value` against the original row, so a collision costs
-- one wasted heap fetch and can never produce a wrong answer.
CREATE OR REPLACE FUNCTION attr_token(attr_key TEXT, attr_value TEXT)
RETURNS BIGINT
LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS $$
    SELECT hashtextextended(attr_value, hashtextextended(attr_key, 0))
$$;

-- JSON null renders as SQL NULL through `->>`, which never equals a filter
-- value, so those pairs are left out of the token set rather than given a
-- token that could never be asked for.
CREATE OR REPLACE FUNCTION attr_tokens(attrs JSONB)
RETURNS BIGINT[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS $$
    SELECT COALESCE(array_agg(attr_token(e.key, e.value)), ARRAY[]::BIGINT[])
    FROM jsonb_each_text(attrs) AS e(key, value)
    WHERE e.value IS NOT NULL
$$;

-- ------------------------------------------------------------------ sidecar
--
-- Not partitioned, and deliberately so. Its rows are reached either through the
-- GIN index or through the (timestamp, id) btree, never by scanning, and
-- retention removes them with a bounded range delete driven by the same cutoff
-- the raw table uses. Partitioning would buy a cheaper DROP at the cost of
-- routing every background insert through the partition machinery.
--
-- timestamp is carried rather than looked up so that a time-bounded attribute
-- query can be answered without touching logs for rows it is going to discard,
-- and so the sidecar can be ordered the same way the API pages: newest first.
CREATE TABLE IF NOT EXISTS log_attr_tokens (
    id          BIGINT      NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    tokens      BIGINT[]    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_log_attr_tokens_gin
    ON log_attr_tokens USING GIN (tokens);

-- Lets a frequent attribute value be answered by walking newest-first and
-- stopping at the page limit, instead of building a bitmap over every row that
-- has ever carried it. The planner chooses between the two from the array
-- element statistics, which is the behaviour the raw table used to get from
-- having both a timestamp index and a GIN.
CREATE INDEX IF NOT EXISTS idx_log_attr_tokens_ts_id
    ON log_attr_tokens ("timestamp" DESC, id DESC);

-- A small pending list on purpose. fastupdate keeps bulk insertion cheap, but
-- the merge is paid by whichever insert happens to overflow the list, and this
-- indexer's whole design is that no single step of it may be long. 512kB caps
-- that merge at a few tens of milliseconds instead of the several hundred a
-- 4MB list would cost.
DO $$
BEGIN
    EXECUTE 'ALTER INDEX idx_log_attr_tokens_gin SET (fastupdate = on, gin_pending_list_limit = 512)';
END $$;

-- ------------------------------------------------------------------- config
CREATE TABLE IF NOT EXISTS attr_index_config (
    id                BOOLEAN PRIMARY KEY DEFAULT TRUE,
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    -- Per-batch execution target. Batches are sized to land here, and shrink
    -- as soon as they exceed max_batch_ms, so one cycle can never turn into
    -- the multi-hundred-millisecond transaction that made the first version of
    -- this design unusable under stress.
    target_batch_ms   INTEGER NOT NULL DEFAULT 25,
    max_batch_ms      INTEGER NOT NULL DEFAULT 60,
    -- How much wall time one cycle may spend indexing while the database is
    -- otherwise quiet. When it is not quiet the cycle does one deliberately
    -- small batch and stops, so this is a catch-up allowance rather than a
    -- share of the server.
    budget_ms         INTEGER NOT NULL DEFAULT 300,
    min_batch_rows    INTEGER NOT NULL DEFAULT 250,
    max_batch_rows    INTEGER NOT NULL DEFAULT 25000,
    -- Backends other than this one that have to be executing before the
    -- database counts as busy. One, because on a single CPU any other backend
    -- executing is already something this job would be taking the core from.
    busy_backends     INTEGER NOT NULL DEFAULT 1,
    CONSTRAINT attr_index_config_singleton CHECK (id),
    CONSTRAINT attr_index_config_batch_ms CHECK (target_batch_ms > 0 AND max_batch_ms >= target_batch_ms),
    CONSTRAINT attr_index_config_budget CHECK (budget_ms >= 0),
    CONSTRAINT attr_index_config_rows CHECK (min_batch_rows > 0 AND max_batch_rows >= min_batch_rows)
);

INSERT INTO attr_index_config (id) VALUES (TRUE) ON CONFLICT (id) DO NOTHING;

-- -------------------------------------------------------------------- state
--
-- watermark_id is the contract between the indexer and the query path:
--
--     every row with id < watermark_id has its tokens in log_attr_tokens
--
-- It starts at 1, which is below every id the sequence hands out, so a database
-- that has never run the indexer answers every attribute query from the raw
-- fallback. That is the safe direction: a watermark that is too low costs
-- speed, one that is too high would lose rows.
CREATE TABLE IF NOT EXISTS attr_index_state (
    id             BOOLEAN PRIMARY KEY DEFAULT TRUE,
    watermark_id   BIGINT  NOT NULL DEFAULT 1,
    batch_rows     INTEGER NOT NULL DEFAULT 2000,
    rows_indexed   BIGINT  NOT NULL DEFAULT 0,
    batches        BIGINT  NOT NULL DEFAULT 0,
    cycles         BIGINT  NOT NULL DEFAULT 0,
    -- Cycles that decided not to run: deferred because a rollup refresh was in
    -- progress, or skipped because another indexer already held the lock.
    deferred       BIGINT  NOT NULL DEFAULT 0,
    contended       BIGINT  NOT NULL DEFAULT 0,
    last_batch_ms  DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_batch_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    peak_batch_ms  DOUBLE PRECISION NOT NULL DEFAULT 0,
    -- Fixed-edge histogram of batch execution times, in milliseconds:
    -- [<5, <10, <20, <40, <80, <160, <320, >=320]. Percentiles are derived
    -- from it on demand, which costs nothing at run time and avoids keeping a
    -- sample buffer in a row that is updated several times a second.
    ms_hist        BIGINT[] NOT NULL DEFAULT ARRAY[0,0,0,0,0,0,0,0]::BIGINT[],
    last_run_at    TIMESTAMPTZ,
    CONSTRAINT attr_index_state_singleton CHECK (id)
);

INSERT INTO attr_index_state (id) VALUES (TRUE) ON CONFLICT (id) DO NOTHING;

-- ------------------------------------------------------------------ indexer
--
-- One bounded batch, in the caller's transaction. Returns the number of rows
-- indexed, 0 when there is nothing to do, -1 when it declined to run, and -2
-- when it did work but overran its time ceiling. Every non-positive value ends
-- the cycle, so the caller does not have to tell them apart.
--
-- The upper bound is not the sequence's last value but the rollup's watermark.
-- A sequence hands out ids before the transaction holding them commits, so
-- indexing up to the sequence would step over rows still in flight and, because
-- the watermark only moves forwards, leave them permanently unindexed. The
-- rollup refresh already establishes that exact boundary — it waits for those
-- transactions to settle before publishing its watermark — so reusing it costs
-- nothing and inherits a property that has already been proven. If the refresh
-- stops, this stops advancing too, and every attribute query falls back to the
-- raw path: slower, never wrong.
--
-- Idempotent because the insert and the watermark advance commit together over
-- a half-open id range that is never revisited. A run that fails writes
-- nothing; a run that is repeated finds the watermark already moved.
CREATE OR REPLACE FUNCTION index_log_attrs_batch(row_cap INTEGER DEFAULT NULL)
RETURNS INTEGER AS $$
DECLARE
    cfg       attr_index_config%ROWTYPE;
    st        attr_index_state%ROWTYPE;
    w_old     BIGINT;
    w_new     BIGINT;
    w_limit   BIGINT;
    n         INTEGER := 0;
    started   TIMESTAMPTZ := clock_timestamp();
    took      DOUBLE PRECISION;
    next_rows INTEGER;
    slot      INTEGER;
BEGIN
    SELECT * INTO cfg FROM attr_index_config WHERE id;
    IF cfg IS NULL OR NOT cfg.enabled THEN
        RETURN -1;
    END IF;

    -- Exactly one indexing batch at a time, whatever else has been scheduled.
    -- Transaction scoped, so it is released by the commit that ends the batch
    -- rather than held across the whole cycle.
    IF NOT pg_try_advisory_xact_lock(7761422) THEN
        UPDATE attr_index_state SET contended = contended + 1 WHERE id;
        RETURN -1;
    END IF;

    SELECT * INTO st FROM attr_index_state WHERE id;
    SELECT watermark_id INTO w_limit FROM rollup_state WHERE id;

    -- LEAST() ignores NULL rather than propagating it, so a missing rollup
    -- state would silently remove the settled-id bound instead of stopping
    -- the indexer. Stop explicitly.
    IF w_limit IS NULL THEN
        RETURN 0;
    END IF;

    w_old := st.watermark_id;
    w_new := LEAST(w_old + LEAST(st.batch_rows, COALESCE(row_cap, st.batch_rows)), w_limit);

    IF w_new <= w_old THEN
        UPDATE attr_index_state SET last_run_at = clock_timestamp() WHERE id;
        RETURN 0;
    END IF;

    -- The BRIN index on logs(id) turns this into a read of the few page ranges
    -- that can hold the batch. It is kept summarised by the rollup refresh and
    -- by autosummarize, so nothing has to be summarised here.
    INSERT INTO log_attr_tokens (id, "timestamp", tokens)
    SELECT l.id, l."timestamp", attr_tokens(l.attributes)
    FROM logs l
    WHERE l.id >= w_old AND l.id < w_new;
    GET DIAGNOSTICS n = ROW_COUNT;

    took := EXTRACT(EPOCH FROM clock_timestamp() - started) * 1000;

    -- Shrink hard, grow gently. An overrun means the database is busier than
    -- the last measurement suggested and the next batch has to be cheaper now;
    -- an underrun only means there may be room, and finding out slowly costs
    -- nothing because the fallback is already answering every query.
    IF took > cfg.max_batch_ms * 3 THEN
        -- Far past the ceiling: the estimate this batch was sized from no
        -- longer describes the database, so go back to the floor and re-learn
        -- rather than halve repeatedly while each attempt is still too big.
        next_rows := cfg.min_batch_rows;
    ELSIF took > cfg.max_batch_ms THEN
        next_rows := GREATEST(cfg.min_batch_rows, (st.batch_rows * 3) / 5);
    ELSIF took < cfg.target_batch_ms THEN
        next_rows := LEAST(cfg.max_batch_rows, (st.batch_rows * 5) / 4 + 50);
    ELSE
        next_rows := st.batch_rows;
    END IF;

    slot := CASE
              WHEN took <   5 THEN 1
              WHEN took <  10 THEN 2
              WHEN took <  20 THEN 3
              WHEN took <  40 THEN 4
              WHEN took <  80 THEN 5
              WHEN took < 160 THEN 6
              WHEN took < 320 THEN 7
              ELSE 8
            END;

    UPDATE attr_index_state
    SET watermark_id   = w_new,
        batch_rows     = next_rows,
        rows_indexed   = rows_indexed + n,
        batches        = batches + 1,
        last_batch_ms  = took,
        total_batch_ms = total_batch_ms + took,
        peak_batch_ms  = GREATEST(peak_batch_ms, took),
        ms_hist        = ms_hist[1:slot-1] || ARRAY[ms_hist[slot] + 1] || ms_hist[slot+1:8],
        last_run_at    = clock_timestamp()
    WHERE id;

    -- -2 rather than the row count when the batch overran its ceiling. Every
    -- caller stops on a non-positive return, so an expensive batch ends the
    -- cycle it was in: the size controller alone cannot hold a time target when
    -- the same number of rows costs ten times more cold than cached, and one
    -- long batch a second is a far smaller disturbance than a run of them.
    IF took > cfg.max_batch_ms THEN
        RETURN -2;
    END IF;

    RETURN n;
END;
$$ LANGUAGE plpgsql;

-- Backends other than this one that are executing something right now.
--
-- Sampled before every batch rather than once per cycle. One probe a second
-- classifies a whole second from a single instant, and on a database whose
-- ingest duty cycle is well under 100% that instant is usually idle — which is
-- how a throttle meant to yield under pressure ended up spending a third of the
-- server's execution time during the stress ramp. Ten probes a second turn the
-- same threshold into a duty cycle that falls off on its own as the database
-- fills up.
CREATE OR REPLACE FUNCTION active_backends()
RETURNS INTEGER
LANGUAGE sql STABLE AS $$
    SELECT count(*)::INTEGER
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND state = 'active'
      AND pid <> pg_backend_pid()
$$;

-- True while a rollup refresh holds its advisory lock. Read from pg_locks
-- rather than taken, so testing it can never make the refresh skip a run: the
-- refresh acquires the lock with try_lock and treats failure as "somebody else
-- is folding", which a probe must not be able to trigger.
CREATE OR REPLACE FUNCTION refresh_in_progress()
RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_locks
        WHERE locktype = 'advisory' AND classid = 0 AND objid = 7761421 AND granted
    )
$$;

-- One cycle: a sequence of short, separately committed batches inside a wall
-- clock budget.
--
-- Transaction control is the reason this is a procedure. The alternative — one
-- large batch per scheduled run — is what the first version of this design did,
-- and it is precisely what broke it: a single fold big enough to keep up with
-- ingestion held the CPU long enough to push aggregate p95 past a second and
-- read-after-write into the tens of seconds. Many small committed batches do
-- the same total work while leaving a gap between every one of them.
--
-- A cycle ends early on any of four cheap tests: a rollup refresh is running,
-- and on a single CPU the two have no reason to fight when the fallback makes
-- this one's progress optional; another backend is executing, which on one core
-- means this job would be taking it from them; the batch overran its ceiling;
-- or the catch-up budget is spent. Finding the database busy at the start of a
-- cycle is not a reason to do nothing at all — one floor-sized batch still
-- runs — so a permanently loaded server still makes progress, slowly.
CREATE OR REPLACE PROCEDURE run_attr_indexer()
LANGUAGE plpgsql AS $$
DECLARE
    cfg      attr_index_config%ROWTYPE;
    deadline TIMESTAMPTZ;
    busy     INTEGER;
    n        INTEGER;
BEGIN
    SELECT * INTO cfg FROM attr_index_config WHERE id;
    IF cfg IS NULL OR NOT cfg.enabled THEN
        RETURN;
    END IF;

    -- Session scoped, so a cycle that runs long cannot be overlapped by the
    -- next scheduled one. Released when this backend disconnects even if the
    -- procedure raises.
    IF NOT pg_try_advisory_lock(7761423) THEN
        UPDATE attr_index_state SET contended = contended + 1 WHERE id;
        RETURN;
    END IF;

    IF refresh_in_progress() THEN
        UPDATE attr_index_state SET cycles = cycles + 1, deferred = deferred + 1 WHERE id;
        PERFORM pg_advisory_unlock(7761423);
        RETURN;
    END IF;

    UPDATE attr_index_state SET cycles = cycles + 1 WHERE id;
    COMMIT;

    busy := active_backends();

    IF busy >= cfg.busy_backends THEN
        -- Something else is executing. Do one batch at the floor size so the
        -- sidecar still creeps forward on a permanently busy server, and give
        -- the core straight back. The fallback is answering these ids exactly
        -- in the meantime, so there is nothing here worth queueing behind.
        SELECT index_log_attrs_batch(cfg.min_batch_rows) INTO n;
        COMMIT;
        PERFORM pg_advisory_unlock(7761423);
        RETURN;
    END IF;

    deadline := clock_timestamp() + make_interval(secs => cfg.budget_ms / 1000.0);

    LOOP
        SELECT index_log_attrs_batch() INTO n;
        COMMIT;
        -- Non-positive covers all three ways to stop: nothing left to index,
        -- another indexer holds the lock, or the batch overran its ceiling.
        EXIT WHEN n <= 0;
        EXIT WHEN clock_timestamp() >= deadline;
        EXIT WHEN refresh_in_progress();
        EXIT WHEN active_backends() >= cfg.busy_backends;
    END LOOP;

    PERFORM pg_advisory_unlock(7761423);
END;
$$;

-- Every second. The cycle itself decides how much of that second it may use,
-- and usually declines most of it, so the schedule is only the opportunity to
-- work rather than a description of the work done.
SELECT cron.unschedule('log-attr-indexer')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'log-attr-indexer');

SELECT cron.schedule('log-attr-indexer', '1 seconds', 'CALL run_attr_indexer()');

-- ---------------------------------------------------------------- retention
--
-- Sidecar rows are trimmed inside raw retention, against the same cutoff and in
-- the same transaction, because the ordering matters in one direction only: a
-- sidecar row deleted while its log row survives makes that log invisible to
-- indexed attribute queries. Computing a fresh cutoff in a separate job would
-- do exactly that for everything logged between the two runs.
--
-- The reverse leftover is harmless. A sidecar row whose log has been deleted
-- matches nothing, because every read joins back to logs; it is wasted space,
-- not a wrong answer.
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
    token_rows     BIGINT := 0;
    all_handled    BOOLEAN := TRUE;
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
            all_handled := FALSE;
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

    -- Trimming the sidecar by the same cutoff is only sound because the loop
    -- above has just removed every log below it. A partition it could not read
    -- a bound for is skipped rather than emptied, and deleting tokens for rows
    -- that are still there would make those rows invisible to indexed attribute
    -- queries — a wrong answer rather than a slow one. So the trim waits for a
    -- run that handled everything; the tokens are only wasted space until then.
    --
    -- Bounded by the (timestamp DESC, id DESC) index, so this is a range delete
    -- over the expired prefix rather than a scan.
    IF all_handled THEN
        DELETE FROM log_attr_tokens WHERE "timestamp" < cutoff;
        GET DIAGNOSTICS token_rows = ROW_COUNT;
    END IF;

    RETURN format(
        'retention: cutoff=%s days=%s dropped=[%s] deleted_rows=%s trimmed=%s default_rows=%s token_rows=%s',
        cutoff, days, array_to_string(dropped, ','), deleted_rows,
        COALESCE(trimmed, 'none'), default_rows, token_rows
    );
END;
$$ LANGUAGE plpgsql;

-- ------------------------------------------------------- the index it replaces
--
-- Dropped last, so the sidecar and its indexer exist before the thing they
-- stand in for goes away. Dropping the partitioned index removes the copy on
-- every partition, present and future.
DROP INDEX IF EXISTS idx_logs_attributes_gin;
