import { readdir, readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Pool } from 'pg';

// Migrations sit next to this module and are copied into the image verbatim,
// so resolve them relative to the compiled file rather than to process.cwd().
const migrationsDir = join(dirname(fileURLToPath(import.meta.url)), 'migrations');

// Any positive int works; it just has to be the same in every replica.
const MIGRATION_LOCK_ID = 776_1420;

/** How many future weekly partitions to provision ahead of the current week. */
const WEEKS_AHEAD = 4;

/** Fallback when RETENTION_DAYS is unset; mirrors the migration's seed value. */
const DEFAULT_RETENTION_DAYS = 30;

async function appliedMigrations(pool: Pool): Promise<Set<string>> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      name        TEXT PRIMARY KEY,
      applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
  const { rows } = await pool.query<{ name: string }>('SELECT name FROM schema_migrations');
  return new Set(rows.map((r) => r.name));
}

/**
 * Applies every .sql file in the migrations folder exactly once, in filename
 * order. Safe to call on every boot and safe to run concurrently: a session
 * advisory lock serialises replicas racing to migrate the same database.
 */
export async function runMigrations(pool: Pool, log: (msg: string) => void): Promise<void> {
  await pool.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_ID]);
  try {
    const applied = await appliedMigrations(pool);
    const files = (await readdir(migrationsDir)).filter((f) => f.endsWith('.sql')).sort();

    if (files.length === 0) {
      throw new Error(`No .sql migrations found in ${migrationsDir}`);
    }

    for (const file of files) {
      if (applied.has(file)) {
        log(`migration ${file} already applied, skipping`);
        continue;
      }

      const sql = await readFile(join(migrationsDir, file), 'utf8');
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [file]);
        await client.query('COMMIT');
        log(`migration ${file} applied`);
      } catch (err) {
        await client.query('ROLLBACK');
        throw new Error(`migration ${file} failed: ${(err as Error).message}`, { cause: err });
      } finally {
        client.release();
      }
    }

    await syncRetentionConfig(pool, log);
    await syncRollupConfig(pool, log);
    await syncAttrIndexConfig(pool, log);
    await ensurePartitions(pool, log);
  } finally {
    await pool.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_ID]);
  }
}

/**
 * Writes the rollup knobs into rollup_config so the pg_cron refresh picks them
 * up, on the same every-boot basis as retention.
 *
 * ROLLUP_LAG_SECONDS is the one worth thinking about. It is how far behind
 * now() the rollup/raw boundary is allowed to sit, and it is the difference
 * between an aggregate reading a minute of raw rows and reading two.
 *
 * It used to have to cover the delay between a client stamping a log and
 * Postgres committing it, which meant guessing generously. It no longer does:
 * the ingest floor (src/ingest/ingestFloor.ts) covers that case exactly, by
 * pulling the boundary below anything this process has recently accepted
 * whatever its timestamp. What is left for the lag to absorb is clock skew
 * between the client and the database, so 10 seconds is ample.
 *
 * The measurement behind caring: at 15,000 logs/s a 90-second hot tail is
 * 1.2M raw rows, 34,715 buffer reads and 1.6s of Postgres time per aggregate.
 * The tail does not grow with the table — that is the point of the rollup — but
 * the constant is worth keeping small.
 *
 * ROLLUP_RETENTION_DAYS defaults to RETENTION_DAYS. Rollup rows are small
 * enough to keep for much longer, but doing so by default would let aggregates
 * report counts for logs that retention has already deleted, which is a change
 * in what the endpoint means rather than a tuning decision.
 */
export async function syncRollupConfig(pool: Pool, log: (msg: string) => void): Promise<void> {
  const lagSeconds = positiveIntFromEnv('ROLLUP_LAG_SECONDS', 10, true);
  const maxFoldRows = positiveIntFromEnv('ROLLUP_MAX_FOLD_ROWS', 1_000_000, false);
  const rollupDays = positiveIntFromEnv('ROLLUP_RETENTION_DAYS', retentionDays(), false);

  await pool.query(
    `INSERT INTO rollup_config (id, lag_seconds, max_fold_rows, retention_days)
     VALUES (TRUE, $1, $2, $3)
     ON CONFLICT (id) DO UPDATE
       SET lag_seconds    = EXCLUDED.lag_seconds,
           max_fold_rows  = EXCLUDED.max_fold_rows,
           retention_days = EXCLUDED.retention_days`,
    [lagSeconds, maxFoldRows, rollupDays]
  );
  log(`rollups: lag ${lagSeconds}s, retention ${rollupDays} day(s), fold cap ${maxFoldRows} rows`);
}

/**
 * Writes the attribute-indexer knobs into attr_index_config, on the same
 * every-boot basis as the others.
 *
 * These are throttle settings, not correctness settings. The indexer is a
 * background consumer of a single CPU that ingestion and queries are also
 * using, and every attribute query stays exact whether it runs, falls behind or
 * never runs at all — the raw fallback covers whatever is not indexed yet. So
 * they are deliberately ungenerous: ATTR_INDEX_BUDGET_MS is how long a cycle
 * may keep working while the database is quiet, and ATTR_INDEX_BUSY_BACKENDS
 * how many other executing backends make it stop after a single small batch
 * and hand the core back.
 *
 * ATTR_INDEX_ENABLED=false turns it off entirely, which leaves every attribute
 * query on the fallback path. That is a supported state, not a broken one, and
 * it is the switch to reach for if the indexer is ever suspected of costing
 * more than it returns.
 */
export async function syncAttrIndexConfig(pool: Pool, log: (msg: string) => void): Promise<void> {
  const enabled = (process.env.ATTR_INDEX_ENABLED ?? 'true').toLowerCase() !== 'false';
  const targetBatchMs = positiveIntFromEnv('ATTR_INDEX_TARGET_BATCH_MS', 25, false);
  const maxBatchMs = positiveIntFromEnv('ATTR_INDEX_MAX_BATCH_MS', 60, false);
  const budgetMs = positiveIntFromEnv('ATTR_INDEX_BUDGET_MS', 300, true);
  const minBatchRows = positiveIntFromEnv('ATTR_INDEX_MIN_BATCH_ROWS', 250, false);
  const maxBatchRows = positiveIntFromEnv('ATTR_INDEX_MAX_BATCH_ROWS', 25_000, false);
  const busyBackends = positiveIntFromEnv('ATTR_INDEX_BUSY_BACKENDS', 1, false);

  await pool.query(
    `INSERT INTO attr_index_config (id, enabled, target_batch_ms, max_batch_ms,
                                    budget_ms, min_batch_rows, max_batch_rows, busy_backends)
     VALUES (TRUE, $1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (id) DO UPDATE
       SET enabled         = EXCLUDED.enabled,
           target_batch_ms = EXCLUDED.target_batch_ms,
           max_batch_ms    = EXCLUDED.max_batch_ms,
           budget_ms       = EXCLUDED.budget_ms,
           min_batch_rows  = EXCLUDED.min_batch_rows,
           max_batch_rows  = EXCLUDED.max_batch_rows,
           busy_backends   = EXCLUDED.busy_backends`,
    [enabled, targetBatchMs, maxBatchMs, budgetMs, minBatchRows, maxBatchRows, busyBackends]
  );
  log(
    `attribute index: ${enabled ? 'on' : 'off'}, batch target ${targetBatchMs}ms (max ${maxBatchMs}ms), ` +
      `catch-up budget ${budgetMs}ms, yields above ${busyBackends} active backend(s)`
  );
}

/** Reads a positive integer setting, or zero-or-more when `allowZero`. */
function positiveIntFromEnv(name: string, fallback: number, allowZero: boolean): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;

  const value = Number(raw);
  if (!Number.isInteger(value) || value < (allowZero ? 0 : 1)) {
    throw new Error(`${name} must be a ${allowZero ? 'non-negative' : 'positive'} integer, got "${raw}"`);
  }
  return value;
}

/**
 * Provisions weekly partitions across the whole window rows can legitimately
 * land in: back to the retention cutoff and WEEKS_AHEAD into the future.
 *
 * Backwards matters as much as forwards. Any row older than the oldest real
 * partition is routed to the DEFAULT partition, which retention cannot drop
 * wholesale and has to clean row by row — so a backfill of historical data, or
 * simply a month of stored history, would otherwise accumulate there. Runs on
 * every boot; pg_cron repeats it hourly.
 */
export async function ensurePartitions(pool: Pool, log: (msg: string) => void): Promise<void> {
  const weeksBack = Math.ceil(retentionDays() / 7);
  const { rows } = await pool.query<{ ensure_logs_partition_range: string }>(
    'SELECT ensure_logs_partition_range($1, $2)',
    [weeksBack, WEEKS_AHEAD]
  );
  log(`partitions ready (${weeksBack} weeks back, ${WEEKS_AHEAD} ahead): ${rows[0].ensure_logs_partition_range}`);
}

/** RETENTION_DAYS, validated. Shared by partitioning and the config upsert. */
function retentionDays(): number {
  const raw = process.env.RETENTION_DAYS ?? String(DEFAULT_RETENTION_DAYS);
  const days = Number(raw);

  if (!Number.isInteger(days) || days <= 0) {
    throw new Error(`RETENTION_DAYS must be a positive integer, got "${raw}"`);
  }
  return days;
}

/**
 * Writes RETENTION_DAYS into retention_config so the pg_cron job picks it up.
 * Runs on every boot, so restarting with a changed env var changes the active
 * retention window without a code change or a re-scheduled job.
 */
export async function syncRetentionConfig(pool: Pool, log: (msg: string) => void): Promise<void> {
  const days = retentionDays();

  await pool.query(
    `INSERT INTO retention_config (id, retention_days, updated_at)
     VALUES (TRUE, $1, now())
     ON CONFLICT (id) DO UPDATE
       SET retention_days = EXCLUDED.retention_days,
           updated_at     = now()`,
    [days]
  );
  log(`retention window set to ${days} day(s)`);
}
