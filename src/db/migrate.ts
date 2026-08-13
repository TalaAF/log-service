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
    await ensurePartitions(pool, log);
  } finally {
    await pool.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_ID]);
  }
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
