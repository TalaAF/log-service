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

    await ensurePartitions(pool, log);
  } finally {
    await pool.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_ID]);
  }
}

/**
 * Provisions the current week's partition plus WEEKS_AHEAD future ones, so
 * ingestion never lands on a missing range. Runs on every boot; a long-lived
 * deployment should also call this on a timer.
 */
export async function ensurePartitions(pool: Pool, log: (msg: string) => void): Promise<void> {
  const created: string[] = [];
  for (let week = 0; week <= WEEKS_AHEAD; week++) {
    const target = new Date(Date.now() + week * 7 * 24 * 60 * 60 * 1000);
    const { rows } = await pool.query<{ ensure_logs_partition: string }>(
      'SELECT ensure_logs_partition($1)',
      [target.toISOString()]
    );
    created.push(rows[0].ensure_logs_partition);
  }
  log(`partitions ready: ${created.join(', ')}`);
}
