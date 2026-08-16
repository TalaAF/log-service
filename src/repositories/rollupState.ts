import { sql } from 'drizzle-orm';
import { db } from '../db/client.js';

/**
 * The refresh job's published position, as the query path sees it.
 *
 * `safeBefore` is the boundary between the two sources: every raw row with a
 * timestamp below it is already counted in `log_rollups`, so that side of the
 * boundary can be answered from the rollup and the other side has to come from
 * the raw table. It is always aligned to a whole minute, which is the rollup's
 * bucket width, so no bucket is ever split between the two.
 */
export interface RollupState {
  safeBefore: Date | null;
  watermarkId: string;
  lastRefreshAt: Date | null;
  lastRows: number;
  lastDurationMs: number;
  refreshes: number;
  skipped: number;
  behind: boolean;
}

interface CachedState {
  value: RollupState;
  readAt: number;
}

/**
 * Reading rollup_state is a single-row primary key lookup, but the aggregate
 * path would otherwise pay for it on every request, and under the benchmark's
 * query rate those round trips are backend wakeups on a CPU that has none to
 * spare.
 *
 * Caching it is safe in one direction only, and this is that direction:
 * `safe_before` only ever moves forwards, so a cached copy is at worst *older*
 * than the truth. An older boundary means more of the range is answered from
 * raw rows — slower, never wrong. A cache that could run ahead of the refresh
 * job would be a correctness bug; this one cannot.
 */
const STATE_TTL_MS = Number(process.env.ROLLUP_STATE_TTL_MS) || 1000;

let cached: CachedState | null = null;
let inFlight: Promise<RollupState> | null = null;

interface StateRow {
  safe_before: Date | string | null;
  watermark_id: string;
  last_refresh_at: Date | string | null;
  last_rows: number;
  last_duration_ms: number;
  refreshes: number;
  skipped: number;
  behind: boolean;
}

const UNAVAILABLE: RollupState = {
  safeBefore: null,
  watermarkId: '0',
  lastRefreshAt: null,
  lastRows: 0,
  lastDurationMs: 0,
  refreshes: 0,
  skipped: 0,
  behind: false,
};

async function read(): Promise<RollupState> {
  const result = await db.execute<StateRow>(sql`
    SELECT safe_before,
           watermark_id::text AS watermark_id,
           last_refresh_at,
           last_rows,
           last_duration_ms,
           refreshes,
           skipped,
           behind
    FROM rollup_state
    WHERE id
  `);

  const row = result.rows[0];
  if (row === undefined) return UNAVAILABLE;

  return {
    safeBefore: row.safe_before === null ? null : new Date(row.safe_before),
    watermarkId: row.watermark_id,
    lastRefreshAt: row.last_refresh_at === null ? null : new Date(row.last_refresh_at),
    lastRows: Number(row.last_rows),
    lastDurationMs: Number(row.last_duration_ms),
    refreshes: Number(row.refreshes),
    skipped: Number(row.skipped),
    behind: row.behind,
  };
}

/**
 * Current state, from cache when fresh. Concurrent callers that miss share one
 * query rather than each opening their own.
 *
 * A read failure degrades to "no rollup available", which routes every
 * aggregate down the raw path. That is the pre-rollup behaviour: slower, but it
 * still returns the right answer, so a problem with the rollup machinery can
 * never turn into a wrong result or a 5xx.
 */
export async function getRollupState(): Promise<RollupState> {
  const now = Date.now();
  if (cached !== null && now - cached.readAt < STATE_TTL_MS) return cached.value;
  if (inFlight !== null) return inFlight;

  inFlight = read()
    .then((value) => {
      cached = { value, readAt: Date.now() };
      return value;
    })
    .catch(() => (cached?.value ?? UNAVAILABLE))
    .finally(() => {
      inFlight = null;
    });

  return inFlight;
}

/** Drops the cached copy. Used by tests that need to observe a refresh at once. */
export function invalidateRollupState(): void {
  cached = null;
}
