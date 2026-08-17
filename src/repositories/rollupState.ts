import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { aggregatePool } from '../db/client.js';
import {
  beginAggregateSql,
  type AggregateRequestTrace,
} from '../observability/aggregateTrace.js';

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
  /**
   * Width of a stored rollup row, in seconds. Read from the database rather
   * than assumed, because the query path aligns its boundary to this and a
   * disagreement between the two would not fail loudly -- it would return
   * counts that are wrong by whatever fell into the mismatched fragment.
   */
  bucketSeconds: number;
  watermarkId: string;
  lastRefreshAt: Date | null;
  lastRows: number;
  lastDurationMs: number;
  refreshes: number;
  skipped: number;
  behind: boolean;
  oldestBucket: Date | null;
  newestBucket: Date | null;
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

interface StateRow extends Record<string, unknown> {
  safe_before: Date | string | null;
  bucket_seconds: number;
  watermark_id: string;
  last_refresh_at: Date | string | null;
  last_rows: number;
  last_duration_ms: number;
  refreshes: number;
  skipped: number;
  behind: boolean;
  oldest_bucket: Date | string | null;
  newest_bucket: Date | string | null;
}

const UNAVAILABLE: RollupState = {
  safeBefore: null,
  bucketSeconds: 60,
  watermarkId: '0',
  lastRefreshAt: null,
  lastRows: 0,
  lastDurationMs: 0,
  refreshes: 0,
  skipped: 0,
  behind: false,
  oldestBucket: null,
  newestBucket: null,
};

async function read(trace: AggregateRequestTrace | null): Promise<RollupState> {
  const part = beginAggregateSql(trace, 'state', null);
  const client = await aggregatePool.connect();
  if (part !== null) part.connectionAcquiredAt = performance.now();

  let rows: StateRow[];
  try {
    if (part !== null) part.sqlStartedAt = performance.now();
    const result = await drizzle(client).execute<StateRow>(sql`
      SELECT rs.safe_before,
             rc.bucket_seconds,
             rs.watermark_id::text AS watermark_id,
             rs.last_refresh_at,
             rs.last_rows,
             rs.last_duration_ms,
             rs.refreshes,
             rs.skipped,
             rs.behind,
             (SELECT min(bucket_start) FROM log_rollups) AS oldest_bucket,
             (SELECT max(bucket_start) FROM log_rollups) AS newest_bucket
      FROM rollup_state rs
      CROSS JOIN rollup_config rc
      WHERE rs.id AND rc.id
    `);
    rows = result.rows as StateRow[];
    if (part !== null) {
      part.sqlFinishedAt = performance.now();
      part.returnedRows = rows.length;
      part.sourceRowsTouched = rows.length;
    }
  } finally {
    if (part !== null && part.sqlFinishedAt === undefined) part.sqlFinishedAt = performance.now();
    client.release();
  }

  const row = rows[0];
  if (row === undefined) return UNAVAILABLE;

  return {
    safeBefore: row.safe_before === null ? null : new Date(row.safe_before),
    bucketSeconds: Number(row.bucket_seconds),
    watermarkId: row.watermark_id,
    lastRefreshAt: row.last_refresh_at === null ? null : new Date(row.last_refresh_at),
    lastRows: Number(row.last_rows),
    lastDurationMs: Number(row.last_duration_ms),
    refreshes: Number(row.refreshes),
    skipped: Number(row.skipped),
    behind: row.behind,
    oldestBucket: row.oldest_bucket === null ? null : new Date(row.oldest_bucket),
    newestBucket: row.newest_bucket === null ? null : new Date(row.newest_bucket),
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
export async function getRollupState(trace: AggregateRequestTrace | null = null): Promise<RollupState> {
  const now = Date.now();
  if (cached !== null && now - cached.readAt < STATE_TTL_MS) return cached.value;
  if (inFlight !== null) return inFlight;

  inFlight = read(trace)
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
