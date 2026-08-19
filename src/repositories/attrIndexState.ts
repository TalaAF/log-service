import { sql } from 'drizzle-orm';
import { aggregatePool } from '../db/client.js';
import { drizzle } from 'drizzle-orm/node-postgres';

/**
 * What the background attribute indexer has done, for /internal/stats.
 *
 * Read on demand and nowhere else. The query path never consults this: it reads
 * the watermark inside its own statement, in the same snapshot as the tokens,
 * because a watermark fetched separately could describe a different moment than
 * the rows it is supposed to divide. This is diagnostics, and diagnostics that
 * cost something on the hot path are not worth having.
 *
 * The number that actually matters here is `unindexed_ids`. It is not query
 * lag: those ids are answered exactly, from the raw table, the moment they
 * commit. It is how much of an attribute query's work has not been pre-computed
 * yet, which is a cost signal, not a correctness one.
 */
export interface AttrIndexSnapshot extends Record<string, unknown> {
  enabled: boolean;
  watermark_id: string;
  unindexed_ids: number;
  batch_rows: number;
  rows_indexed: number;
  sidecar_rows_estimate: number;
  batches: number;
  cycles: number;
  deferred: number;
  contended: number;
  last_batch_ms: number;
  peak_batch_ms: number;
  mean_batch_ms: number;
  p50_batch_ms: number;
  p95_batch_ms: number;
  db_ms_total: number;
  last_run_at: string | null;
}

interface StateRow extends Record<string, unknown> {
  enabled: boolean;
  watermark_id: string;
  unindexed_ids: string | number | null;
  batch_rows: number;
  rows_indexed: number;
  sidecar_rows_estimate: number;
  batches: number;
  cycles: number;
  deferred: number;
  contended: number;
  last_batch_ms: number;
  peak_batch_ms: number;
  total_batch_ms: number;
  ms_hist: number[] | string[];
  last_run_at: Date | string | null;
}

/** Upper edge of each histogram slot, in milliseconds; the last is unbounded. */
const HISTOGRAM_EDGES = [5, 10, 20, 40, 80, 160, 320, Infinity];


function percentile(hist: number[], fraction: number): number {
  const total = hist.reduce((sum, n) => sum + n, 0);
  if (total === 0) return 0;

  const target = total * fraction;
  let seen = 0;
  for (let slot = 0; slot < hist.length; slot++) {
    seen += hist[slot];
    if (seen >= target) return HISTOGRAM_EDGES[slot] === Infinity ? 320 : HISTOGRAM_EDGES[slot];
  }
  return HISTOGRAM_EDGES[HISTOGRAM_EDGES.length - 2];
}

export async function getAttrIndexSnapshot(): Promise<AttrIndexSnapshot | null> {
  const client = await aggregatePool.connect();
  try {
    const result = await drizzle(client).execute<StateRow>(sql`
      SELECT c.enabled,
             s.watermark_id::text AS watermark_id,
             GREATEST(0, COALESCE(pg_sequence_last_value('logs_id_seq'::regclass), 0) + 1 - s.watermark_id) AS unindexed_ids,
             s.batch_rows,
             s.rows_indexed,
             -- The planner's row estimate, not a count: counting the sidecar
             -- would read every page of it to fill in a diagnostics field.
             (SELECT reltuples::bigint FROM pg_class WHERE relname = 'log_attr_tokens') AS sidecar_rows_estimate,
             s.batches,
             s.cycles,
             s.deferred,
             s.contended,
             s.last_batch_ms,
             s.peak_batch_ms,
             s.total_batch_ms,
             s.ms_hist,
             s.last_run_at
      FROM attr_index_state s
      CROSS JOIN attr_index_config c
      WHERE s.id AND c.id
    `);

    const row = result.rows[0] as StateRow | undefined;
    if (row === undefined) return null;

    const hist = (row.ms_hist as unknown[]).map((n) => Number(n));
    const batches = Number(row.batches);

    return {
      enabled: row.enabled,
      watermark_id: row.watermark_id,
      unindexed_ids: Number(row.unindexed_ids ?? 0),
      batch_rows: Number(row.batch_rows),
      rows_indexed: Number(row.rows_indexed),
      sidecar_rows_estimate: Number(row.sidecar_rows_estimate ?? 0),
      batches,
      cycles: Number(row.cycles),
      deferred: Number(row.deferred),
      contended: Number(row.contended),
      last_batch_ms: Number(Number(row.last_batch_ms).toFixed(1)),
      peak_batch_ms: Number(Number(row.peak_batch_ms).toFixed(1)),
      mean_batch_ms: batches === 0 ? 0 : Number((Number(row.total_batch_ms) / batches).toFixed(1)),
      p50_batch_ms: percentile(hist, 0.5),
      p95_batch_ms: percentile(hist, 0.95),
      db_ms_total: Number(Number(row.total_batch_ms).toFixed(1)),
      last_run_at: row.last_run_at === null ? null : new Date(row.last_run_at).toISOString(),
    };
  } finally {
    client.release();
  }
}
