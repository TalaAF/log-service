import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { sql } from 'drizzle-orm';
import { db, pool, writePool } from '../db/client.js';
import { snapshot } from '../observability/metrics.js';
import { gateStats } from '../observability/aggregateGate.js';
import { bufferStats } from '../ingest/writeBuffer.js';
import { getRollupState } from '../repositories/rollupState.js';

/**
 * Diagnostics, outside the four endpoints of the contract.
 *
 * It exists because the interesting question about this service — is the
 * aggregate being answered from the rollup, from raw rows, or from both, and
 * how far behind is the refresh — cannot be answered from latency numbers
 * alone, and the alternative way of surfacing it is per-request logging, which
 * an earlier round measured as the largest single consumer of the app's CPU
 * budget. Everything here is read on demand: nothing is computed unless this
 * endpoint is called.
 */
export function registerStatsRoute(app: FastifyInstance) {
  app.get('/internal/stats', handleGetStats);
}

interface DbStatsRow extends Record<string, unknown> {
  active_connections: number;
  rollup_rows: number;
}

async function handleGetStats(request: FastifyRequest, reply: FastifyReply) {
  const state = await getRollupState();

  let dbStats: DbStatsRow | undefined;
  try {
    const result = await db.execute<DbStatsRow>(sql`
      SELECT (SELECT count(*)::int FROM pg_stat_activity
              WHERE datname = current_database() AND state = 'active') AS active_connections,
             (SELECT count(*)::int FROM log_rollups) AS rollup_rows
    `);
    dbStats = result.rows[0];
  } catch {
    // Diagnostics must never be the reason a request fails.
  }

  const now = Date.now();

  return reply.status(200).send({
    ...snapshot(),
    aggregate_gate: gateStats(),
    ingest: bufferStats(),
    rollup: {
      safe_before: state.safeBefore?.toISOString() ?? null,
      // How much of the timeline the aggregate still has to read raw. This is
      // the number that decides whether the endpoint's cost is bounded.
      hot_tail_seconds:
        state.safeBefore === null ? null : Math.round((now - state.safeBefore.getTime()) / 1000),
      watermark_id: state.watermarkId,
      last_refresh_at: state.lastRefreshAt?.toISOString() ?? null,
      refresh_age_seconds:
        state.lastRefreshAt === null
          ? null
          : Math.round((now - state.lastRefreshAt.getTime()) / 1000),
      last_rows: state.lastRows,
      last_duration_ms: Number(state.lastDurationMs.toFixed(1)),
      refreshes: state.refreshes,
      skipped: state.skipped,
      catching_up: state.behind,
    },
    pools: {
      read: { total: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount },
      write: { total: writePool.totalCount, idle: writePool.idleCount, waiting: writePool.waitingCount },
    },
    db: dbStats ?? null,
  });
}
