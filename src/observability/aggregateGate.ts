import { metrics } from './metrics.js';

/**
 * Admission control for the one endpoint that can spend unbounded CPU.
 *
 * Postgres has a single core. An aggregate that has to read raw rows is the
 * only request in this service whose cost is not bounded by a LIMIT, and
 * letting an arbitrary number of them run at once does not make them finish
 * sooner — it makes each of them slower by the same factor while taking the
 * core away from ingestion and from paged reads. Measured before this existed,
 * four aggregates per second against ~2M rows drove ingestion from 15,000
 * logs/s to 315 and left GET /logs at a p95 of 4.1 s.
 *
 * A queue does not slow the system down here; it only moves the waiting out of
 * Postgres, where waiting is expensive because a waiting backend still holds a
 * connection and a share of the CPU, into the application, where it is a
 * resolved promise.
 *
 * Scope is deliberately narrow:
 *   - ingestion is untouched, and writes go through their own pool;
 *   - GET /logs is untouched, and keeps its own share of the read pool;
 *   - only aggregate work queues, and only behind other aggregate work.
 *
 * The limit is also what keeps GET /logs off the aggregate's tail: at most
 * AGGREGATE_CONCURRENCY of the read pool's connections can be held by
 * aggregates, so the rest stay available for paged reads.
 */

function intFromEnv(name: string, fallback: number): number {
  const parsed = Number(process.env[name]);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

/**
 * Aggregates come in two cost classes and they do not belong in one queue.
 *
 * A rollup or hybrid request reads pre-aggregated rows plus, at most, the hot
 * tail — bounded by the refresh boundary, tens of milliseconds in practice.
 * A request carrying `q` or an `attr.*` filter cannot use the rollup at all and
 * reads every raw row in the requested range: measured at 27s p95 under load,
 * during which it halved ingestion throughput.
 *
 * Sharing one limit forces a choice between throttling the cheap queries
 * needlessly and letting the expensive ones run several at a time. Splitting
 * them lets the bounded class stay responsive while the unbounded class is held
 * to a single concurrent scan, which is what actually protects the write path.
 */
const BOUNDED_LIMIT = intFromEnv('AGGREGATE_CONCURRENCY', 2);
const SCAN_LIMIT = intFromEnv('AGGREGATE_SCAN_CONCURRENCY', 1);

/** Cost class of an aggregate, decided by the repository's routing. */
export type AggregateCost = 'bounded' | 'scan';

interface Lane {
  limit: number;
  active: number;
  queue: Array<() => void>;
}

const lanes: Record<AggregateCost, Lane> = {
  bounded: { limit: BOUNDED_LIMIT, active: 0, queue: [] },
  scan: { limit: SCAN_LIMIT, active: 0, queue: [] },
};

function acquire(lane: Lane): Promise<void> {
  if (lane.active < lane.limit) {
    lane.active++;
    return Promise.resolve();
  }

  metrics.aggregateAdmission.queued++;
  if (lane.queue.length + 1 > metrics.aggregateAdmission.maxQueueDepth) {
    metrics.aggregateAdmission.maxQueueDepth = lane.queue.length + 1;
  }

  return new Promise<void>((resolve) => {
    lane.queue.push(() => {
      lane.active++;
      resolve();
    });
  });
}

function release(lane: Lane): void {
  lane.active--;
  const next = lane.queue.shift();
  if (next !== undefined) next();
}

/**
 * Runs `work` once a slot is free.
 *
 * Each admitted request still executes its own query against a fresh database
 * snapshot — nothing is shared or reused between callers, so a request can
 * never be answered from a view of the data older than the moment it started
 * running. Queueing changes when a request is served, never what it sees.
 */
export async function withAggregateSlot<T>(cost: AggregateCost, work: () => Promise<T>): Promise<T> {
  const lane = lanes[cost];
  const waitStart = performance.now();
  await acquire(lane);
  metrics.latency.aggregateWait.record(performance.now() - waitStart);

  try {
    return await work();
  } finally {
    release(lane);
  }
}

export function gateStats() {
  return {
    bounded: { limit: lanes.bounded.limit, active: lanes.bounded.active, waiting: lanes.bounded.queue.length },
    scan: { limit: lanes.scan.limit, active: lanes.scan.active, waiting: lanes.scan.queue.length },
  };
}
