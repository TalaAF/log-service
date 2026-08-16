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

const LIMIT = intFromEnv('AGGREGATE_CONCURRENCY', 2);

let active = 0;
const queue: Array<() => void> = [];

function acquire(): Promise<void> {
  if (active < LIMIT) {
    active++;
    return Promise.resolve();
  }

  metrics.aggregateAdmission.queued++;
  if (queue.length + 1 > metrics.aggregateAdmission.maxQueueDepth) {
    metrics.aggregateAdmission.maxQueueDepth = queue.length + 1;
  }

  return new Promise<void>((resolve) => {
    queue.push(() => {
      active++;
      resolve();
    });
  });
}

function release(): void {
  active--;
  const next = queue.shift();
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
export async function withAggregateSlot<T>(work: () => Promise<T>): Promise<T> {
  const waitStart = performance.now();
  await acquire();
  metrics.latency.aggregateWait.record(performance.now() - waitStart);

  try {
    return await work();
  } finally {
    release();
  }
}

export function gateStats() {
  return { limit: LIMIT, active, waiting: queue.length };
}
