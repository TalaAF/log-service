/**
 * In-process counters, read back through GET /internal/stats.
 *
 * Deliberately counters and reservoirs rather than log lines. An earlier round
 * measured Fastify's two JSON lines per request as the single largest consumer
 * of the app container's half CPU — larger than parsing or validation — so
 * anything that writes per request is not an option here. Incrementing a field
 * on a plain object is free by comparison, and the latency reservoirs are
 * fixed-size arrays that never grow.
 */

/** Latency samples kept per endpoint. Fixed so memory cannot creep. */
const RESERVOIR = 4096;

class Latencies {
  private readonly samples = new Float64Array(RESERVOIR);
  private count = 0;
  private next = 0;

  record(ms: number): void {
    this.samples[this.next] = ms;
    this.next = (this.next + 1) % RESERVOIR;
    if (this.count < RESERVOIR) this.count++;
  }

  /** p50/p95/p99 over the retained window, or zeroes when nothing was recorded. */
  percentiles(): { count: number; p50: number; p95: number; p99: number } {
    if (this.count === 0) return { count: 0, p50: 0, p95: 0, p99: 0 };
    const sorted = Array.from(this.samples.subarray(0, this.count)).sort((a, b) => a - b);
    const at = (p: number) =>
      Number(sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))].toFixed(2));
    return { count: this.count, p50: at(50), p95: at(95), p99: at(99) };
  }
}

export const metrics = {
  requests: { post: 0, getLogs: 0, aggregate: 0 },
  /** Entries accepted and rejected by validation, not requests. */
  entries: { accepted: 0, rejected: 0 },
  /** Which source answered each aggregate; the point of the whole exercise. */
  aggregatePath: { rollup: 0, raw: 0, hybrid: 0 },
  /** Aggregates that waited for a slot, and the deepest the queue ever got. */
  aggregateAdmission: { queued: 0, maxQueueDepth: 0 },
  /**
   * Logical aggregate keys seen, for deciding whether coalescing could help.
   *
   * `requests` counts every aggregate; `distinctKeys` counts how many were
   * semantically different. A request is only a duplicate if every parameter
   * affecting the result matches, so `duplicates` is a lower bound on what
   * coalescing or a short TTL cache could ever remove.
   */
  aggregateKeys: { requests: 0, distinctKeys: 0, duplicates: 0, concurrentDuplicates: 0 },
  latency: {
    post: new Latencies(),
    getLogs: new Latencies(),
    aggregate: new Latencies(),
    /** Time spent waiting for an admission slot, excluded from query time. */
    aggregateWait: new Latencies(),
    /**
     * Time inside the slot: the database round trips plus merging and sorting.
     * Recorded directly rather than derived by subtracting wait percentiles from
     * total percentiles — percentiles are not additive, and that subtraction
     * hides a heavy execution tail, which is exactly what distinguishes "the
     * queue is saturated" from "a few slow queries block the lane".
     */
    aggregateExec: new Latencies(),
  },
};

export function snapshot() {
  return {
    requests: { ...metrics.requests },
    entries: { ...metrics.entries },
    aggregate_path: { ...metrics.aggregatePath },
    aggregate_admission: { ...metrics.aggregateAdmission },
    aggregate_keys: { ...metrics.aggregateKeys },
    latency_ms: {
      post: metrics.latency.post.percentiles(),
      get_logs: metrics.latency.getLogs.percentiles(),
      aggregate: metrics.latency.aggregate.percentiles(),
      aggregate_wait: metrics.latency.aggregateWait.percentiles(),
      aggregate_exec: metrics.latency.aggregateExec.percentiles(),
    },
  };
}
