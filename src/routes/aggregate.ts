import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import {
  aggregateCost,
  aggregateLogs,
  BUCKET_SECONDS,
  GROUP_BY_COLUMNS,
  type AggregateFilters,
} from '../repositories/aggregateRepository.js';
import { withAggregateSlot } from '../observability/aggregateGate.js';
import { metrics } from '../observability/metrics.js';
import {
  beginAggregateTrace,
  finishAggregateTrace,
  markAggregateTrace,
} from '../observability/aggregateTrace.js';

const LEVELS = ['debug', 'info', 'warn', 'error'];
const ATTR_PREFIX = 'attr.';

export function registerAggregateRoute(app: FastifyInstance) {
  app.get('/logs/aggregate', handleGetAggregate);
}

async function handleGetAggregate(request: FastifyRequest, reply: FastifyReply) {
  const query = request.query as Record<string, unknown>;
  const trace = beginAggregateTrace(request.url, {
    since: singleValue(query.since),
    until: singleValue(query.until),
    bucket: singleValue(query.bucket),
    groupBy: singleValue(query.group_by),
  });
  reply.raw.once('finish', () => finishAggregateTrace(trace));

  const parsed = parseAggregateQuery(query);
  if (!parsed.valid) {
    if (trace !== null) trace.validationError = parsed.error;
    return reply.status(400).send({ error: parsed.error });
  }

  if (trace !== null) {
    trace.resolvedSince = parsed.filters.since;
    trace.resolvedUntil = parsed.filters.until;
  }

  metrics.requests.aggregate++;
  const started = performance.now();
  const releaseKey = noteAggregateKey(parsed.filters);

  // Validation is done before queueing, so a malformed request still gets its
  // 400 immediately rather than waiting behind expensive work it was never
  // going to do. Everything past this point can touch raw rows.
  //
  // The lane is chosen from the filters alone, which is enough to know whether
  // the rollup can contribute: a cheap request must not queue behind an
  // unbounded scan just because both are aggregates.
  // Execution is timed inside the slot so queue wait and database work are two
  // independent measurements rather than one derived by subtracting the other.
  markAggregateTrace(trace, 'admissionQueueEntered');
  const { buckets } = await withAggregateSlot(
    aggregateCost(parsed.filters),
    () =>
      (async () => {
        const execStart = performance.now();
        try {
          return await aggregateLogs(parsed.filters, trace);
        } finally {
          metrics.latency.aggregateExec.record(performance.now() - execStart);
          releaseKey();
        }
      })(),
    () => markAggregateTrace(trace, 'admissionSlotAcquired')
  );

  metrics.latency.aggregate.record(performance.now() - started);

  return reply.status(200).send({ buckets });
}

/**
 * Counts how much aggregate traffic is semantically repeated.
 *
 * Coalescing identical in-flight requests, or a short TTL cache, can only help
 * in proportion to how often the same logical question is asked. The key is
 * every parameter that can change the answer, so two requests sharing it are
 * genuinely interchangeable; anything less would risk answering one query with
 * another's result.
 *
 * `duplicates` counts repeats over the retained window, and
 * `concurrentDuplicates` counts the stricter case that in-flight coalescing
 * could actually remove: a request arriving while an identical one is still
 * running. Diagnostic only — nothing reads these to make a decision.
 */
const inFlightKeys = new Map<string, number>();
const seenKeys = new Set<string>();
/** Bounded so a long run cannot grow this without limit. */
const SEEN_KEY_CAP = 20_000;

function aggregateKey(f: AggregateFilters): string {
  const attrs = Object.entries(f.attributes ?? {})
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join(',');
  return [f.since, f.until, f.bucket, f.groupBy ?? '', f.service ?? '', f.level ?? '', f.q ?? '', attrs].join('|');
}

function noteAggregateKey(f: AggregateFilters): () => void {
  const key = aggregateKey(f);
  metrics.aggregateKeys.requests++;

  if (seenKeys.has(key)) metrics.aggregateKeys.duplicates++;
  else {
    if (seenKeys.size >= SEEN_KEY_CAP) seenKeys.clear();
    seenKeys.add(key);
    metrics.aggregateKeys.distinctKeys++;
  }

  const inFlight = inFlightKeys.get(key) ?? 0;
  if (inFlight > 0) metrics.aggregateKeys.concurrentDuplicates++;
  inFlightKeys.set(key, inFlight + 1);

  return () => {
    const n = (inFlightKeys.get(key) ?? 1) - 1;
    if (n <= 0) inFlightKeys.delete(key);
    else inFlightKeys.set(key, n);
  };
}

/** Validates and normalises the raw query string into repository filters. */
function parseAggregateQuery(
  query: Record<string, unknown>
): { valid: true; filters: AggregateFilters } | { valid: false; error: string } {
  const since = singleValue(query.since);
  if (since === undefined) {
    return { valid: false, error: 'since is required' };
  }
  if (!isValidTimestamp(since)) {
    return { valid: false, error: 'since must be a valid ISO 8601 timestamp' };
  }

  const until = singleValue(query.until);
  if (until === undefined) {
    return { valid: false, error: 'until is required' };
  }
  if (!isValidTimestamp(until)) {
    return { valid: false, error: 'until must be a valid ISO 8601 timestamp' };
  }

  if (new Date(until).getTime() < new Date(since).getTime()) {
    return { valid: false, error: 'until must not be earlier than since' };
  }

  const bucket = singleValue(query.bucket);
  if (bucket === undefined) {
    return { valid: false, error: 'bucket is required' };
  }
  if (!Object.hasOwn(BUCKET_SECONDS, bucket)) {
    return { valid: false, error: `bucket must be one of ${Object.keys(BUCKET_SECONDS).join(', ')}` };
  }

  const filters: AggregateFilters = { since, until, bucket };

  const groupBy = singleValue(query.group_by);
  if (groupBy !== undefined) {
    if (!Object.hasOwn(GROUP_BY_COLUMNS, groupBy)) {
      return {
        valid: false,
        error: `group_by must be one of ${Object.keys(GROUP_BY_COLUMNS).join(', ')}`,
      };
    }
    filters.groupBy = groupBy;
  }

  const service = singleValue(query.service);
  if (service !== undefined) {
    if (service === '') return { valid: false, error: 'service must be a non-empty string' };
    filters.service = service;
  }

  const level = singleValue(query.level);
  if (level !== undefined) {
    if (!LEVELS.includes(level)) {
      return { valid: false, error: `level must be one of ${LEVELS.join(', ')}` };
    }
    filters.level = level;
  }

  const q = singleValue(query.q);
  if (q !== undefined) {
    if (q === '') return { valid: false, error: 'q must be a non-empty string' };
    filters.q = q;
  }

  const attributes: Record<string, string> = {};
  for (const [key, rawValue] of Object.entries(query)) {
    if (!key.startsWith(ATTR_PREFIX)) continue;

    const attrKey = key.slice(ATTR_PREFIX.length);
    if (attrKey === '') return { valid: false, error: 'attribute filter key must not be empty' };

    const value = singleValue(rawValue);
    if (value === undefined) {
      return { valid: false, error: `attribute filter ${key} must have a single value` };
    }
    attributes[attrKey] = value;
  }
  if (Object.keys(attributes).length > 0) {
    filters.attributes = attributes;
  }

  return { valid: true, filters };
}

/**
 * Fastify collects repeated query params into an array. Filters take a single
 * value, so anything else is treated as absent (arrays are rejected upstream).
 */
function singleValue(value: unknown): string | undefined {
  if (typeof value === 'string') return value;
  return undefined;
}

function isValidTimestamp(value: string): boolean {
  return value !== '' && !Number.isNaN(new Date(value).getTime());
}
