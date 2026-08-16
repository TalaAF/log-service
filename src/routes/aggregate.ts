import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import {
  aggregateLogs,
  BUCKET_SECONDS,
  GROUP_BY_COLUMNS,
  type AggregateFilters,
} from '../repositories/aggregateRepository.js';
import { withAggregateSlot } from '../observability/aggregateGate.js';
import { metrics } from '../observability/metrics.js';

const LEVELS = ['debug', 'info', 'warn', 'error'];
const ATTR_PREFIX = 'attr.';

export function registerAggregateRoute(app: FastifyInstance) {
  app.get('/logs/aggregate', handleGetAggregate);
}

async function handleGetAggregate(request: FastifyRequest, reply: FastifyReply) {
  const parsed = parseAggregateQuery(request.query as Record<string, unknown>);
  if (!parsed.valid) {
    return reply.status(400).send({ error: parsed.error });
  }

  metrics.requests.aggregate++;
  const started = performance.now();

  // Validation is done before queueing, so a malformed request still gets its
  // 400 immediately rather than waiting behind expensive work it was never
  // going to do. Everything past this point can touch raw rows.
  const { buckets } = await withAggregateSlot(() => aggregateLogs(parsed.filters));

  metrics.latency.aggregate.record(performance.now() - started);

  return reply.status(200).send({ buckets });
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
