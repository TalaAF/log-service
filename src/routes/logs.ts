import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { processLogBatch } from '../validation/logEntry.js';
import { enqueue } from '../ingest/writeBuffer.js';
import {
  queryLogs,
  type LogCursor,
  type QueryLogsFilters,
} from '../repositories/logsRepository.js';

const LEVELS = ['debug', 'info', 'warn', 'error'];
const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;
const ATTR_PREFIX = 'attr.';

export function registerLogsRoute(app: FastifyInstance) {
  app.post('/logs', handlePostLogs);
  app.get('/logs', handleGetLogs);
}
async function handlePostLogs(request: FastifyRequest, reply: FastifyReply) {
  const body = request.body;

  const isValidShape =
    body != null &&
    typeof body === 'object' &&
    !Array.isArray(body) &&
    Array.isArray((body as Record<string, unknown>).logs);

  if (!isValidShape) {
    return reply.status(400).send({ error: 'request body must be an object with a "logs" array' });
  }

  const rawLogs = (body as Record<string, unknown>).logs as unknown[];
  const { accepted, rejected } = processLogBatch(rawLogs);

  if (accepted.length === 0) {
    return reply.status(400).send({ error: 'all entries were rejected', rejected });
  }

  // Resolves only once the commit carrying these rows has returned, so a 200
  // still means Postgres holds the data — the buffer batches writes, it does
  // not acknowledge them early. A flush failure throws and becomes a 5xx.
  await enqueue(accepted);

  return reply.status(200).send({
    accepted: accepted.length,
    rejected,
  });
}

async function handleGetLogs(request: FastifyRequest, reply: FastifyReply) {
  const parsed = parseLogQuery(request.query as Record<string, unknown>);
  if (!parsed.valid) {
    return reply.status(400).send({ error: parsed.error });
  }

  const { rows, hasMore } = await queryLogs(parsed.filters);

  // hasMore comes from a lookahead row the repository fetched and discarded, so
  // the cursor is non-null only when a further page genuinely exists.
  const last = hasMore ? rows[rows.length - 1] : undefined;

  return reply.status(200).send({
    logs: rows,
    next_cursor: last ? encodeCursor({ timestamp: last.timestamp, id: last.id }) : null,
  });
}

/** Validates and normalises the raw query string into repository filters. */
function parseLogQuery(
  query: Record<string, unknown>
): { valid: true; filters: QueryLogsFilters } | { valid: false; error: string } {
  const filters: QueryLogsFilters = { limit: DEFAULT_LIMIT };

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

  const since = singleValue(query.since);
  if (since !== undefined) {
    if (!isValidTimestamp(since)) {
      return { valid: false, error: 'since must be a valid ISO 8601 timestamp' };
    }
    filters.since = since;
  }

  const until = singleValue(query.until);
  if (until !== undefined) {
    if (!isValidTimestamp(until)) {
      return { valid: false, error: 'until must be a valid ISO 8601 timestamp' };
    }
    filters.until = until;
  }

  if (
    filters.since !== undefined &&
    filters.until !== undefined &&
    new Date(filters.until).getTime() < new Date(filters.since).getTime()
  ) {
    return { valid: false, error: 'until must not be earlier than since' };
  }

  const q = singleValue(query.q);
  if (q !== undefined) {
    if (q === '') return { valid: false, error: 'q must be a non-empty string' };
    filters.q = q;
  }

  const rawLimit = singleValue(query.limit);
  if (rawLimit !== undefined) {
    if (!/^\d+$/.test(rawLimit)) {
      return { valid: false, error: 'limit must be a positive integer' };
    }
    const limit = Number(rawLimit);
    if (limit < 1 || limit > MAX_LIMIT) {
      return { valid: false, error: `limit must be between 1 and ${MAX_LIMIT}` };
    }
    filters.limit = limit;
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

  const rawCursor = singleValue(query.cursor);
  if (rawCursor !== undefined) {
    const cursor = decodeCursor(rawCursor);
    if (cursor === null) return { valid: false, error: 'cursor is invalid or malformed' };
    filters.cursor = cursor;
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

function encodeCursor(cursor: LogCursor): string {
  return Buffer.from(JSON.stringify(cursor), 'utf8').toString('base64');
}

function decodeCursor(raw: string): LogCursor | null {
  try {
    const decoded = JSON.parse(Buffer.from(raw, 'base64').toString('utf8')) as unknown;
    if (decoded == null || typeof decoded !== 'object' || Array.isArray(decoded)) return null;

    const { timestamp, id } = decoded as Record<string, unknown>;
    if (typeof timestamp !== 'string' || !isValidTimestamp(timestamp)) return null;
    if (typeof id !== 'string' || !/^\d+$/.test(id)) return null;

    return { timestamp, id };
  } catch {
    return null;
  }
}

