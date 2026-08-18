import { sql } from 'drizzle-orm';
import { from as copyFrom } from 'pg-copy-streams';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { db, writePool } from '../db/client.js';
import { logs, type NewLogEntry } from '../db/schema.js';
import { attributeCondition } from './attributeFilter.js';

const COPY_COLUMNS = '"timestamp", level, service, message, attributes';
const COPY_SQL = `COPY logs (${COPY_COLUMNS}) FROM STDIN`;

/**
 * COPY's text format reserves the backslash and the three whitespace codes
 * below; every other byte is passed through untouched. NUL and unpaired
 * surrogates are already removed during validation, so nothing reaching here
 * can break the stream.
 */
const COPY_ESCAPES: Record<string, string> = {
  '\\': '\\\\',
  '\n': '\\n',
  '\r': '\\r',
  '\t': '\\t',
};
const COPY_SPECIAL = /[\\\n\r\t]/g;

function copyEscape(value: string): string {
  return value.replace(COPY_SPECIAL, (char) => COPY_ESCAPES[char]);
}

/**
 * Writes a batch with COPY FROM STDIN rather than INSERT.
 *
 * INSERT ... VALUES costs a parse, a plan and a bind of six parameters per row,
 * and the query builder emits different SQL text for every distinct batch size,
 * so Postgres can never reuse a plan. COPY sends one fixed statement followed by
 * a stream of pre-formatted tuples, which is the cheapest way to get rows in and
 * matters most on the single CPU the database container is allowed.
 *
 * Row data is never interpolated into SQL: the statement is a compile-time
 * constant and the values travel in COPY's own data stream, so this path has no
 * injection surface.
 */
export async function copyLogs(entries: NewLogEntry[]): Promise<void> {
  if (entries.length === 0) return;

  const client = await writePool.connect();
  try {
    // Built as one string for the whole batch: thousands of small stream writes
    // cost far more in the app container, which has half a CPU, than a single
    // large buffer does.
    const parts: string[] = [];
    for (const entry of entries) {
      parts.push(
        copyEscape(entry.timestamp as string),
        '\t',
        copyEscape(entry.level),
        '\t',
        copyEscape(entry.service),
        '\t',
        copyEscape(entry.message),
        '\t',
        copyEscape(JSON.stringify(entry.attributes ?? {})),
        '\n'
      );
    }

    const target = client.query(copyFrom(COPY_SQL));
    await pipeline(Readable.from([parts.join('')], { objectMode: false }), target);
  } finally {
    client.release();
  }
}

/**
 * Single-batch insert, kept for callers that need a row visible immediately
 * without going through the group-commit buffer (backfills, tests).
 */
export async function insertLogs(entries: NewLogEntry[]): Promise<void> {
  if (entries.length === 0) return;
  await db.insert(logs).values(entries);
}

/** Keyset position: the timestamp/id of the last row of the previous page. */
export interface LogCursor {
  timestamp: string;
  id: string;
}

export interface QueryLogsFilters {
  service?: string;
  level?: string;
  since?: string;
  until?: string;
  attributes?: Record<string, string>;
  q?: string;
  limit: number;
  cursor?: LogCursor;
}

export interface QueriedLog extends Record<string, unknown> {
  id: string;
  timestamp: string;
  level: string;
  service: string;
  message: string;
  attributes: Record<string, unknown>;
}

export interface LogPage {
  rows: QueriedLog[];
  hasMore: boolean;
}

const MARKER_QUERY_CONCURRENCY = 1;
let activeMarkerQueries = 0;

// A marker lookup walks every service/time candidate to prove there are no
// further substring matches. On the one-CPU database, allowing all seven read
// clients to do that together increases total DB time and delays every other
// workload. One slot can serve about 58 queries/s at the measured 17.15 ms
// mean, above the benchmark's 34-query/s peak, while the rest queue cheaply in
// the application.
const markerQueue: Array<() => void> = [];

async function acquireMarkerSlot(): Promise<void> {
  if (activeMarkerQueries < MARKER_QUERY_CONCURRENCY) {
    activeMarkerQueries++;
    return;
  }

  // releaseMarkerSlot transfers the active slot directly to this waiter, so
  // no newly arriving request can jump the queue between release and resume.
  await new Promise<void>((resolve) => markerQueue.push(resolve));
}

function releaseMarkerSlot(): void {
  const next = markerQueue.shift();
  if (next === undefined) activeMarkerQueries--;
  else next();
}

async function withMarkerSlot<T>(work: () => Promise<T>): Promise<T> {
  await acquireMarkerSlot();
  try {
    return await work();
  } finally {
    releaseMarkerSlot();
  }
}

/**
 * Fetches one page of logs ordered by (timestamp DESC, id DESC).
 *
 * Every filter is appended to a single WHERE clause built with the `sql`
 * template tag, so values are bound as parameters rather than interpolated.
 * Pagination is keyset-based off the composite (timestamp DESC, id DESC)
 * index — no OFFSET, so page cost stays flat however deep the caller walks.
 *
 * One row beyond `limit` is fetched and then discarded. That is what makes
 * `next_cursor` honest: without it, a result that happens to be exactly `limit`
 * rows long would hand back a cursor leading to an empty page, and the contract
 * requires a null cursor when no further results exist.
 */
export async function queryLogs(filters: QueryLogsFilters): Promise<LogPage> {
  const conditions = [sql`TRUE`];

  if (filters.service !== undefined) {
    conditions.push(sql`service = ${filters.service}`);
  }

  if (filters.level !== undefined) {
    conditions.push(sql`level = ${filters.level}`);
  }

  if (filters.since !== undefined) {
    conditions.push(sql`"timestamp" >= ${filters.since}::timestamptz`);
  }

  if (filters.until !== undefined) {
    conditions.push(sql`"timestamp" < ${filters.until}::timestamptz`);
  }

  if (filters.attributes !== undefined) {
    for (const [key, value] of Object.entries(filters.attributes)) {
      conditions.push(attributeCondition(key, value));
    }
  }

  if (filters.q !== undefined) {
    conditions.push(sql`message ILIKE ${'%' + escapeLikePattern(filters.q) + '%'}`);
  }

  if (filters.cursor !== undefined) {
    const { timestamp, id } = filters.cursor;
    // Row-wise comparison, not the equivalent OR form.
    //
    // `ts < $1 OR (ts = $1 AND id < $2)` is logically identical but Postgres
    // cannot turn it into an index range condition — it applies it as a Filter
    // and walks the index from the top on every page, discarding everything
    // before the cursor. Measured at a cursor 500k rows deep: 44,263 buffers
    // and 500,001 rows discarded, making a full walk O(n^2). The row
    // constructor becomes an Index Cond and starts the scan at the cursor: 61
    // buffers, nothing discarded.
    //
    // The leading `"timestamp" <=` is redundant — it is entailed by the row
    // comparison, so the result set is unchanged — but a RowCompareExpr cannot
    // drive partition pruning, and without it every page opens all 11
    // partitions instead of the 7 that can hold matching rows.
    conditions.push(
      sql`"timestamp" <= ${timestamp}::timestamptz AND ("timestamp", id) < (${timestamp}::timestamptz, ${id}::bigint)`
    );
  }

  const whereClause = sql.join(conditions, sql` AND `);

  // Ordering and LIMIT happen in the inner query, on the native bigint/timestamptz
  // columns, so the (timestamp DESC, id DESC) index supplies rows already sorted.
  // The outer SELECT only reshapes the page for JSON (bigint -> text so large ids
  // survive, timestamp -> ISO 8601) and must not re-sort: ordering by the text
  // forms would put '9' after '10'.
  const statement = sql`
    SELECT id::text AS id,
           to_char("timestamp" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS timestamp,
           level,
           service,
           message,
           attributes
    FROM (
      SELECT id, "timestamp", level, service, message, attributes
      FROM logs
      WHERE ${whereClause}
      ORDER BY "timestamp" DESC, id DESC
      LIMIT ${filters.limit + 1}
    ) page
  `;

  const result =
    filters.q === undefined
      ? await db.execute<QueriedLog>(statement)
      : await withMarkerSlot(() => db.execute<QueriedLog>(statement));

  const hasMore = result.rows.length > filters.limit;
  return {
    rows: hasMore ? result.rows.slice(0, filters.limit) : result.rows,
    hasMore,
  };
}

/** Neutralises LIKE wildcards so `q` stays a literal substring match. */
function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, (char) => `\\${char}`);
}
