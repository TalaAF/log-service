import { sql, type SQL } from 'drizzle-orm';
import { from as copyFrom } from 'pg-copy-streams';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { db, writePool } from '../db/client.js';
import { logs, type NewLogEntry } from '../db/schema.js';
import { attributeCondition, attributeTokens } from './attributeFilter.js';

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
  const statement =
    filters.attributes === undefined
      ? plainStatement(filters)
      : attributeStatement(filters, filters.attributes);

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

/**
 * Reshapes an already ordered, already limited page for JSON.
 *
 * Ordering and LIMIT happen inside `inner`, on the native bigint/timestamptz
 * columns, so rows arrive sorted by the composite index rather than by a sort.
 * This wrapper only converts them (bigint -> text so large ids survive,
 * timestamp -> ISO 8601) and must not re-sort: ordering by the text forms would
 * put '9' after '10'.
 */
function page(inner: SQL): SQL {
  return sql`
    SELECT id::text AS id,
           to_char("timestamp" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS timestamp,
           level,
           service,
           message,
           attributes
    FROM (${inner}) page
  `;
}

/** One indexed scan, for the queries that carry no attribute filter. */
function plainStatement(filters: QueryLogsFilters): SQL {
  const conditions = [
    sql`TRUE`,
    ...boundConditions(filters, sql.raw('')),
    ...rowConditions(filters),
  ];

  return page(sql`
      SELECT id, "timestamp", level, service, message, attributes
      FROM logs
      WHERE ${sql.join(conditions, sql` AND `)}
      ORDER BY "timestamp" DESC, id DESC
      LIMIT ${filters.limit + 1}
  `);
}

/**
 * The same page, assembled from the two id ranges an attribute filter has to
 * cover separately.
 *
 * Below the sidecar watermark the tokens are known to exist, so the hashed
 * prefilter finds candidates without reading raw rows and the join back applies
 * the exact predicate to each one. At or above the watermark the tokens may not
 * exist yet, so those rows are read straight from logs with that same exact
 * predicate. The ranges are disjoint and their union is everything, which is
 * what makes the answer exact however far behind the indexer is: a row is
 * queryable when it is committed, not when it is indexed.
 *
 * Both branches are limited before the merge. The top n of a union of two
 * disjoint sets is the top n of the union of their individual top n, so the
 * indexed side never has to produce more than a page even for an attribute
 * value that half the table carries.
 *
 * The watermark is read inside the statement rather than fetched first. One
 * snapshot sees the tokens and the watermark move together, so the branches
 * stay exactly complementary; read in a separate round trip, a watermark that
 * advanced in between would leave the ids it crossed owned by neither branch.
 */
function attributeStatement(
  filters: QueryLogsFilters,
  attributes: Record<string, string>
): SQL {
  const row = rowConditions(filters);

  // The recheck is a scalar subquery, deliberately, and not a join or an EXISTS.
  //
  // Written as a join it is a join, and the planner may implement it as one:
  // measured on 9M rows it hashed the sidecar and sequentially scanned every
  // partition of logs to build the other side — 375,000 buffer reads to return
  // a hundred rows. Written as EXISTS it is no better, because the planner
  // strips the LIMIT from an EXISTS sublink and pulls it up into the same
  // semijoin, then unique-ifies logs and drives from there: 341,733 rows
  // aggregated to answer a page of 101.
  //
  // Neither estimate is unreasonable on its own terms — both sides really are
  // comparable in size, and nothing in a join says only the first page is
  // wanted. What is wanted is specifically the sidecar as the driving side, so
  // that the page limit terminates the scan, and a correlated scalar subquery
  // is the one form that cannot be turned into anything else. It stays a
  // per-row subplan: one (timestamp, id) index probe per candidate, and the
  // outer ORDER BY ... LIMIT stops producing candidates once the page is full.
  //
  // What the planner still chooses, correctly, is how to find the candidates:
  // the GIN index when the value is rare, a newest-first walk of the sidecar
  // when it is common.
  const recheck = [
    sql`l."timestamp" = t."timestamp"`,
    sql`l.id = t.id`,
    ...row,
  ];

  const indexed = [
    sql`t.tokens @> ${attributeTokens(attributes)}`,
    sql`t.id < (SELECT watermark_id FROM watermark)`,
    ...boundConditions(filters, sql.raw('t.')),
    sql`(SELECT 1 FROM logs l WHERE ${sql.join(recheck, sql` AND `)} LIMIT 1) IS NOT NULL`,
  ];

  // No ORDER BY or LIMIT on this branch. Bounded by the size of the unindexed
  // tail, it would otherwise invite a plan that walks the timestamp index
  // newest-first looking for a page's worth of matches — fine when the value is
  // common, unbounded when it is rare, because nothing tells that scan when it
  // has passed the last unindexed id. Collecting the tail by id keeps the cost
  // proportional to the backlog rather than to the history being searched.
  const unindexed = [
    sql`l.id >= (SELECT watermark_id FROM watermark)`,
    ...boundConditions(filters, sql.raw('l.')),
    ...row,
  ];

  // Both branches carry keys only. The unindexed one has no page limit to hold
  // it down, so materialising whole rows would mean holding every matching row
  // in the backlog — message, attributes and all — to return a hundred of them.
  // The page is reconstituted at the end from the keys that survived, which is
  // one index probe per row actually returned.
  return page(sql`
      WITH watermark AS MATERIALIZED (
        SELECT watermark_id FROM attr_index_state WHERE id
      ),
      indexed AS MATERIALIZED (
        SELECT t.id, t."timestamp"
        FROM log_attr_tokens t
        WHERE ${sql.join(indexed, sql` AND `)}
        ORDER BY t."timestamp" DESC, t.id DESC
        LIMIT ${filters.limit + 1}
      ),
      unindexed AS MATERIALIZED (
        SELECT l.id, l."timestamp"
        FROM logs l
        WHERE ${sql.join(unindexed, sql` AND `)}
      ),
      merged AS (
        SELECT id, "timestamp"
        FROM (SELECT * FROM indexed UNION ALL SELECT * FROM unindexed) sources
        ORDER BY "timestamp" DESC, id DESC
        LIMIT ${filters.limit + 1}
      )
      SELECT l.id, l."timestamp", l.level, l.service, l.message, l.attributes
      FROM merged m
      JOIN logs l ON l."timestamp" = m."timestamp" AND l.id = m.id
      ORDER BY m."timestamp" DESC, m.id DESC
  `);
}

/**
 * Filters on the log row itself.
 *
 * Deliberately unqualified: every column referenced here exists only on `logs`,
 * so the same fragments are valid in the plain query, in the branch that joins
 * the sidecar and in the branch that reads raw rows. The exact attribute
 * predicate is therefore literally the same expression wherever it is applied,
 * which is what keeps the prefiltered and unfiltered paths from being able to
 * disagree.
 */
function rowConditions(filters: QueryLogsFilters): SQL[] {
  const conditions: SQL[] = [];

  if (filters.service !== undefined) {
    conditions.push(sql`service = ${filters.service}`);
  }

  if (filters.level !== undefined) {
    conditions.push(sql`level = ${filters.level}`);
  }

  if (filters.attributes !== undefined) {
    for (const [key, value] of Object.entries(filters.attributes)) {
      conditions.push(attributeCondition(key, value));
    }
  }

  if (filters.q !== undefined) {
    conditions.push(sql`message ILIKE ${'%' + escapeLikePattern(filters.q) + '%'}`);
  }

  return conditions;
}

/**
 * Time window and keyset position, qualified by `alias` because the sidecar
 * carries a timestamp and an id of its own.
 */
function boundConditions(filters: QueryLogsFilters, alias: SQL): SQL[] {
  const conditions: SQL[] = [];

  if (filters.since !== undefined) {
    conditions.push(sql`${alias}"timestamp" >= ${filters.since}::timestamptz`);
  }

  if (filters.until !== undefined) {
    conditions.push(sql`${alias}"timestamp" < ${filters.until}::timestamptz`);
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
      sql`${alias}"timestamp" <= ${timestamp}::timestamptz AND (${alias}"timestamp", ${alias}id) < (${timestamp}::timestamptz, ${id}::bigint)`
    );
  }

  return conditions;
}

/** Neutralises LIKE wildcards so `q` stays a literal substring match. */
function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, (char) => `\\${char}`);
}
