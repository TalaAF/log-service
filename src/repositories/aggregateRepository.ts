import { sql, type SQL } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { aggregatePool } from '../db/client.js';
import { attributeCondition } from './attributeFilter.js';
import { getRollupState } from './rollupState.js';
import { metrics } from '../observability/metrics.js';
import {
  beginAggregateSql,
  markAggregateTrace,
  type AggregateRequestTrace,
  type AggregateSqlSource,
} from '../observability/aggregateTrace.js';

/** Bucket sizes the API accepts, mapped to their width in seconds. */
export const BUCKET_SECONDS: Record<string, number> = {
  '1m': 60,
  '5m': 300,
  '1h': 3600,
  '1d': 86400,
};

/** Columns `group_by` may reference, mapped to the SQL identifier to group on. */
export const GROUP_BY_COLUMNS: Record<string, string> = {
  service: 'service',
  level: 'level',
};

export interface AggregateFilters {
  since: string;
  until: string;
  bucket: string;
  groupBy?: string;
  service?: string;
  level?: string;
  attributes?: Record<string, string>;
  q?: string;
}

export interface AggregateBucket extends Record<string, unknown> {
  start: string;
  group: string | null;
  count: number;
}

/** Which sources answered a request. Reported by /internal/stats, not to clients. */
export type AggregatePath = 'rollup' | 'raw' | 'hybrid';

/**
 * Cost class, decided before any query runs so admission control can put the
 * request in the right queue.
 *
 * `scan` means the rollup cannot contribute at all, so the whole requested
 * range is read from raw rows and the cost is unbounded — it grows with however
 * much history the caller asked for. Everything else is bounded by the refresh
 * watermark. The distinction is exactly the filters the rollup cannot represent,
 * so it is derived from them rather than guessed at from the range.
 */
export function aggregateCost(filters: AggregateFilters): 'bounded' | 'scan' {
  return filters.q !== undefined || filters.attributes !== undefined ? 'scan' : 'bounded';
}

export interface AggregateResult {
  buckets: AggregateBucket[];
  path: AggregatePath;
}

/** Half-open [from, until) window. `null` means "unbounded", which never occurs here. */
interface TimeRange {
  from: string;
  until: string;
  /** Raw rows in an aligned rollup range are restricted to unfolded IDs. */
  minId?: string;
}

/**
 * Counts matching rows per time bucket, optionally split by service or level.
 *
 * Two sources can answer this, and the choice is made here rather than in the
 * route so there is exactly one place that decides:
 *
 *   log_rollups   pre-aggregated minute counts, bounded by elapsed time
 *   logs          the raw rows, bounded by how many of them there are
 *
 * The rollup can only answer what it stores — counts per minute, service and
 * level — so anything depending on a column it does not carry (`q` over
 * `message`, any `attr.*` predicate) has to fall through to the raw path. The
 * raw path is not a fallback in the sense of being second best; it is the only
 * correct answer for those queries, and it stays exactly as it was.
 */
export async function aggregateLogs(
  filters: AggregateFilters,
  trace: AggregateRequestTrace | null = null
): Promise<AggregateResult> {
  const width = BUCKET_SECONDS[filters.bucket];
  if (width === undefined) {
    throw new Error(`unsupported bucket size: ${filters.bucket}`);
  }

  const plan = await planAggregate(filters, width, trace);

  let rows: AggregateBucket[];
  let actualPath: AggregatePath;
  if (plan.rollup === null) {
    rows = await rawAggregate(filters, width, plan.raw, trace);
    actualPath = 'raw';
  } else {
    const hybrid = await watermarkAggregate(filters, width, plan, trace);
    rows = hybrid.rows;
    actualPath =
      hybrid.rollupRowsTouched > 0
        ? hybrid.rawRowsTouched > 0
          ? 'hybrid'
          : 'rollup'
        : 'raw';
  }

  metrics.aggregatePath[actualPath]++;
  if (trace !== null) trace.path = actualPath;

  // Ascending by bucket start, then by group. `start` is rendered as a
  // zero-padded ISO 8601 UTC string, so lexicographic order is chronological
  // order. Group ordering is not specified by the contract; code-point order is
  // used because it is deterministic and independent of database collation.
  rows.sort((a, b) => {
    if (a.start !== b.start) return a.start < b.start ? -1 : 1;
    const left = a.group ?? '';
    const right = b.group ?? '';
    if (left === right) return 0;
    return left < right ? -1 : 1;
  });
  markAggregateTrace(trace, 'mergeFinished');

  return { buckets: rows, path: actualPath };
}

interface AggregatePlan {
  path: AggregatePath;
  /** Range answered from log_rollups, or null when the rollup cannot be used. */
  rollup: TimeRange | null;
  /** Raw ranges; aligned overlap is made disjoint from `rollup` by `minId`. */
  raw: TimeRange[];
}

/**
 * Decides which source covers which part of the requested window.
 *
 * Three things have to line up before any of it can come from the rollup:
 *
 *   1. the filters only reference columns the rollup carries;
 *   2. the requested bucket is a whole multiple of the stored minute, so larger
 *      buckets can be derived by summing minutes;
 *   3. the sub-range is aligned to whole stored buckets — a partial stored
 *      bucket cannot use a row that counts the whole bucket.
 *
 * Whole buckets use rollups for IDs below the refresh watermark and raw rows
 * at or above it. Partial edge buckets stay entirely raw. The database reads
 * the watermark and both sources in one statement snapshot, making the sources
 * disjoint even if a refresh commits concurrently.
 */
async function planAggregate(
  filters: AggregateFilters,
  width: number,
  trace: AggregateRequestTrace | null
): Promise<AggregatePlan> {
  const wholeRange: TimeRange[] = [{ from: filters.since, until: filters.until }];
  const rawOnly: AggregatePlan = { path: 'raw', rollup: null, raw: wholeRange };

  // `q` matches `message` and `attr.*` matches `attributes`; the rollup carries
  // neither, and approximating either one would be a wrong answer rather than a
  // slower one.
  if (filters.q !== undefined) {
    notePlan(trace, rawOnly);
    return rawOnly;
  }
  if (filters.attributes !== undefined) {
    notePlan(trace, rawOnly);
    return rawOnly;
  }

  const state = await getRollupState(trace);
  if (trace !== null) {
    trace.safeBefore = state.safeBefore?.toISOString() ?? null;
    trace.rollupWatermark = state.watermarkId;
    trace.oldestRollupBucket = state.oldestBucket?.toISOString() ?? null;
    trace.newestRollupBucket = state.newestBucket?.toISOString() ?? null;
  }
  if (state.watermarkId === '0') {
    notePlan(trace, rawOnly);
    return rawOnly;
  }

  // A requested bucket that is not a whole multiple of the stored width cannot
  // be assembled from stored rows -- the edges would not line up and the sum
  // would be partial. Every width the API offers is a multiple of the stored
  // ten seconds; the check is here so that changing either one degrades to the
  // raw path instead of quietly returning wrong counts.
  const stored = state.bucketSeconds;
  if (stored <= 0 || width < stored || width % stored !== 0) {
    notePlan(trace, rawOnly);
    return rawOnly;
  }

  const since = Date.parse(filters.since);
  const until = Date.parse(filters.until);
  if (Number.isNaN(since) || Number.isNaN(until)) {
    notePlan(trace, rawOnly);
    return rawOnly;
  }

  // Whole stored buckets are read from the rollup for folded IDs and from raw
  // for IDs at/above the watermark. The two sources are disjoint by ID, which
  // remains exact even when refresh is behind or an entry is backdated.
  const rollupFrom = ceilBucket(since, filters.since, stored);
  const rollupUntil = floorBucket(until, stored);

  if (rollupFrom >= rollupUntil) {
    notePlan(trace, rawOnly);
    return rawOnly;
  }

  const raw: TimeRange[] = [];
  // Partial stored buckets cannot use their whole rollup row, so all IDs in
  // those fragments come from raw.
  if (since < rollupFrom) raw.push({ from: filters.since, until: isoUtc(rollupFrom) });
  raw.push({ from: isoUtc(rollupFrom), until: isoUtc(rollupUntil), minId: state.watermarkId });
  if (rollupUntil < until) raw.push({ from: isoUtc(rollupUntil), until: filters.until });

  const plan: AggregatePlan = {
    path: 'hybrid',
    rollup: { from: isoUtc(rollupFrom), until: isoUtc(rollupUntil) },
    raw,
  };
  notePlan(trace, plan);
  return plan;
}

function notePlan(trace: AggregateRequestTrace | null, plan: AggregatePlan): void {
  if (trace === null) return;
  trace.path = plan.path;
  trace.rollupRange = plan.rollup === null ? null : { ...plan.rollup };
  trace.rawRanges = plan.raw.map((range) => ({ ...range }));
  const tail = plan.raw.find((range) => range.minId !== undefined) ?? plan.raw.at(-1);
  trace.rawTailStart = tail?.from ?? null;
  trace.rawTailEnd = tail?.until ?? null;
}

/**
 * Counts from the raw table over one or more disjoint ranges.
 *
 * This is the original implementation, unchanged in what it computes. The range
 * filter stays a plain "timestamp" >= / < comparison so partition pruning and
 * the (timestamp DESC, id DESC) index still apply — wrapping the column in the
 * bucket expression would make it unsearchable.
 */
async function rawAggregate(
  filters: AggregateFilters,
  width: number,
  ranges: TimeRange[],
  trace: AggregateRequestTrace | null
): Promise<AggregateBucket[]> {
  if (ranges.length === 0) return [];
  const range = ranges[0];

  const conditions = [
    sql`"timestamp" >= ${range.from}::timestamptz`,
    sql`"timestamp" < ${range.until}::timestamptz`,
  ];

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

  const whereClause = sql.join(conditions, sql` AND `);

  // date_bin is a C-implemented function that bins a timestamptz directly.
  // The previous formulation, floor(extract(epoch FROM ts) / w) * w fed back
  // through to_timestamp(), forced three arbitrary-precision numeric operations
  // and two type conversions per row, which is a large cost to pay millions of
  // times on a single CPU. Binning from the epoch origin produces exactly the
  // same boundaries: every supported width divides evenly into a day.
  const bucketStart = sql`date_bin(make_interval(secs => ${width}), "timestamp", TIMESTAMPTZ 'epoch')`;

  // group_by names a column, which cannot be a bind parameter. Only the two
  // whitelisted keys above ever reach sql.raw, so the identifier is never
  // attacker-controlled; the NULL branch keeps one query shape for both cases.
  const groupColumn: SQL =
    filters.groupBy !== undefined ? sql.raw(GROUP_BY_COLUMNS[filters.groupBy]) : sql`NULL::text`;

  // No ORDER BY in SQL, deliberately.
  //
  // The planner cannot estimate the distinct count of a function expression, so
  // for the bucket column it assumes something between 10% and 100% of the
  // table. Asked to return sorted rows, it then satisfies the grouping and the
  // ordering with one sort of every input row, and picks GroupAggregate over a
  // Sort of the whole scan: measured at 2.35M rows that was an external merge
  // spilling 59MB to disk, 974ms. Dropping the ORDER BY lets it choose
  // HashAggregate over the same scan — 6.2MB in memory, 515ms — because the
  // grouping no longer has to produce ordered output. The caller sorts the
  // aggregated rows instead, of which there are only buckets x groups.
  const rows = await executeAggregate<AggregateBucket>(sql`
    SELECT to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS start,
           grp AS group,
           entries AS count
    FROM (
      SELECT ${bucketStart} AS bucket_start,
             ${groupColumn} AS grp,
             count(*)::bigint AS entries
      FROM logs
      WHERE ${whereClause}
      GROUP BY 1, 2
    ) aggregated
  `, trace, 'raw', range);

  const touched = rows.reduce((sum, row) => sum + row.count, 0);
  if (trace !== null) trace.rawRowsTouched += touched;
  const part = trace?.sql.find(
    (candidate) => candidate.source === 'raw' && candidate.range?.from === range.from
  );
  if (part !== undefined) part.sourceRowsTouched = touched;
  return rows;
}

interface WatermarkAggregateResult {
  rows: AggregateBucket[];
  rawRowsTouched: number;
  rollupRowsTouched: number;
}

interface WatermarkBucketRow extends AggregateBucket {
  raw_rows: number;
  rollup_rows: number;
}

/**
 * Combines folded and unfolded IDs inside one PostgreSQL statement.
 *
 * The statement snapshot is essential. A refresh updates log_rollups and its
 * watermark in one transaction, so this query sees either both old values or
 * both new values. Reading the watermark in one round trip and the rollup in a
 * later one could straddle that commit and count the newly folded IDs twice.
 */
async function watermarkAggregate(
  filters: AggregateFilters,
  width: number,
  plan: AggregatePlan,
  trace: AggregateRequestTrace | null
): Promise<WatermarkAggregateResult> {
  if (plan.rollup === null) throw new Error('watermark aggregate requires a rollup range');

  const rollupConditions = [
    sql`bucket_start >= ${plan.rollup.from}::timestamptz`,
    sql`bucket_start < ${plan.rollup.until}::timestamptz`,
  ];
  if (filters.service !== undefined) rollupConditions.push(sql`service = ${filters.service}`);
  if (filters.level !== undefined) rollupConditions.push(sql`level = ${filters.level}`);

  const rawRanges = plan.raw.map((range) => {
    const conditions = [
      sql`"timestamp" >= ${range.from}::timestamptz`,
      sql`"timestamp" < ${range.until}::timestamptz`,
    ];
    if (range.minId !== undefined) {
      conditions.push(sql`id >= (SELECT watermark_id FROM watermark)`);

      // Redundant lower bound, present only so the planner can see a constant.
      //
      // The line above is the ownership invariant and stays exactly as it was:
      // the watermark is re-read inside this statement's snapshot so folded and
      // unfolded IDs are disjoint even while a refresh commits. But it reaches
      // the planner as an InitPlan parameter of unknown value, so `id >= ?` gets
      // a default selectivity guess over the whole requested window. The planner
      // then decides the timestamp index is the cheaper driver and walks every
      // index entry in the window — measured at 15.8s and ~40k buffers to return
      // a 16k-row tail, with cost scaling by window size rather than tail size.
      //
      // With a constant here instead, the same query plans as a BRIN scan on id
      // (37 buffers, 1.2ms for the index step) because `id` is physically
      // correlated on an append-only table.
      //
      // Safe because it can only ever be weaker than the invariant above.
      // `range.minId` is the watermark this request already read through
      // getRollupState, and runtime refreshes only advance it
      // (h_new := LEAST(seq_last + 1, h_old + max_fold_rows) >= h_old), so a
      // cached copy is always <= the value the subquery returns. `id >= cached`
      // is therefore implied by `id >= current` and cannot exclude a visible
      // row — including a backdated one, whose ownership is decided by ID and
      // not by its timestamp. If the cache were ever ahead of the database this
      // would start excluding rows, which is why it is derived from the state
      // this statement is already planned against rather than from a separate
      // read.
      //
      // Inlined rather than bound so it is a true constant at plan time; the
      // digit test is what makes that safe, and the value's only source is a
      // bigint column read back as text.
      if (/^\d+$/.test(range.minId)) {
        conditions.push(sql`id >= ${sql.raw(range.minId)}`);
      }
    }
    return sql`(${sql.join(conditions, sql` AND `)})`;
  });

  const rawConditions: SQL[] = [sql`(${sql.join(rawRanges, sql` OR `)})`];
  if (filters.service !== undefined) rawConditions.push(sql`service = ${filters.service}`);
  if (filters.level !== undefined) rawConditions.push(sql`level = ${filters.level}`);

  const rollupBucket = sql`date_bin(make_interval(secs => ${width}), bucket_start, TIMESTAMPTZ 'epoch')`;
  const rawBucket = sql`date_bin(make_interval(secs => ${width}), "timestamp", TIMESTAMPTZ 'epoch')`;
  const groupColumn: SQL =
    filters.groupBy !== undefined ? sql.raw(GROUP_BY_COLUMNS[filters.groupBy]) : sql`NULL::text`;

  const rows = await executeAggregate<WatermarkBucketRow>(sql`
    WITH watermark AS MATERIALIZED (
      SELECT watermark_id FROM rollup_state WHERE id
    ),
    rollup_source AS (
      SELECT ${rollupBucket} AS bucket,
             ${groupColumn} AS grp,
             sum(entry_count)::bigint AS entries,
             count(*)::bigint AS rollup_rows,
             0::bigint AS raw_rows
      FROM log_rollups
      WHERE ${sql.join(rollupConditions, sql` AND `)}
      GROUP BY 1, 2
    ),
    raw_source AS (
      SELECT ${rawBucket} AS bucket,
             ${groupColumn} AS grp,
             count(*)::bigint AS entries,
             0::bigint AS rollup_rows,
             count(*)::bigint AS raw_rows
      FROM logs
      WHERE ${sql.join(rawConditions, sql` AND `)}
      GROUP BY 1, 2
    ),
    aggregated AS (
      SELECT bucket,
             grp,
             sum(entries)::bigint AS entries,
             sum(rollup_rows)::bigint AS rollup_rows,
             sum(raw_rows)::bigint AS raw_rows
      FROM (
        SELECT * FROM rollup_source
        UNION ALL
        SELECT * FROM raw_source
      ) sources
      GROUP BY 1, 2
    )
    SELECT to_char(bucket AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS start,
           grp AS group,
           entries AS count,
           rollup_rows,
           raw_rows
    FROM aggregated
  `, trace, 'hybrid', plan.rollup);

  const rawRowsTouched = rows.reduce((sum, row) => sum + row.raw_rows, 0);
  const rollupRowsTouched = rows.reduce((sum, row) => sum + row.rollup_rows, 0);
  if (trace !== null) {
    trace.rawRowsTouched += rawRowsTouched;
    trace.rollupRowsTouched += rollupRowsTouched;
  }
  const part = trace?.sql.find((candidate) => candidate.source === 'hybrid');
  if (part !== undefined) part.sourceRowsTouched = rawRowsTouched + rollupRowsTouched;

  return {
    rows: rows.map(({ raw_rows: _rawRows, rollup_rows: _rollupRows, ...row }) => row),
    rawRowsTouched,
    rollupRowsTouched,
  };
}

/**
 * Counts from the pre-aggregated minute rows.
 *
 * Buckets wider than a minute are derived by summing the minutes inside them,
 * which is exact because every supported width is a whole multiple of a minute
 * and both are binned from the same epoch origin, so the boundaries coincide.
 *
 * The scan here is over tens of rows per minute of history rather than the
 * hundreds of thousands of raw rows behind them, and the primary key carries
 * entry_count as an INCLUDE column, so it is served without touching the heap.
 */
async function rollupAggregate(
  filters: AggregateFilters,
  width: number,
  range: TimeRange,
  trace: AggregateRequestTrace | null
): Promise<AggregateBucket[]> {
  const conditions = [
    sql`bucket_start >= ${range.from}::timestamptz`,
    sql`bucket_start < ${range.until}::timestamptz`,
  ];

  if (filters.service !== undefined) {
    conditions.push(sql`service = ${filters.service}`);
  }

  if (filters.level !== undefined) {
    conditions.push(sql`level = ${filters.level}`);
  }

  const whereClause = sql.join(conditions, sql` AND `);

  // Re-binning an already-binned minute is a no-op when width is 60, and gives
  // the containing 5m/1h/1d bucket otherwise.
  const bucketStart = sql`date_bin(make_interval(secs => ${width}), bucket_start, TIMESTAMPTZ 'epoch')`;

  const groupColumn: SQL =
    filters.groupBy !== undefined ? sql.raw(GROUP_BY_COLUMNS[filters.groupBy]) : sql`NULL::text`;

  // sum() over bigint returns numeric, which node-postgres hands back as a
  // string to protect precision; the contract requires `count` to be a JSON
  // number, so it is narrowed back to bigint and parsed by the int8 handler.
  const rows = await executeAggregate<AggregateBucket & { source_rows: number }>(sql`
    SELECT to_char(bucket AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS start,
           grp AS group,
           entries AS count,
           source_rows
    FROM (
      SELECT ${bucketStart} AS bucket,
             ${groupColumn} AS grp,
             sum(entry_count)::bigint AS entries,
             count(*)::bigint AS source_rows
      FROM log_rollups
      WHERE ${whereClause}
      GROUP BY 1, 2
    ) aggregated
  `, trace, 'rollup', range);

  const touched = rows.reduce((sum, row) => sum + row.source_rows, 0);
  if (trace !== null) trace.rollupRowsTouched += touched;
  const part = trace?.sql.find(
    (candidate) => candidate.source === 'rollup' && candidate.range?.from === range.from
  );
  if (part !== undefined) part.sourceRowsTouched = touched;
  return rows.map(({ source_rows: _sourceRows, ...row }) => row);
}

async function executeAggregate<T extends Record<string, unknown>>(
  statement: SQL,
  trace: AggregateRequestTrace | null,
  source: AggregateSqlSource,
  range: TimeRange
): Promise<T[]> {
  const part = beginAggregateSql(trace, source, range);
  const client = await aggregatePool.connect();
  if (part !== null) part.connectionAcquiredAt = performance.now();

  try {
    const scopedDb = drizzle(client);
    if (part !== null) part.sqlStartedAt = performance.now();
    const result = await scopedDb.execute<T>(statement);
    if (part !== null) {
      part.sqlFinishedAt = performance.now();
      part.returnedRows = result.rows.length;
    }
    return result.rows as T[];
  } finally {
    if (part !== null && part.sqlFinishedAt === undefined) part.sqlFinishedAt = performance.now();
    client.release();
  }
}

/** Adds up counts for the same (bucket, group) across sources. */
function mergeBuckets(sources: AggregateBucket[][]): AggregateBucket[] {
  const merged = new Map<string, AggregateBucket>();

  for (const rows of sources) {
    for (const row of rows) {
      // A NUL cannot appear in a service name or a level: validation strips it
      // before anything is stored, so it is safe as a key separator.
      const key = `${row.start}\u0000${row.group ?? ''}`;
      const existing = merged.get(key);
      if (existing === undefined) merged.set(key, { ...row });
      else existing.count += row.count;
    }
  }

  return [...merged.values()];
}

/** Sub-millisecond digits, which Date.parse silently drops but Postgres keeps. */
const SUB_MS_PRECISION = /\.\d{4,}/;

function floorBucket(ms: number, bucketSeconds: number): number {
  if (!Number.isFinite(ms)) return ms;
  const width = bucketSeconds * 1000;
  return Math.floor(ms / width) * width;
}

/**
 * Smallest stored bucket edge at or after `ms`.
 *
 * Rounding *up* is what keeps the rollup honest at the start of a range: a
 * request beginning at 10:00:05 must not be served the stored row for 10:00:00,
 * which also counts the first five seconds. The raw path covers the remainder.
 *
 * `raw` is consulted because Date.parse truncates to milliseconds. A timestamp
 * of 10:00:00.000400Z parses to an exact edge, and treating it as one would
 * pull in 400 microseconds of rows the caller excluded — so anything carrying
 * finer precision is rounded up to the next edge regardless.
 */
function ceilBucket(ms: number, raw: string, bucketSeconds: number): number {
  const floored = floorBucket(ms, bucketSeconds);
  if (floored === ms && !SUB_MS_PRECISION.test(raw)) return ms;
  return floored + bucketSeconds * 1000;
}

function isoUtc(ms: number): string {
  return new Date(ms).toISOString();
}

/** Neutralises LIKE wildcards so `q` stays a literal substring match. */
function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, (char) => `\\${char}`);
}
