import { sql, type SQL } from 'drizzle-orm';
import { db } from '../db/client.js';
import { attributeCondition } from './attributeFilter.js';
import { getRollupState } from './rollupState.js';
import { acceptedFloorMs } from '../ingest/ingestFloor.js';
import { metrics } from '../observability/metrics.js';

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

/** Width of one stored rollup row. Every supported bucket is a multiple of it. */
const ROLLUP_SECONDS = 60;

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

export interface AggregateResult {
  buckets: AggregateBucket[];
  path: AggregatePath;
}

/** Half-open [from, until) window. `null` means "unbounded", which never occurs here. */
interface TimeRange {
  from: string;
  until: string;
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
export async function aggregateLogs(filters: AggregateFilters): Promise<AggregateResult> {
  const width = BUCKET_SECONDS[filters.bucket];
  if (width === undefined) {
    throw new Error(`unsupported bucket size: ${filters.bucket}`);
  }

  const plan = await planAggregate(filters, width);

  let rows: AggregateBucket[];
  if (plan.rollup === null) {
    rows = await rawAggregate(filters, width, plan.raw);
  } else if (plan.raw.length === 0) {
    rows = await rollupAggregate(filters, width, plan.rollup);
  } else {
    // Hybrid. The two sources cover disjoint half-open time ranges that tile the
    // request exactly, so a row is counted by one of them and never by both;
    // merging is a straight addition per (bucket, group).
    rows = mergeBuckets([
      await rollupAggregate(filters, width, plan.rollup),
      ...(await Promise.all(plan.raw.map((range) => rawAggregate(filters, width, [range])))),
    ]);
  }

  metrics.aggregatePath[plan.path]++;

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

  return { buckets: rows, path: plan.path };
}

interface AggregatePlan {
  path: AggregatePath;
  /** Range answered from log_rollups, or null when the rollup cannot be used. */
  rollup: TimeRange | null;
  /** Ranges answered from the raw table. Disjoint from `rollup` and from each other. */
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
 *   3. the sub-range is *both* below the refresh boundary and aligned to whole
 *      minutes — a partial minute cannot be taken from a row that counts the
 *      whole minute.
 *
 * Whatever is left over is read from the raw table. In the steady state that
 * leftover is the newest minute or two, whose size depends on the ingest rate
 * and not at all on how much history has accumulated. That is the property that
 * matters: the cost of this endpoint stops growing with the table.
 */
async function planAggregate(filters: AggregateFilters, width: number): Promise<AggregatePlan> {
  const wholeRange: TimeRange[] = [{ from: filters.since, until: filters.until }];
  const rawOnly: AggregatePlan = { path: 'raw', rollup: null, raw: wholeRange };

  // `q` matches `message` and `attr.*` matches `attributes`; the rollup carries
  // neither, and approximating either one would be a wrong answer rather than a
  // slower one.
  if (filters.q !== undefined) return rawOnly;
  if (filters.attributes !== undefined) return rawOnly;

  // A bucket narrower than a stored rollup row cannot be reconstructed from it.
  // No supported width is, today: 1m is both the smallest offered bucket and the
  // stored granularity. The check is here so that adding a narrower bucket to
  // BUCKET_SECONDS degrades to the raw path instead of returning wrong counts.
  if (width < ROLLUP_SECONDS || width % ROLLUP_SECONDS !== 0) return rawOnly;

  const state = await getRollupState();
  if (state.safeBefore === null) return rawOnly;

  const since = Date.parse(filters.since);
  const until = Date.parse(filters.until);
  if (Number.isNaN(since) || Number.isNaN(until)) return rawOnly;

  // The published boundary assumes logs are stamped near the time they are
  // sent. A deliberately backdated entry breaks that assumption: it can land
  // under a boundary the refresh has already moved past, and would then be
  // absent from the rollup until the next fold. So the boundary is pulled down
  // to the oldest timestamp this process has recently accepted, which puts that
  // entry's minute back on the raw side where it is counted. In the steady
  // state the floor is newer than the published boundary and this is a no-op.
  const boundary = Math.min(state.safeBefore.getTime(), floorMinute(acceptedFloorMs()));

  // The rollup can serve [rollupFrom, rollupUntil): inside the request, below
  // the refresh boundary, and on whole-minute edges at both ends.
  const rollupFrom = ceilMinute(since, filters.since);
  const rollupUntil = floorMinute(Math.min(until, boundary));

  if (rollupFrom >= rollupUntil) return rawOnly;

  const raw: TimeRange[] = [];
  // Leading partial minute: the request started mid-bucket.
  if (since < rollupFrom) raw.push({ from: filters.since, until: isoUtc(rollupFrom) });
  // The hot tail, plus any trailing partial minute.
  if (rollupUntil < until) raw.push({ from: isoUtc(rollupUntil), until: filters.until });

  return {
    path: raw.length === 0 ? 'rollup' : 'hybrid',
    rollup: { from: isoUtc(rollupFrom), until: isoUtc(rollupUntil) },
    raw,
  };
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
  ranges: TimeRange[]
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
  const result = await db.execute<AggregateBucket>(sql`
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
  `);

  return result.rows;
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
  range: TimeRange
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
  const result = await db.execute<AggregateBucket>(sql`
    SELECT to_char(bucket AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS start,
           grp AS group,
           entries AS count
    FROM (
      SELECT ${bucketStart} AS bucket,
             ${groupColumn} AS grp,
             sum(entry_count)::bigint AS entries
      FROM log_rollups
      WHERE ${whereClause}
      GROUP BY 1, 2
    ) aggregated
  `);

  return result.rows;
}

/** Adds up counts for the same (bucket, group) across sources. */
function mergeBuckets(sources: AggregateBucket[][]): AggregateBucket[] {
  const merged = new Map<string, AggregateBucket>();

  for (const rows of sources) {
    for (const row of rows) {
      //   cannot appear in a service name or a level: validation strips it
      // before anything is stored, so it is safe as a key separator.
      const key = `${row.start} ${row.group ?? ''}`;
      const existing = merged.get(key);
      if (existing === undefined) merged.set(key, { ...row });
      else existing.count += row.count;
    }
  }

  return [...merged.values()];
}

const MINUTE_MS = 60_000;

/** Sub-millisecond digits, which Date.parse silently drops but Postgres keeps. */
const SUB_MS_PRECISION = /\.\d{4,}/;

function floorMinute(ms: number): number {
  if (!Number.isFinite(ms)) return ms;
  return Math.floor(ms / MINUTE_MS) * MINUTE_MS;
}

/**
 * Smallest whole minute at or after `ms`.
 *
 * Rounding *up* is what keeps the rollup honest at the start of a range: a
 * request beginning at 10:00:30 must not be served the rollup row for 10:00,
 * which also counts the first thirty seconds. The raw path covers the remainder.
 *
 * `raw` is consulted because Date.parse truncates to milliseconds. A timestamp
 * of 10:00:00.000400Z parses to an exact minute, and treating it as one would
 * pull in 400 microseconds of rows the caller excluded — so anything carrying
 * finer precision is rounded up to the next minute regardless.
 */
function ceilMinute(ms: number, raw: string): number {
  const floored = floorMinute(ms);
  if (floored === ms && !SUB_MS_PRECISION.test(raw)) return ms;
  return floored + MINUTE_MS;
}

function isoUtc(ms: number): string {
  return new Date(ms).toISOString();
}

/** Neutralises LIKE wildcards so `q` stays a literal substring match. */
function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, (char) => `\\${char}`);
}
