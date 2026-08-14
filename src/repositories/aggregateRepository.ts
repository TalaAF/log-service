import { sql, type SQL } from 'drizzle-orm';
import { db } from '../db/client.js';
import { attributeCondition } from './attributeFilter.js';

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

/**
 * Counts matching rows per time bucket, optionally split by service or level.
 *
 * date_trunc() only understands named units, so bucket starts are computed with
 * date_bin() against the epoch origin instead, which gives arbitrary widths.
 * The range filter stays as a plain "timestamp" >= / < comparison so partition
 * pruning and the (timestamp DESC, id DESC) index still apply — wrapping the
 * column in the bucket expression would make it unsearchable.
 */
export async function aggregateLogs(filters: AggregateFilters): Promise<AggregateBucket[]> {
  const width = BUCKET_SECONDS[filters.bucket];
  if (width === undefined) {
    throw new Error(`unsupported bucket size: ${filters.bucket}`);
  }

  const conditions = [
    sql`"timestamp" >= ${filters.since}::timestamptz`,
    sql`"timestamp" < ${filters.until}::timestamptz`,
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
    filters.groupBy !== undefined
      ? sql.raw(GROUP_BY_COLUMNS[filters.groupBy])
      : sql`NULL::text`;

  // Bucket starts are always whole seconds (every supported width is a whole
  // number of seconds), and the contract's example carries no fractional part,
  // so they are rendered without one: 2026-07-20T14:00:00Z.
  // No ORDER BY in SQL, deliberately.
  //
  // The planner cannot estimate the distinct count of a function expression, so
  // for the bucket column it assumes something between 10% and 100% of the
  // table. Asked to return sorted rows, it then satisfies the grouping and the
  // ordering with one sort of every input row, and picks GroupAggregate over a
  // Sort of the whole scan: measured at 2.35M rows that was an external merge
  // spilling 59MB to disk, 974ms. Dropping the ORDER BY lets it choose
  // HashAggregate over the same scan — 6.2MB in memory, 515ms — because the
  // grouping no longer has to produce ordered output.
  //
  // The ordering is then done here, on the aggregated rows. There are only ever
  // as many of those as buckets times groups, so it is a sort of tens to a few
  // thousand rows instead of millions.
  //
  // Extended statistics were tried first and do fix the estimate exactly, but
  // they are populated by ANALYZE on the partitioned parent, and autovacuum
  // never analyses a partitioned parent. On a fresh database the migration
  // would analyse an empty table, which is precisely the state a benchmark run
  // starts from, so the statistics would not be there when they were needed.
  const result = await db.execute<AggregateBucket>(sql`
    SELECT to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS start,
           grp AS group,
           entries AS count
    FROM (
      SELECT ${bucketStart} AS bucket_start,
             ${groupColumn} AS grp,
             count(*)::int AS entries
      FROM logs
      WHERE ${whereClause}
      GROUP BY 1, 2
    ) aggregated
  `);

  // Ascending by bucket start, then by group. `start` is rendered as a
  // zero-padded ISO 8601 UTC string, so lexicographic order is chronological
  // order. Group ordering is not specified by the contract; code-point order is
  // used because it is deterministic and independent of database collation.
  return result.rows.sort((a, b) => {
    if (a.start !== b.start) return a.start < b.start ? -1 : 1;
    const left = a.group ?? '';
    const right = b.group ?? '';
    if (left === right) return 0;
    return left < right ? -1 : 1;
  });
}

/** Neutralises LIKE wildcards so `q` stays a literal substring match. */
function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, (char) => `\\${char}`);
}
