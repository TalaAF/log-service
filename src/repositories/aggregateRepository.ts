import { sql, type SQL } from 'drizzle-orm';
import { db } from '../db/client.js';

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
 * date_trunc() only understands named units, so bucket starts are computed on
 * the epoch instead: floor(epoch / width) * width, back through to_timestamp().
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
      conditions.push(sql`attributes ->> ${key} = ${value}`);
    }
  }

  if (filters.q !== undefined) {
    conditions.push(sql`message ILIKE ${'%' + escapeLikePattern(filters.q) + '%'}`);
  }

  const whereClause = sql.join(conditions, sql` AND `);

  const bucketStart = sql`to_timestamp(floor(extract(epoch FROM "timestamp") / ${width}) * ${width})`;

  // group_by names a column, which cannot be a bind parameter. Only the two
  // whitelisted keys above ever reach sql.raw, so the identifier is never
  // attacker-controlled; the NULL branch keeps one query shape for both cases.
  const groupColumn: SQL =
    filters.groupBy !== undefined
      ? sql.raw(GROUP_BY_COLUMNS[filters.groupBy])
      : sql`NULL::text`;

  const result = await db.execute<AggregateBucket>(sql`
    SELECT to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS start,
           grp AS group,
           entries AS count
    FROM (
      SELECT ${bucketStart} AS bucket_start,
             ${groupColumn} AS grp,
             count(*)::int AS entries
      FROM logs
      WHERE ${whereClause}
      GROUP BY 1, 2
      ORDER BY 1 ASC, 2 ASC
    ) aggregated
  `);

  return result.rows;
}

/** Neutralises LIKE wildcards so `q` stays a literal substring match. */
function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, (char) => `\\${char}`);
}
