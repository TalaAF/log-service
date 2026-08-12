import { sql } from 'drizzle-orm';
import { db } from '../db/client.js';
import { logs, type NewLogEntry } from '../db/schema.js';

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

/**
 * Fetches one page of logs ordered by (timestamp DESC, id DESC).
 *
 * Every filter is appended to a single WHERE clause built with the `sql`
 * template tag, so values are bound as parameters rather than interpolated.
 * Pagination is keyset-based off the composite (timestamp DESC, id DESC)
 * index — no OFFSET, so page cost stays flat however deep the caller walks.
 */
export async function queryLogs(filters: QueryLogsFilters): Promise<QueriedLog[]> {
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
      conditions.push(sql`attributes ->> ${key} = ${value}`);
    }
  }

  if (filters.q !== undefined) {
    conditions.push(sql`message ILIKE ${'%' + escapeLikePattern(filters.q) + '%'}`);
  }

  if (filters.cursor !== undefined) {
    const { timestamp, id } = filters.cursor;
    conditions.push(
      sql`("timestamp" < ${timestamp}::timestamptz OR ("timestamp" = ${timestamp}::timestamptz AND id < ${id}::bigint))`
    );
  }

  const whereClause = sql.join(conditions, sql` AND `);

  // Ordering and LIMIT happen in the inner query, on the native bigint/timestamptz
  // columns, so the (timestamp DESC, id DESC) index supplies rows already sorted.
  // The outer SELECT only reshapes the page for JSON (bigint -> text so large ids
  // survive, timestamp -> ISO 8601) and must not re-sort: ordering by the text
  // forms would put '9' after '10'.
  const result = await db.execute<QueriedLog>(sql`
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
      LIMIT ${filters.limit}
    ) page
  `);

  return result.rows;
}

/** Neutralises LIKE wildcards so `q` stays a literal substring match. */
function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, (char) => `\\${char}`);
}
