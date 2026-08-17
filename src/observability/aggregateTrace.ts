/**
 * Sampled, bounded aggregate traces used to decompose one request end-to-end.
 *
 * Aggregate traffic is only about one request per second in the benchmark, but
 * the reservoir is still fixed so diagnostics can never grow with uptime. No
 * trace is logged on the hot ingest path.
 */

export type AggregateTracePath = 'rollup' | 'raw' | 'hybrid';
export type AggregateSqlSource = 'state' | 'rollup' | 'raw' | 'hybrid';

export interface AggregateSqlTrace {
  source: AggregateSqlSource;
  range: { from: string; until: string } | null;
  connectionRequestedAt: number;
  connectionAcquiredAt?: number;
  sqlStartedAt?: number;
  sqlFinishedAt?: number;
  returnedRows?: number;
  sourceRowsTouched?: number;
}

export interface AggregateRequestTrace {
  id: number;
  epochMs: number;
  origin: number;
  url: string;
  requestedSince: string | null;
  requestedUntil: string | null;
  resolvedSince: string | null;
  resolvedUntil: string | null;
  bucket: string | null;
  groupBy: string | null;
  validationError?: string;
  path?: AggregateTracePath;
  safeBefore?: string | null;
  ingestFloor?: string | null;
  rollupWatermark?: string;
  oldestRollupBucket?: string | null;
  newestRollupBucket?: string | null;
  rollupRange?: { from: string; until: string } | null;
  rawRanges?: Array<{ from: string; until: string; minId?: string }>;
  rawTailStart?: string | null;
  rawTailEnd?: string | null;
  rawRowsTouched: number;
  rollupRowsTouched: number;
  marks: Partial<Record<AggregateMark, number>>;
  sql: AggregateSqlTrace[];
}

export type AggregateMark =
  | 'requestReceived'
  | 'admissionQueueEntered'
  | 'admissionSlotAcquired'
  | 'mergeFinished'
  | 'responseSent';

const sampleEveryRaw = Number(process.env.AGGREGATE_TRACE_SAMPLE_EVERY ?? '1');
const SAMPLE_EVERY = Number.isInteger(sampleEveryRaw) && sampleEveryRaw > 0 ? sampleEveryRaw : 0;
const TRACE_CAP = 128;
const completed: AggregateRequestTrace[] = [];
let requests = 0;
let nextId = 1;

export function beginAggregateTrace(
  url: string,
  requested: { since?: string; until?: string; bucket?: string; groupBy?: string }
): AggregateRequestTrace | null {
  requests++;
  if (SAMPLE_EVERY === 0 || (requests - 1) % SAMPLE_EVERY !== 0) return null;

  const origin = performance.now();
  return {
    id: nextId++,
    epochMs: Date.now(),
    origin,
    url,
    requestedSince: requested.since ?? null,
    requestedUntil: requested.until ?? null,
    resolvedSince: null,
    resolvedUntil: null,
    bucket: requested.bucket ?? null,
    groupBy: requested.groupBy ?? null,
    rawRowsTouched: 0,
    rollupRowsTouched: 0,
    marks: { requestReceived: origin },
    sql: [],
  };
}

export function markAggregateTrace(trace: AggregateRequestTrace | null, mark: AggregateMark): void {
  if (trace !== null) trace.marks[mark] = performance.now();
}

export function finishAggregateTrace(trace: AggregateRequestTrace | null): void {
  if (trace === null) return;
  markAggregateTrace(trace, 'responseSent');
  if (completed.length >= TRACE_CAP) completed.shift();
  completed.push(trace);
}

export function beginAggregateSql(
  trace: AggregateRequestTrace | null,
  source: AggregateSqlSource,
  range: { from: string; until: string } | null
): AggregateSqlTrace | null {
  if (trace === null) return null;
  const part: AggregateSqlTrace = {
    source,
    range: range === null ? null : { ...range },
    connectionRequestedAt: performance.now(),
  };
  trace.sql.push(part);
  return part;
}

export function aggregateTraceSnapshot(): unknown[] {
  return completed.map(renderTrace);
}

function renderTrace(trace: AggregateRequestTrace) {
  const allSql = trace.sql;
  const firstConnection = allSql[0];
  const started = allSql.flatMap((part) => (part.sqlStartedAt === undefined ? [] : [part.sqlStartedAt]));
  const finished = allSql.flatMap((part) => (part.sqlFinishedAt === undefined ? [] : [part.sqlFinishedAt]));
  const firstSqlStarted = started.length === 0 ? undefined : Math.min(...started);
  const lastSqlFinished = finished.length === 0 ? undefined : Math.max(...finished);
  // The two raw edge queries in a hybrid plan run concurrently. Unioning their
  // intervals keeps this a wall-clock decomposition instead of double-counting
  // overlapping connection waits or execution.
  const connectionWaitMs = unionDuration(
    allSql.flatMap((part) =>
      part.connectionAcquiredAt === undefined
        ? []
        : [[part.connectionRequestedAt, part.connectionAcquiredAt] as const]
    )
  );
  const sqlExecutionMs = unionDuration(
    allSql.flatMap((part) =>
      part.sqlStartedAt === undefined || part.sqlFinishedAt === undefined
        ? []
        : [[part.sqlStartedAt, part.sqlFinishedAt] as const]
    )
  );

  return {
    id: trace.id,
    url: trace.url,
    request_received_at: at(trace, trace.marks.requestReceived),
    admission_queue_entered_at: at(trace, trace.marks.admissionQueueEntered),
    admission_slot_acquired_at: at(trace, trace.marks.admissionSlotAcquired),
    db_connection_requested_at: at(trace, firstConnection?.connectionRequestedAt),
    db_connection_acquired_at: at(trace, firstConnection?.connectionAcquiredAt),
    sql_started_at: at(trace, firstSqlStarted),
    sql_finished_at: at(trace, lastSqlFinished),
    merge_finished_at: at(trace, trace.marks.mergeFinished),
    response_sent_at: at(trace, trace.marks.responseSent),
    queue_wait_ms: duration(trace.marks.admissionQueueEntered, trace.marks.admissionSlotAcquired),
    connection_wait_ms: rounded(connectionWaitMs),
    sql_execution_ms: rounded(sqlExecutionMs),
    merge_ms: duration(lastSqlFinished, trace.marks.mergeFinished),
    total_ms: duration(trace.marks.requestReceived, trace.marks.responseSent),
    validation_error: trace.validationError ?? null,
    selected_route: trace.path ?? null,
    requested_since: trace.requestedSince,
    requested_until: trace.requestedUntil,
    resolved_since: trace.resolvedSince,
    resolved_until: trace.resolvedUntil,
    bucket: trace.bucket,
    group_by: trace.groupBy,
    safe_before: trace.safeBefore ?? null,
    ingest_floor: trace.ingestFloor ?? null,
    rollup_watermark: trace.rollupWatermark ?? null,
    oldest_rollup_bucket: trace.oldestRollupBucket ?? null,
    newest_rollup_bucket: trace.newestRollupBucket ?? null,
    rollup_range: trace.rollupRange ?? null,
    raw_ranges: trace.rawRanges ?? [],
    raw_tail_start: trace.rawTailStart ?? null,
    raw_tail_end: trace.rawTailEnd ?? null,
    raw_rows_touched: trace.rawRowsTouched,
    rollup_rows_touched: trace.rollupRowsTouched,
    sql_parts: allSql.map((part) => ({
      source: part.source,
      range: part.range,
      connection_requested_at: at(trace, part.connectionRequestedAt),
      connection_acquired_at: at(trace, part.connectionAcquiredAt),
      connection_wait_ms: duration(part.connectionRequestedAt, part.connectionAcquiredAt),
      sql_started_at: at(trace, part.sqlStartedAt),
      sql_finished_at: at(trace, part.sqlFinishedAt),
      sql_execution_ms: duration(part.sqlStartedAt, part.sqlFinishedAt),
      returned_rows: part.returnedRows ?? null,
      source_rows_touched: part.sourceRowsTouched ?? null,
    })),
  };
}

function at(trace: AggregateRequestTrace, point: number | undefined): string | null {
  if (point === undefined) return null;
  return new Date(trace.epochMs + point - trace.origin).toISOString();
}

function duration(from: number | undefined, until: number | undefined): number | null {
  if (from === undefined || until === undefined) return null;
  return rounded(until - from);
}

function rounded(value: number): number {
  return Number(value.toFixed(3));
}

function unionDuration(intervals: ReadonlyArray<readonly [number, number]>): number {
  if (intervals.length === 0) return 0;
  const ordered = [...intervals].sort((a, b) => a[0] - b[0]);
  let start = ordered[0][0];
  let end = ordered[0][1];
  let total = 0;

  for (const [nextStart, nextEnd] of ordered.slice(1)) {
    if (nextStart <= end) {
      end = Math.max(end, nextEnd);
      continue;
    }
    total += end - start;
    start = nextStart;
    end = nextEnd;
  }
  return total + end - start;
}
