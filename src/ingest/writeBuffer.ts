import type { NewLogEntry } from '../db/schema.js';
import { copyLogs } from '../repositories/logsRepository.js';

/**
 * Group-commit buffer for the ingest path.
 *
 * Before: one POST /logs meant one INSERT, one transaction, one commit, one WAL
 * flush and one pass of index maintenance — for a batch of ~30 rows. At the
 * offered rate that is hundreds of tiny transactions per second, and Postgres
 * spends its single CPU on per-transaction overhead rather than on rows.
 *
 * Now: accepted entries land in a shared buffer and a background flusher drains
 * it in large consolidated writes, so thousands of small transactions collapse
 * into a handful of big ones.
 *
 * The handler still awaits its own rows: `enqueue` resolves only after the
 * COMMIT that contains them returns, so a 200 continues to mean "Postgres has
 * accepted this data", never "it is sitting in a process that might die". A
 * failed flush rejects every waiter in it, and the route surfaces a 5xx.
 */

function intFromEnv(name: string, fallback: number): number {
  const parsed = Number(process.env[name]);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

/** Longest a row may sit in memory before it is written. */
const FLUSH_INTERVAL_MS = intFromEnv('FLUSH_INTERVAL_MS', 40);

/** Flush early once the buffer reaches this many rows. */
const FLUSH_MAX_ROWS = intFromEnv('FLUSH_MAX_ROWS', 8000);

/**
 * Flushes allowed to be in flight at once. Postgres has a single CPU, so this
 * exists to keep the write path pipelined (one batch being built while another
 * commits), not to parallelise it.
 */
const MAX_CONCURRENT_FLUSHES = intFromEnv('FLUSH_CONCURRENCY', 3);

interface Waiter {
  resolve: () => void;
  reject: (err: Error) => void;
}

let buffer: NewLogEntry[] = [];
let waiters: Waiter[] = [];
let timer: NodeJS.Timeout | null = null;
let inFlight = 0;
let draining = false;

const stats = {
  flushes: 0,
  rowsWritten: 0,
  failedFlushes: 0,
  retriedFlushes: 0,
  maxBatch: 0,
};

/**
 * Buffers a validated batch and resolves once it has been committed.
 * Rejects if the flush containing it could not be written.
 */
export function enqueue(entries: NewLogEntry[]): Promise<void> {
  if (entries.length === 0) return Promise.resolve();

  return new Promise<void>((resolve, reject) => {
    for (const entry of entries) buffer.push(entry);
    waiters.push({ resolve, reject });

    if (buffer.length >= FLUSH_MAX_ROWS) {
      void flush();
      return;
    }
    // The timer is armed by whoever finds the buffer empty, so a row waits at
    // most FLUSH_INTERVAL_MS rather than restarting the clock on every arrival.
    if (timer === null) {
      timer = setTimeout(() => {
        timer = null;
        void flush();
      }, FLUSH_INTERVAL_MS);
      timer.unref();
    }
  });
}

/** Takes whatever is buffered and writes it. Safe to call at any time. */
async function flush(): Promise<void> {
  if (buffer.length === 0) return;

  // Nothing to gain from a fourth concurrent write on a one-CPU server; leave
  // the rows buffered and let the in-flight flush's completion pick them up.
  if (inFlight >= MAX_CONCURRENT_FLUSHES) return;

  if (timer !== null) {
    clearTimeout(timer);
    timer = null;
  }

  const rows = buffer;
  const batchWaiters = waiters;
  buffer = [];
  waiters = [];
  inFlight++;

  try {
    await writeWithRetry(rows);
    stats.flushes++;
    stats.rowsWritten += rows.length;
    if (rows.length > stats.maxBatch) stats.maxBatch = rows.length;
    for (const w of batchWaiters) w.resolve();
  } catch (err) {
    stats.failedFlushes++;
    const error = err instanceof Error ? err : new Error(String(err));
    for (const w of batchWaiters) w.reject(error);
  } finally {
    inFlight--;
    // Rows that arrived while this flush ran, or that a busy flusher declined
    // to take, must not wait for the next enqueue to be noticed.
    if (buffer.length > 0 && !draining) {
      if (buffer.length >= FLUSH_MAX_ROWS) void flush();
      else if (timer === null) {
        timer = setTimeout(() => {
          timer = null;
          void flush();
        }, FLUSH_INTERVAL_MS);
        timer.unref();
      }
    }
  }
}

/**
 * One retry on a fresh connection. A flush carries thousands of rows from many
 * different clients, so a transient connection-level failure would otherwise
 * turn into a large number of 5xx responses for data that was perfectly valid.
 */
async function writeWithRetry(rows: NewLogEntry[]): Promise<void> {
  try {
    await copyLogs(rows);
  } catch (err) {
    stats.retriedFlushes++;
    await copyLogs(rows);
    void err;
  }
}

/** Writes everything still buffered. Used on shutdown so nothing is lost. */
export async function drain(): Promise<void> {
  draining = true;
  if (timer !== null) {
    clearTimeout(timer);
    timer = null;
  }
  while (buffer.length > 0 || inFlight > 0) {
    if (buffer.length > 0 && inFlight < MAX_CONCURRENT_FLUSHES) await flush();
    else await new Promise((r) => setTimeout(r, 5));
  }
  draining = false;
}

export function bufferStats() {
  return { ...stats, pendingRows: buffer.length, inFlightFlushes: inFlight };
}
