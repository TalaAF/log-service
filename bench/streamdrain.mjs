// Streaming read-back verifier.
//
// The old drain retained every id in a Set and only checked the walk at the end.
// Over ~1.6M wide rows that is a multi-hundred-megabyte structure built inside
// the same Node process driving the benchmark, and it made the read path look
// like it topped out at ~13-17k rows/s when the server independently measures
// ~38-40k. The instrument was the bottleneck.
//
// This verifies each page as it arrives and then drops it, so memory is O(page)
// rather than O(rows read), while still catching everything the old one did.
//
// Duplicate detection without a Set: the contract requires the walk to be
// strictly decreasing in (timestamp DESC, id DESC). In a strictly decreasing
// sequence every tuple is distinct, so a repeated row necessarily breaks strict
// decrease at the point it reappears — whether it repeats immediately or a
// million rows later, because the comparison is against the running minimum.
// Checking strict decrease is therefore sufficient for duplicates, and needs
// only the previous tuple.
//
//   node bench/streamdrain.mjs --host app --mode stream --limit 1000
//   node bench/streamdrain.mjs --host app --mode old    --limit 1000
import http from 'node:http';

const argv = process.argv;
const num = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : Number(argv[i + 1]); };
const str = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };

const HOST = str('host', '127.0.0.1');
const PORT = num('port', 8080);
const LIMIT = num('limit', 1000);
const MODE = str('mode', 'stream');            // stream | old
const BUDGET = num('budget', 30);              // seconds, the grader's drain budget
const SINCE = str('since', '');
const EXPECTED = num('expected', 0);
const SAMPLE_EVERY = num('sample-every', 200); // pages between memory samples

const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
const TS_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

function get(path) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = http.request({ host: HOST, port: PORT, path, agent }, (res) => {
      const chunks = [];
      let bytes = 0;
      res.on('data', (c) => { bytes += c.length; chunks.push(c); });
      res.on('end', () => resolve({
        ms: performance.now() - t0, status: res.statusCode, bytes,
        body: Buffer.concat(chunks).toString('utf8'),
      }));
    });
    req.on('error', (e) => resolve({ ms: performance.now() - t0, status: 0, bytes: 0, body: '', err: String(e.message || e) }));
    req.setTimeout(120000, () => req.destroy(new Error('timeout')));
    req.end();
  });
}

/** Strictly-decreasing comparison over (timestamp, id). Returns true if a < b. */
function strictlyBefore(aTs, aId, bTs, bId) {
  if (aTs !== bTs) return aTs < bTs;          // fixed-width ISO, so lexicographic is chronological
  return BigInt(aId) < BigInt(bId);           // ids can exceed 2^53, so compare as BigInt
}

const errors = { shape: 0, order: 0, duplicate: 0, cursor: 0, http: 0 };
const firstError = [];
const note = (kind, detail) => { errors[kind]++; if (firstError.length < 5) firstError.push(`${kind}: ${detail}`); };

// O(1) verification state: the previous tuple, counters, and cursor progress.
let prevTs = null, prevId = null;
let rowsSeen = 0, pages = 0, bytesTotal = 0;
let prevCursor = null, sameCursorRepeats = 0;
// Only used by --mode old, to reproduce the original behaviour for comparison.
const seenIds = MODE === 'old' ? new Set() : null;

const pageMs = [];
const mem = [];
const sampleMem = () => {
  const m = process.memoryUsage();
  mem.push({ pages, rss_mb: +(m.rss / 1048576).toFixed(1), heap_mb: +(m.heapUsed / 1048576).toFixed(1) });
};

function verifyPage(parsed, pageLen) {
  if (!Array.isArray(parsed.logs)) return note('shape', 'logs not an array');
  if (!('next_cursor' in parsed)) return note('shape', 'next_cursor absent');
  if (parsed.next_cursor !== null && typeof parsed.next_cursor !== 'string') return note('shape', 'next_cursor type');
  if (parsed.logs.length > pageLen) return note('shape', `page longer than limit (${parsed.logs.length})`);

  for (const l of parsed.logs) {
    if (typeof l.id !== 'string' || !/^\d+$/.test(l.id)) { note('shape', `id ${typeof l.id}`); return; }
    if (!TS_RE.test(l.timestamp)) { note('shape', `timestamp ${l.timestamp}`); return; }
    if (typeof l.level !== 'string' || typeof l.service !== 'string' || typeof l.message !== 'string') {
      note('shape', 'field type'); return;
    }
    if (l.attributes === null || typeof l.attributes !== 'object' || Array.isArray(l.attributes)) {
      note('shape', 'attributes type'); return;
    }

    if (prevTs !== null) {
      // Must be strictly before the previous row. Equality is a duplicate;
      // anything greater is an ordering violation.
      if (l.timestamp === prevTs && l.id === prevId) note('duplicate', `${l.timestamp}/${l.id}`);
      else if (!strictlyBefore(l.timestamp, l.id, prevTs, prevId)) note('order', `${l.timestamp}/${l.id} after ${prevTs}/${prevId}`);
    }
    prevTs = l.timestamp; prevId = l.id;

    if (seenIds !== null) {
      if (seenIds.has(l.id)) note('duplicate', l.id);
      seenIds.add(l.id);
    }
    rowsSeen++;
  }
}

const t0 = performance.now();
let cursor = null;
sampleMem();

while ((performance.now() - t0) / 1000 < BUDGET) {
  const qs = `limit=${LIMIT}` + (SINCE ? `&since=${encodeURIComponent(SINCE)}` : '') +
             (cursor ? `&cursor=${encodeURIComponent(cursor)}` : '');
  const r = await get(`/logs?${qs}`);
  pageMs.push(r.ms); pages++; bytesTotal += r.bytes;

  if (r.status !== 200) { note('http', `status ${r.status} ${r.err ?? ''}`); break; }
  let parsed;
  try { parsed = JSON.parse(r.body); } catch { note('shape', 'unparseable'); break; }

  verifyPage(parsed, LIMIT);

  // Cursor must advance; a repeated cursor means the walk cannot terminate.
  if (parsed.next_cursor !== null && parsed.next_cursor === prevCursor) {
    if (++sameCursorRepeats > 1) { note('cursor', 'cursor not advancing'); break; }
  } else sameCursorRepeats = 0;
  prevCursor = parsed.next_cursor;

  if (pages % SAMPLE_EVERY === 0) sampleMem();
  cursor = parsed.next_cursor;
  if (!cursor) break;
}

sampleMem();
const wall = (performance.now() - t0) / 1000;
const pct = (a, p) => (a.length ? Number([...a].sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(p / 100 * a.length))].toFixed(2)) : 0);

console.log(JSON.stringify({
  mode: MODE, limit: LIMIT, budget_s: BUDGET,
  completed: cursor === null,
  rows_seen: rowsSeen, expected: EXPECTED || null,
  missing: EXPECTED ? Math.max(0, EXPECTED - rowsSeen) : null,
  pages, drain_s: +wall.toFixed(2),
  rows_per_sec: Math.round(rowsSeen / wall),
  mb_per_sec: +(bytesTotal / wall / 1048576).toFixed(1),
  page_ms: { p50: pct(pageMs, 50), p95: pct(pageMs, 95) },
  errors,
  first_errors: firstError,
  memory: { start: mem[0], peak_rss_mb: Math.max(...mem.map((m) => m.rss_mb)), peak_heap_mb: Math.max(...mem.map((m) => m.heap_mb)), end: mem[mem.length - 1] },
  memory_samples: mem.filter((_, i) => i % Math.max(1, Math.ceil(mem.length / 6)) === 0 || i === mem.length - 1),
}, null, 1));

agent.destroy();
