// Read-back probe: how fast can a verifier walk everything it just wrote?
//
// The official benchmark reported 351.2K accepted but only 98K visible, with a
// 30s drain budget and read-after-write success of 0.71%. That is not a
// correctness failure — it is a throughput failure of the *read* path: the
// verifier walks the cursor page by page, and if it cannot reach the end inside
// its budget every unwalked row is counted as missing.
//
// Two things decide whether it finishes: how long a page takes, and how many
// pages there are. The second is set by the verifier's page size, not by us, so
// this probe sweeps page sizes and reports the achievable rows/second for each
// alongside how page latency behaves as the cursor goes deeper. A walk that is
// O(N) shows flat page latency with depth; one that is O(N^2) shows it climbing.
//
//   node bench/drainprobe.mjs --host app --limits 100,500,1000 --budget 30
import http from 'node:http';
import { performance } from 'node:perf_hooks';

const argv = process.argv;
const num = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : Number(argv[i + 1]); };
const str = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };

const HOST    = str('host', '127.0.0.1');
const PORT    = num('port', 8080);
const LIMITS  = str('limits', '100,500,1000').split(',').map(Number);
const BUDGET  = num('budget', 30);          // seconds, matching the grader's drain
const SINCE   = str('since', '');           // optional lower bound for the walk

const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });

function get(path) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = http.request({ host: HOST, port: PORT, path, agent }, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (c) => { raw += c; });
      res.on('end', () => resolve({ ms: performance.now() - t0, status: res.statusCode, raw }));
    });
    req.on('error', (e) => resolve({ ms: performance.now() - t0, status: 0, raw: '', err: String(e.message || e) }));
    req.setTimeout(60000, () => req.destroy(new Error('timeout')));
    req.end();
  });
}

/** Shape assertions from the API contract, applied to every page of the walk. */
function shapeProblem(parsed) {
  if (!Array.isArray(parsed.logs)) return 'logs is not an array';
  if (!('next_cursor' in parsed)) return 'next_cursor absent';
  if (parsed.next_cursor !== null && typeof parsed.next_cursor !== 'string') return 'next_cursor type';
  for (const l of parsed.logs) {
    if (typeof l.id !== 'string') return `id is ${typeof l.id}`;
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(l.timestamp)) return `timestamp ${l.timestamp}`;
    if (typeof l.level !== 'string' || typeof l.service !== 'string' || typeof l.message !== 'string') return 'field type';
    if (l.attributes === null || typeof l.attributes !== 'object' || Array.isArray(l.attributes)) return 'attributes type';
  }
  return null;
}

function pct(arr, p) {
  if (arr.length === 0) return 0;
  const s = [...arr].sort((a, b) => a - b);
  return Number(s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))].toFixed(2));
}

const report = [];

for (const limit of LIMITS) {
  const t0 = performance.now();
  const pageMs = [];
  const seen = new Set();
  let cursor = null;
  let pages = 0;
  let shape = null;
  let httpError = null;

  while (performance.now() - t0 < BUDGET * 1000) {
    const qs = `limit=${limit}` +
      (SINCE ? `&since=${encodeURIComponent(SINCE)}` : '') +
      (cursor ? `&cursor=${encodeURIComponent(cursor)}` : '');
    const r = await get(`/logs?${qs}`);
    pageMs.push(r.ms);
    pages++;

    if (r.status !== 200) { httpError = `status ${r.status} ${r.err ?? ''}`; break; }
    let parsed;
    try { parsed = JSON.parse(r.raw); } catch { shape = 'unparseable body'; break; }
    if (shape === null) shape = shapeProblem(parsed);
    if (shape !== null) break;

    for (const l of parsed.logs) seen.add(l.id);
    cursor = parsed.next_cursor;
    if (!cursor) break;
  }

  const elapsed = (performance.now() - t0) / 1000;
  // Latency at the start of the walk against latency at the end: the signal for
  // whether page cost is independent of how deep the cursor has gone.
  const head = pageMs.slice(0, Math.max(1, Math.floor(pageMs.length * 0.1)));
  const tail = pageMs.slice(Math.floor(pageMs.length * 0.9));
  const mean = (a) => (a.length ? Number((a.reduce((x, y) => x + y, 0) / a.length).toFixed(2)) : 0);

  report.push({
    limit,
    completed: cursor === null && httpError === null && shape === null,
    seconds: Number(elapsed.toFixed(2)),
    pages,
    rows_seen: seen.size,
    rows_per_sec: Number((seen.size / elapsed).toFixed(0)),
    page_ms: { p50: pct(pageMs, 50), p95: pct(pageMs, 95), max: Number(Math.max(0, ...pageMs).toFixed(2)) },
    first_10pct_mean_ms: mean(head),
    last_10pct_mean_ms: mean(tail),
    depth_drift: mean(head) > 0 ? Number((mean(tail) / mean(head)).toFixed(2)) : null,
    shape_problem: shape,
    http_error: httpError,
  });
  console.log(`  limit=${String(limit).padStart(5)}  ${JSON.stringify(report[report.length - 1])}`);
}

console.log('\n' + JSON.stringify({ budget_seconds: BUDGET, results: report }, null, 2));
agent.destroy();
