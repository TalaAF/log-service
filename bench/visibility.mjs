// Read-after-write check: POST a uniquely marked batch, then poll GET /logs
// until every record is visible, and report how long that took.
// Run it while ingestion load is in flight — that is the condition the grader
// measures under.
//   node bench/visibility.mjs --rounds 10 --host app
import http from 'node:http';
import { performance } from 'node:perf_hooks';

function arg(n, d) { const i = process.argv.indexOf(`--${n}`); return i === -1 ? d : Number(process.argv[i + 1]); }
function argStr(n, d) { const i = process.argv.indexOf(`--${n}`); return i === -1 ? d : process.argv[i + 1]; }

const ROUNDS = arg('rounds', 10);
const BATCH = arg('batch', 20);
const GAP_MS = arg('gap', 3000);
const DEADLINE_MS = arg('deadline', 20000);   // the contract's visibility budget
const HOST = argStr('host', '127.0.0.1');
const PORT = arg('port', 8080);

const agent = new http.Agent({ keepAlive: true, maxSockets: 4 });

function call(method, path, body) {
  const payload = body === undefined ? null : Buffer.from(JSON.stringify(body));
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host: HOST, port: PORT, path, method, agent,
        headers: payload ? { 'content-type': 'application/json', 'content-length': payload.length } : {} },
      (res) => {
        let raw = ''; res.setEncoding('utf8');
        res.on('data', (c) => { raw += c; });
        res.on('end', () => {
          let json = null; try { json = JSON.parse(raw); } catch { /* left null */ }
          resolve({ status: res.statusCode, json });
        });
      });
    req.on('error', reject);
    req.setTimeout(30000, () => req.destroy(new Error('timeout')));
    req.end(payload);
  });
}

const results = [];

for (let round = 0; round < ROUNDS; round++) {
  const marker = `vis-${Date.now()}-${round}`;
  const logs = Array.from({ length: BATCH }, (_, i) => ({
    timestamp: new Date().toISOString(),
    level: 'warn',
    service: 'visibility-probe',
    message: `read-after-write probe ${marker} #${i}`,
    attributes: { marker, seq: i },
  }));

  const postedAt = performance.now();
  const post = await call('POST', '/logs', { logs });
  const ackMs = performance.now() - postedAt;

  if (post.status !== 200 || post.json?.accepted !== BATCH) {
    results.push({ round, ok: false, detail: `POST status ${post.status} accepted ${post.json?.accepted}` });
    continue;
  }

  let visibleMs = null;
  let seen = 0;
  while (performance.now() - postedAt < DEADLINE_MS) {
    const r = await call('GET', `/logs?attr.marker=${marker}&limit=1000`);
    seen = r.json?.logs?.length ?? 0;
    if (seen >= BATCH) { visibleMs = performance.now() - postedAt; break; }
    await new Promise((res) => setTimeout(res, 100));
  }

  results.push({
    round,
    ok: visibleMs !== null,
    ack_ms: Number(ackMs.toFixed(1)),
    visible_after_ms: visibleMs === null ? null : Number(visibleMs.toFixed(1)),
    seen,
  });

  await new Promise((res) => setTimeout(res, GAP_MS));
}

const ok = results.filter((r) => r.ok);
const visible = ok.map((r) => r.visible_after_ms);
console.log(JSON.stringify({
  rounds: ROUNDS,
  all_visible_within_deadline: ok.length === ROUNDS,
  deadline_ms: DEADLINE_MS,
  visible_after_ms: {
    min: visible.length ? Math.min(...visible) : null,
    max: visible.length ? Math.max(...visible) : null,
    mean: visible.length ? Number((visible.reduce((a, b) => a + b, 0) / visible.length).toFixed(1)) : null,
  },
  ack_ms_max: ok.length ? Math.max(...ok.map((r) => r.ack_ms)) : null,
  per_round: results,
}, null, 2));
agent.destroy();
process.exit(ok.length === ROUNDS ? 0 : 1);
