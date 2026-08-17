// Continuous aggregate load, independent of the scenario harness.
//
// scenario.mjs stops all query traffic before it drains, so its eventual-
// consistency number is measured on an otherwise idle database. That hides
// exactly the question worth asking: whether read-back collapses when
// analytical queries are running alongside it. This keeps issuing aggregates for
// a fixed wall-clock duration so it spans the drain as well as the load phase.
//
//   node bench/agghammer.mjs --host app --seconds 200 --rate 4 --q declined
import http from 'node:http';
import { performance } from 'node:perf_hooks';

const a = process.argv;
const num = (n, d) => { const i = a.indexOf(`--${n}`); return i === -1 ? d : Number(a[i + 1]); };
const str = (n, d) => { const i = a.indexOf(`--${n}`); return i === -1 ? d : a[i + 1]; };
const HOST = str('host', 'app'), SECONDS = num('seconds', 200), RATE = num('rate', 4);
const Q = str('q', ''), WINDOW = num('window', 3600), CONC = num('conc', 4);

const agent = new http.Agent({ keepAlive: true, maxSockets: CONC + 4 });
const lat = []; let sent = 0, errors = 0, running = true;

function once() {
  const until = new Date(Date.now() + 60000).toISOString();
  const since = new Date(Date.now() + 60000 - WINDOW * 1000).toISOString();
  const q = Q ? `&q=${encodeURIComponent(Q)}` : '';
  const path = `/logs/aggregate?since=${since}&until=${until}&bucket=1m&group_by=service${q}`;
  return new Promise((r) => {
    const t0 = performance.now();
    const req = http.request({ host: HOST, port: 8080, path, agent }, (res) => {
      res.resume();
      res.on('end', () => { lat.push(performance.now() - t0); sent++; if (res.statusCode !== 200) errors++; r(); });
    });
    req.on('error', () => { errors++; r(); });
    req.setTimeout(60000, () => req.destroy(new Error('timeout')));
    req.end();
  });
}

// Rate-paced but capped by CONC in flight, so a slow server suppresses the rate
// rather than building an unbounded client-side backlog.
const t0 = performance.now();
let dispatched = 0;
async function worker() {
  while (running) {
    const elapsed = (performance.now() - t0) / 1000;
    if (Math.floor(elapsed * RATE) > dispatched) { dispatched++; await once(); }
    else await new Promise((r) => setTimeout(r, 5));
  }
}
const ws = Array.from({ length: CONC }, worker);
setTimeout(() => { running = false; }, SECONDS * 1000);
await Promise.allSettled(ws);

const s = lat.sort((x, y) => x - y);
const p = (q) => (s.length ? Number(s[Math.min(s.length - 1, Math.floor(q / 100 * s.length))].toFixed(1)) : 0);
console.log(JSON.stringify({ q: Q || 'simple', sent, errors, p50: p(50), p95: p(95), p99: p(99) }));
agent.destroy();
