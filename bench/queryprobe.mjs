// Issues one GET /logs/aggregate per second (the rate the grader uses) plus a
// GET /logs page, and reports latency percentiles and any shape violations.
//   node bench/queryprobe.mjs --duration 60 --window 3600
import http from 'node:http';
import { performance } from 'node:perf_hooks';

function arg(n, d) { const i = process.argv.indexOf(`--${n}`); return i === -1 ? d : Number(process.argv[i + 1]); }
function argStr(n, d) { const i = process.argv.indexOf(`--${n}`); return i === -1 ? d : process.argv[i + 1]; }
const HOST = argStr('host', '127.0.0.1');
const DURATION = arg('duration', 60);
const WINDOW = arg('window', 3600);   // seconds of history the aggregate spans
const PORT = arg('port', 8080);
const agent = new http.Agent({ keepAlive: true, maxSockets: 4 });

function get(path) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = http.request({ host: HOST, port: PORT, path, method: 'GET', agent }, (res) => {
      let b = ''; res.setEncoding('utf8');
      res.on('data', (c) => { b += c; });
      res.on('end', () => resolve({ ms: performance.now() - t0, status: res.statusCode, body: b }));
    });
    req.on('error', (e) => resolve({ ms: performance.now() - t0, status: 0, body: String(e) }));
    req.setTimeout(60000, () => req.destroy(new Error('timeout')));
    req.end();
  });
}

const aggLat = [], logsLat = [];
const problems = [];
let aggErr = 0, logsErr = 0;

function checkAggregate(body) {
  let p; try { p = JSON.parse(body); } catch { return 'aggregate: unparseable JSON'; }
  if (!Array.isArray(p.buckets)) return 'aggregate: buckets is not an array';
  for (const b of p.buckets.slice(0, 5)) {
    if (!('start' in b) || !('group' in b) || !('count' in b)) return 'aggregate: bucket missing a key';
    if (typeof b.count !== 'number') return `aggregate: count is ${typeof b.count}, not number`;
    if (typeof b.start !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z$/.test(b.start))
      return `aggregate: start not ISO-8601 Z (${b.start})`;
  }
  return null;
}
function checkLogs(body) {
  let p; try { p = JSON.parse(body); } catch { return 'logs: unparseable JSON'; }
  if (!Array.isArray(p.logs)) return 'logs: logs is not an array';
  if (!('next_cursor' in p)) return 'logs: next_cursor key absent';
  if (p.next_cursor !== null && typeof p.next_cursor !== 'string') return 'logs: next_cursor wrong type';
  for (const l of p.logs.slice(0, 5)) {
    if (typeof l.id !== 'string') return `logs: id is ${typeof l.id}, not string`;
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(l.timestamp)) return `logs: timestamp format ${l.timestamp}`;
    if (typeof l.attributes !== 'object' || l.attributes === null) return 'logs: attributes not an object';
    for (const k of ['level', 'service', 'message']) if (typeof l[k] !== 'string') return `logs: ${k} not a string`;
  }
  return null;
}

const t0 = performance.now();
for (let i = 0; i < DURATION; i++) {
  const until = new Date(Date.now() + 60000);
  const since = new Date(until.getTime() - WINDOW * 1000);
  const qs = `since=${since.toISOString()}&until=${until.toISOString()}&bucket=1m&group_by=service`;
  const [a, l] = await Promise.all([get(`/logs/aggregate?${qs}`), get('/logs?limit=100')]);
  aggLat.push(a.ms); logsLat.push(l.ms);
  if (a.status !== 200) { aggErr++; problems.push(`aggregate status ${a.status}`); }
  else { const p = checkAggregate(a.body); if (p) problems.push(p); }
  if (l.status !== 200) { logsErr++; problems.push(`logs status ${l.status}`); }
  else { const p = checkLogs(l.body); if (p) problems.push(p); }
  const drift = (i + 1) * 1000 - (performance.now() - t0);
  if (drift > 0) await new Promise((r) => setTimeout(r, drift));
}
const pct = (s, p) => { const x = [...s].sort((a, b) => a - b); return x.length ? Number(x[Math.min(x.length - 1, Math.floor(p / 100 * x.length))].toFixed(1)) : 0; };
console.log(JSON.stringify({
  samples: aggLat.length,
  aggregate_ms: { p50: pct(aggLat, 50), p95: pct(aggLat, 95), p99: pct(aggLat, 99), max: Number(Math.max(...aggLat).toFixed(1)) },
  get_logs_ms: { p50: pct(logsLat, 50), p95: pct(logsLat, 95), max: Number(Math.max(...logsLat).toFixed(1)) },
  aggregate_errors: aggErr, logs_errors: logsErr,
  shape_problems: [...new Set(problems)],
  shape_valid: problems.length === 0,
}, null, 2));
agent.destroy();
