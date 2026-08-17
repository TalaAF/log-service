// Aggregate shape matrix: what every supported query combination actually costs.
//
// The application decides rollup / hybrid / raw routing in aggregateRepository,
// but a request classified as "rollup" can still end up reading raw rows if the
// routing and the SQL disagree. So each shape is measured twice over: the path
// the application reports through /internal/stats, and what Postgres actually
// did, taken from pg_stat_statements deltas around the call. If those two ever
// disagree, the routing is lying and that alone would explain a Postgres-heavy
// benchmark.
//
//   node bench/aggmatrix.mjs                 # idle
//   node bench/aggmatrix.mjs --label under-load
import http from 'node:http';
import pg from 'pg';
import { performance } from 'node:perf_hooks';

const argv = process.argv;
const num = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : Number(argv[i + 1]); };
const str = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };

const PORT = num('port', 8080);
const PG = str('pg', 'postgres://loguser:logpass@localhost:55432/logs');
const LABEL = str('label', 'idle');
const REPEATS = num('repeats', 3);

const agent = new http.Agent({ keepAlive: true, maxSockets: 2 });
const client = new pg.Client({ connectionString: PG });
await client.connect();

function get(path) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = http.request({ host: '127.0.0.1', port: PORT, path, agent }, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (c) => { raw += c; });
      res.on('end', () => resolve({ ms: performance.now() - t0, status: res.statusCode, raw }));
    });
    req.on('error', (e) => resolve({ ms: performance.now() - t0, status: 0, raw: '', err: String(e) }));
    req.setTimeout(120000, () => req.destroy(new Error('timeout')));
    req.end();
  });
}

const paths = async () => (await (await get('/internal/stats')).raw ? JSON.parse((await get('/internal/stats')).raw).aggregate_path : null);

/** Statements that ran during `fn`, with the blocks and rows they touched. */
async function withStatementDelta(fn) {
  const snap = async () => {
    const { rows } = await client.query(`
      SELECT queryid::text AS id, calls, rows, shared_blks_hit AS hit, shared_blks_read AS rd,
             total_exec_time AS ms, query
      FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname='logs')`);
    return new Map(rows.map((r) => [r.id, r]));
  };
  const before = await snap();
  const result = await fn();
  const after = await snap();

  const touched = [];
  for (const [id, a] of after) {
    const b = before.get(id) ?? { calls: 0, rows: 0, hit: 0, rd: 0, ms: 0 };
    const calls = Number(a.calls) - Number(b.calls);
    if (calls <= 0) continue;
    // Ignore the probe's own bookkeeping.
    if (/pg_stat_statements|internal|rollup_state|pg_database/i.test(a.query)) continue;
    touched.push({
      calls,
      rows: Number(a.rows) - Number(b.rows),
      hit: Number(a.hit) - Number(b.hit),
      read: Number(a.rd) - Number(b.rd),
      ms: Number(a.ms) - Number(b.ms),
      reads_logs: /FROM logs/i.test(a.query),
      reads_rollups: /FROM log_rollups/i.test(a.query),
    });
  }
  return { result, touched };
}

const iso = (ms) => new Date(ms).toISOString();
const now = Date.now();

function qs({ bucket = '1m', groupBy, service, level, q, attr, rangeMs = 3600_000 }) {
  const parts = [
    `since=${encodeURIComponent(iso(now - rangeMs))}`,
    `until=${encodeURIComponent(iso(now + 60_000))}`,
    `bucket=${bucket}`,
  ];
  if (groupBy) parts.push(`group_by=${groupBy}`);
  if (service) parts.push(`service=${encodeURIComponent(service)}`);
  if (level) parts.push(`level=${level}`);
  if (q) parts.push(`q=${encodeURIComponent(q)}`);
  if (attr) for (const [k, v] of Object.entries(attr)) parts.push(`attr.${k}=${encodeURIComponent(v)}`);
  return `/logs/aggregate?${parts.join('&')}`;
}

const MIN = 60_000, HOUR = 3600_000, DAY = 86400_000;
const shapes = [];
// 1. bucket width x group_by, no filters — the routing check §14 asks for.
for (const bucket of ['1m', '5m', '1h', '1d'])
  for (const groupBy of [undefined, 'service', 'level'])
    shapes.push({ name: `bucket=${bucket} group=${groupBy ?? 'none'}`, spec: { bucket, groupBy } });
// 2. filter interactions §15.
for (const [name, extra] of [
  ['service', { service: 'checkout' }],
  ['level', { level: 'error' }],
  ['service+level', { service: 'checkout', level: 'error' }],
  ['q common', { q: 'declined' }],
  ['q selective', { q: '#31415' }],
  ['service+q', { service: 'checkout', q: 'declined' }],
  ['attr low-sel', { attr: { region: 'eu-west' } }],
  ['attr high-sel', { attr: { user_id: '4242' } }],
  ['attr missing key', { attr: { nosuchkey: 'x' } }],
  ['service+attr', { service: 'checkout', attr: { region: 'eu-west' } }],
]) shapes.push({ name: `filter ${name}`, spec: { bucket: '1m', groupBy: 'service', ...extra } });
// 3. time-range sensitivity §13.
for (const [name, rangeMs] of [['5m', 5 * MIN], ['1h', HOUR], ['6h', 6 * HOUR], ['24h', DAY], ['7d', 7 * DAY], ['30d', 30 * DAY]])
  shapes.push({ name: `range ${name} bucket=1m`, spec: { bucket: '1m', groupBy: 'service', rangeMs } });
for (const [name, rangeMs] of [['24h', DAY], ['7d', 7 * DAY], ['30d', 30 * DAY]])
  shapes.push({ name: `range ${name} bucket=1h`, spec: { bucket: '1h', groupBy: 'service', rangeMs } });

console.log(`[aggmatrix] label=${LABEL} shapes=${shapes.length} repeats=${REPEATS}\n`);
console.log(
  'shape'.padEnd(30) + 'app_path'.padEnd(9) + 'p50ms'.padStart(9) + 'maxms'.padStart(9) +
  'sql_rows'.padStart(10) + 'blks_hit'.padStart(10) + 'blks_rd'.padStart(9) + '  reads'
);

const results = [];
for (const { name, spec } of shapes) {
  const url = qs(spec);
  const times = [];
  let pathUsed = '?';
  let touchedAgg = { rows: 0, hit: 0, read: 0, logs: false, rollups: false };

  for (let i = 0; i < REPEATS; i++) {
    const beforeStats = JSON.parse((await get('/internal/stats')).raw).aggregate_path;
    const { result, touched } = await withStatementDelta(() => get(url));
    const afterStats = JSON.parse((await get('/internal/stats')).raw).aggregate_path;

    if (result.status !== 200) { times.push(NaN); continue; }
    times.push(result.ms);
    for (const k of ['rollup', 'raw', 'hybrid']) if (afterStats[k] > beforeStats[k]) pathUsed = k;
    if (i === REPEATS - 1) {
      for (const t of touched) {
        touchedAgg.rows += t.rows; touchedAgg.hit += t.hit; touchedAgg.read += t.read;
        touchedAgg.logs ||= t.reads_logs; touchedAgg.rollups ||= t.reads_rollups;
      }
    }
  }

  const ok = times.filter((t) => !Number.isNaN(t)).sort((a, b) => a - b);
  const p50 = ok.length ? ok[Math.floor(ok.length / 2)] : NaN;
  const max = ok.length ? ok[ok.length - 1] : NaN;
  const reads = [touchedAgg.rollups && 'rollups', touchedAgg.logs && 'LOGS'].filter(Boolean).join('+') || '-';

  results.push({ name, path: pathUsed, p50, max, ...touchedAgg, reads });
  console.log(
    name.padEnd(30) + pathUsed.padEnd(9) + p50.toFixed(1).padStart(9) + max.toFixed(1).padStart(9) +
    String(touchedAgg.rows).padStart(10) + String(touchedAgg.hit).padStart(10) +
    String(touchedAgg.read).padStart(9) + '  ' + reads
  );
}

// The check that matters: a shape the application calls "rollup" must not have
// read the raw table.
const liars = results.filter((r) => r.path === 'rollup' && r.logs);
console.log('\nrouting consistency: ' + (liars.length === 0
  ? 'OK — no shape reported rollup while reading raw logs'
  : 'MISMATCH — ' + liars.map((r) => r.name).join(', ')));

const worst = [...results].sort((a, b) => b.p50 - a.p50).slice(0, 5);
console.log('\nslowest shapes:');
for (const r of worst) console.log(`  ${r.name.padEnd(30)} ${r.p50.toFixed(1)}ms  path=${r.path}  reads=${r.reads}`);

await client.end();
agent.destroy();
