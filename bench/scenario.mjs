// Closed-loop scenario harness — models the official grader rather than a
// pure write benchmark.
//
// The previous harness (bench/loadtest.mjs) drove POSTs from a dedicated socket
// pool and ran query probes on a *separate* connection pool. That decouples the
// two, so a 5-second aggregate cost nothing in ingest throughput and the harness
// reported 14,959 logs/s where the grader measured 2,896.
//
// This one shares one pool of virtual users between POST and query work. A VU
// blocked on a slow aggregate is a VU not sending POSTs, which is the coupling
// that turns slow reads into an ingest collapse.
//
//   node bench/scenario.mjs --host app --rate 15000 --duration 120 --vus 200
import http from 'node:http';
import { performance } from 'node:perf_hooks';

const argv = process.argv;
function num(name, d) { const i = argv.indexOf(`--${name}`); return i === -1 ? d : Number(argv[i + 1]); }
function str(name, d) { const i = argv.indexOf(`--${name}`); return i === -1 ? d : argv[i + 1]; }

const HOST        = str('host', '127.0.0.1');
const PORT        = num('port', 8080);
const RATE        = num('rate', 15000);        // target logs/sec
const DURATION    = num('duration', 120);      // seconds of load
const BATCH       = num('batch', 33);          // logs per POST
const VUS         = num('vus', 200);           // shared virtual users
const AGG_RATE    = num('agg-rate', 1);        // aggregate requests/sec
const GET_RATE    = num('get-rate', 1);        // GET /logs requests/sec
const AGG_WINDOW  = num('agg-window', 3600);   // seconds of history the aggregate spans
const AGG_BUCKET  = str('agg-bucket', '1m');
const AGG_GROUP   = str('agg-group', 'service');
// Filters that no rollup can answer, so the aggregate has to read raw rows.
// These are the queries that used to take the whole database down, and they are
// still the expensive ones — the point of measuring them separately is to show
// what bounds them now that the rollup cannot.
// Payload shape. The generator's default entry is unrealistically small — a
// ~30 character message and four short attributes — and row width drives heap
// bytes, WAL volume and GIN maintenance, which is Postgres CPU per log rather
// than per request. A grader sending realistic log lines would make the same
// logical rate cost several times more in the database.
const MSG_BYTES   = num('msg-bytes', 0);       // 0 = leave the template message
const ATTR_COUNT  = num('attr-count', 0);      // 0 = leave the four defaults
const AGG_Q       = str('agg-q', '');
const AGG_ATTR    = str('agg-attr', '');
// Ramped load, for the stress and breakpoint scenarios the official benchmark
// runs: --stages "15000:40,22500:40,30000:40" means 40s at each rate. The point
// of ramping is not the peak number but whether throughput keeps climbing or
// falls off a cliff, so each stage is reported separately.
const STAGES_ARG  = str('stages', '');
const DRAIN       = num('drain', 30);          // seconds allowed to read everything back
const DRAIN_LIMIT = num('drain-limit', 1000);
const SAMPLE_MS   = num('sample', 5000);       // throughput sample interval
const REQ_TIMEOUT = num('timeout', 30000);

const agent = new http.Agent({ keepAlive: true, maxSockets: VUS + 16, maxFreeSockets: VUS + 16 });

const SERVICES = ['checkout', 'auth', 'payments', 'search', 'inventory', 'shipping', 'notify', 'gateway'];
const LEVELS   = ['debug', 'info', 'warn', 'error'];
const REGIONS  = ['eu-west', 'us-east', 'ap-south', 'sa-east'];
const MESSAGES = [
  'payment declined', 'user login succeeded', 'cache miss for key',
  'upstream timeout after 3000ms', 'order committed to ledger',
  'rate limit applied to tenant', 'index rebuild finished', 'connection reset by peer',
];

// ---------------------------------------------------------------- transport
function call(method, path, body) {
  const payload = body === undefined ? null : Buffer.from(body);
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = http.request(
      { host: HOST, port: PORT, path, method, agent,
        headers: payload ? { 'content-type': 'application/json', 'content-length': payload.length } : {} },
      (res) => {
        let raw = '';
        res.setEncoding('utf8');
        res.on('data', (c) => { raw += c; });
        res.on('end', () => resolve({ ms: performance.now() - t0, status: res.statusCode, raw }));
      }
    );
    req.on('error', (e) => resolve({ ms: performance.now() - t0, status: 0, raw: '', err: String(e.message || e) }));
    req.setTimeout(REQ_TIMEOUT, () => req.destroy(new Error('timeout')));
    req.end(payload);
  });
}

// --------------------------------------------------------------- body pool
// Pre-serialised so the generator itself never becomes the bottleneck. Only the
// timestamps are rewritten per send, so rows always land at "now" the way the
// grader's do.
const BODY_POOL = 96;
const bodies = [];
for (let p = 0; p < BODY_POOL; p++) {
  const logs = new Array(BATCH);
  for (let i = 0; i < BATCH; i++) {
    const n = p * BATCH + i;
    let message = `${MESSAGES[n % MESSAGES.length]} #${n}`;
    if (MSG_BYTES > 0) {
      // Pad with varied text rather than one repeated character, so compression
      // and the message's own entropy stay representative.
      while (message.length < MSG_BYTES) {
        message += ` ${MESSAGES[(n + message.length) % MESSAGES.length]}`;
      }
      message = message.slice(0, MSG_BYTES);
    }
    const attributes = {
      user_id: String(n % 10000),
      region: REGIONS[n % REGIONS.length],
      retries: n % 5,
      cached: (n & 1) === 0,
    };
    for (let k = 4; k < ATTR_COUNT; k++) {
      attributes[`field_${k}`] = `${MESSAGES[(n + k) % MESSAGES.length]}-${n % 1000}`;
    }
    logs[i] = {
      timestamp: '',
      level: LEVELS[n % LEVELS.length],
      service: SERVICES[n % SERVICES.length],
      message,
      attributes,
    };
  }
  bodies.push(logs);
}
let bodyCursor = 0;
function nextBody() {
  const logs = bodies[bodyCursor++ % BODY_POOL];
  const now = new Date().toISOString();
  for (const l of logs) l.timestamp = now;
  return JSON.stringify({ logs });
}

// ------------------------------------------------------------------ metrics
const lat = { post: [], aggregate: [], getlogs: [], all: [] };
const counts = { post: 0, aggregate: 0, getlogs: 0 };
const status = new Map();
let accepted = 0, rejected = 0, httpErrors = 0, timeouts = 0;
let shapeProblems = new Set();
let shapeChecks = 0, shapeOk = 0;
const samples = [];
let acceptedAtLastSample = 0;

function record(kind, r) {
  lat[kind].push(r.ms);
  lat.all.push(r.ms);
  counts[kind]++;
  status.set(r.status, (status.get(r.status) ?? 0) + 1);
  if (r.status === 0) { httpErrors++; if ((r.err || '').includes('timeout')) timeouts++; }
  else if (r.status >= 400) httpErrors++;
}

function checkLogsShape(raw) {
  shapeChecks++;
  let p; try { p = JSON.parse(raw); } catch { shapeProblems.add('logs: unparseable'); return; }
  if (!Array.isArray(p.logs)) { shapeProblems.add('logs: not an array'); return; }
  if (!('next_cursor' in p)) { shapeProblems.add('logs: next_cursor absent'); return; }
  if (p.next_cursor !== null && typeof p.next_cursor !== 'string') { shapeProblems.add('logs: next_cursor type'); return; }
  for (const l of p.logs.slice(0, 3)) {
    if (typeof l.id !== 'string') { shapeProblems.add(`logs: id is ${typeof l.id}`); return; }
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(l.timestamp)) { shapeProblems.add('logs: timestamp format'); return; }
  }
  shapeOk++;
}

function checkAggShape(raw) {
  shapeChecks++;
  let p; try { p = JSON.parse(raw); } catch { shapeProblems.add('aggregate: unparseable'); return; }
  if (!Array.isArray(p.buckets)) { shapeProblems.add('aggregate: buckets not an array'); return; }
  for (const b of p.buckets.slice(0, 3)) {
    if (typeof b.count !== 'number') { shapeProblems.add(`aggregate: count is ${typeof b.count}`); return; }
    if (!('group' in b)) { shapeProblems.add('aggregate: group absent'); return; }
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(b.start ?? '')) { shapeProblems.add(`aggregate: start ${b.start}`); return; }
  }
  shapeOk++;
}

// ---------------------------------------------------------------- scheduler
// Wants POSTs at RATE/BATCH per second and queries at their own rates. A task is
// only dispatched when a VU is free, so an operation that blocks its VU directly
// suppresses the achieved rate — the closed-loop behaviour the grader exhibits.
const STAGES = STAGES_ARG === ''
  ? [{ rate: RATE, seconds: DURATION }]
  : STAGES_ARG.split(',').map((part) => {
      const [rate, seconds] = part.split(':').map(Number);
      return { rate, seconds };
    });
const TOTAL_SECONDS = STAGES.reduce((a, st) => a + st.seconds, 0);

/**
 * POSTs that *should* have been dispatched by `elapsed`, integrating the rate
 * over the stage schedule. A flat rate is just the one-stage case, so both
 * modes go through the same scheduler.
 */
function targetPostsBy(elapsed) {
  let total = 0;
  let remaining = elapsed;
  for (const stage of STAGES) {
    if (remaining <= 0) break;
    const dt = Math.min(remaining, stage.seconds);
    total += (stage.rate / BATCH) * dt;
    remaining -= dt;
  }
  return total;
}

/** Which stage `elapsed` falls in, for per-stage reporting. */
function stageAt(elapsed) {
  let acc = 0;
  for (let i = 0; i < STAGES.length; i++) {
    acc += STAGES[i].seconds;
    if (elapsed < acc) return i;
  }
  return STAGES.length - 1;
}

let startedAt = 0;
let dispatchedPost = 0, dispatchedAgg = 0, dispatchedGet = 0;
let running = true;

function nextTask() {
  const elapsed = (performance.now() - startedAt) / 1000;
  if (Math.floor(elapsed * AGG_RATE) > dispatchedAgg) { dispatchedAgg++; return 'aggregate'; }
  if (Math.floor(elapsed * GET_RATE) > dispatchedGet) { dispatchedGet++; return 'getlogs'; }
  if (targetPostsBy(elapsed) > dispatchedPost) { dispatchedPost++; return 'post'; }
  return null;
}

async function runTask(kind) {
  if (kind === 'post') {
    const r = await call('POST', '/logs', nextBody());
    record('post', r);
    if (r.status === 200) {
      try { const p = JSON.parse(r.raw); accepted += p.accepted ?? 0; rejected += (p.rejected ?? []).length; }
      catch { shapeProblems.add('post: unparseable'); }
    }
    return;
  }
  if (kind === 'aggregate') {
    const until = new Date(Date.now() + 60000).toISOString();
    const since = new Date(Date.now() + 60000 - AGG_WINDOW * 1000).toISOString();
    const group = AGG_GROUP === 'none' ? '' : `&group_by=${AGG_GROUP}`;
    const q = AGG_Q === '' ? '' : `&q=${encodeURIComponent(AGG_Q)}`;
    const attr = AGG_ATTR === '' ? '' : `&attr.${AGG_ATTR}`;
    const r = await call('GET', `/logs/aggregate?since=${since}&until=${until}&bucket=${AGG_BUCKET}${group}${q}${attr}`);
    record('aggregate', r);
    if (r.status === 200) checkAggShape(r.raw);
    return;
  }
  const r = await call('GET', `/logs?limit=100`);
  record('getlogs', r);
  if (r.status === 200) checkLogsShape(r.raw);
}

async function vu() {
  while (running) {
    const kind = nextTask();
    if (kind === null) { await new Promise((r) => setTimeout(r, 2)); continue; }
    await runTask(kind);
  }
}

// -------------------------------------------------------------------- pctl
function pct(arr, p) {
  if (arr.length === 0) return 0;
  const s = [...arr].sort((a, b) => a - b);
  return Number(s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))].toFixed(1));
}

// -------------------------------------------------------------------- drain
// After load stops, read back as much as possible within DRAIN seconds by
// walking the keyset cursor — the same thing the grader's consistency check does.
async function drain(runStartIso) {
  const t0 = performance.now();
  const seen = new Set();
  let cursor = null;
  let pages = 0, pageErrors = 0, pageTimeouts = 0;
  const pageLat = [];

  while (performance.now() - t0 < DRAIN * 1000) {
    const qs = `since=${runStartIso}&limit=${DRAIN_LIMIT}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
    const r = await call('GET', `/logs?${qs}`);
    pageLat.push(r.ms);
    pages++;
    if (r.status !== 200) {
      pageErrors++;
      if ((r.err || '').includes('timeout')) pageTimeouts++;
      break;
    }
    let p; try { p = JSON.parse(r.raw); } catch { pageErrors++; break; }
    for (const l of p.logs) seen.add(l.id);
    cursor = p.next_cursor;
    if (!cursor) break;
  }

  const elapsed = (performance.now() - t0) / 1000;
  return {
    drain_seconds: Number(elapsed.toFixed(2)),
    hit_cap: elapsed >= DRAIN - 0.2,
    pages_walked: pages,
    page_errors: pageErrors,
    page_timeouts: pageTimeouts,
    page_ms: { p50: pct(pageLat, 50), p95: pct(pageLat, 95), max: Number(Math.max(0, ...pageLat).toFixed(1)) },
    visible_records: seen.size,
    read_rows_per_sec: Number((seen.size / elapsed).toFixed(1)),
  };
}

// --------------------------------------------------------------------- main
console.log(`[scenario] rate=${RATE} logs/s batch=${BATCH} vus=${VUS} duration=${DURATION}s ` +
            `agg=${AGG_RATE}/s (${AGG_BUCKET}, ${AGG_WINDOW}s window, group_by=${AGG_GROUP}` +
            `${AGG_Q ? `, q=${AGG_Q}` : ''}${AGG_ATTR ? `, attr.${AGG_ATTR}` : ''}) get=${GET_RATE}/s drain=${DRAIN}s` +
            `${STAGES_ARG ? ` stages=${STAGES_ARG}` : ''}`);

const runStartIso = new Date(Date.now() - 5000).toISOString();
startedAt = performance.now();

const sampler = setInterval(() => {
  const delta = accepted - acceptedAtLastSample;
  acceptedAtLastSample = accepted;
  const t = Math.round((performance.now() - startedAt) / 1000);
  const rate = Math.round(delta / (SAMPLE_MS / 1000));
  const stage = stageAt(t);
  samples.push({ t, stage, offered: STAGES[stage].rate, logs_per_sec: rate });
  console.log(`  t=${String(t).padStart(3)}s  stage ${stage} (offered ${STAGES[stage].rate})  ${String(rate).padStart(6)} logs/s`);
}, SAMPLE_MS);

const vus = Array.from({ length: VUS }, () => vu());
await new Promise((r) => setTimeout(r, TOTAL_SECONDS * 1000));
running = false;
clearInterval(sampler);
await Promise.all(vus);

const wall = (performance.now() - startedAt) / 1000;
const peak = samples.reduce((m, s) => Math.max(m, s.logs_per_sec), 0);
const tail = samples.slice(Math.floor(samples.length / 2));
const tailAvg = tail.length ? Math.round(tail.reduce((a, s) => a + s.logs_per_sec, 0) / tail.length) : 0;

console.log('\n[scenario] load phase complete, draining...');
const drainResult = await drain(runStartIso);

console.log('\n' + JSON.stringify({
  config: { target_logs_per_sec: RATE, batch: BATCH, vus: VUS, duration_s: TOTAL_SECONDS,
            stages: STAGES,
            aggregate: `${AGG_BUCKET}/${AGG_WINDOW}s/group_by=${AGG_GROUP}` },
  throughput: {
    accepted_logs: accepted,
    rejected_logs: rejected,
    achieved_logs_per_sec: Number((accepted / wall).toFixed(1)),
    peak_logs_per_sec: peak,
    second_half_avg_logs_per_sec: tailAvg,
    collapse_ratio: peak > 0 ? Number((tailAvg / peak).toFixed(3)) : null,
    http_requests: counts.post + counts.aggregate + counts.getlogs,
  },
  operations: counts,
  per_stage: STAGES.map((stage, i) => {
    const mine = samples.filter((x) => x.stage === i);
    // The first sample of a stage straddles the boundary, so it is dropped.
    const settled = mine.slice(1);
    return {
      offered_logs_per_sec: stage.rate,
      seconds: stage.seconds,
      achieved_logs_per_sec: settled.length
        ? Math.round(settled.reduce((a, x) => a + x.logs_per_sec, 0) / settled.length)
        : null,
    };
  }),
  latency_ms: {
    post:      { p50: pct(lat.post, 50),      p95: pct(lat.post, 95),      p99: pct(lat.post, 99) },
    aggregate: { p50: pct(lat.aggregate, 50), p95: pct(lat.aggregate, 95), p99: pct(lat.aggregate, 99) },
    getlogs:   { p50: pct(lat.getlogs, 50),   p95: pct(lat.getlogs, 95),   p99: pct(lat.getlogs, 99) },
    overall:   { p50: pct(lat.all, 50),       p95: pct(lat.all, 95),       p99: pct(lat.all, 99) },
  },
  errors: { http_errors: httpErrors, timeouts, statuses: Object.fromEntries(status) },
  shape: { checks: shapeChecks, ok: shapeOk, valid: shapeProblems.size === 0, problems: [...shapeProblems] },
  eventual_consistency: {
    ...drainResult,
    accepted_records: accepted,
    missing_records: Math.max(0, accepted - drainResult.visible_records),
    passed: drainResult.visible_records >= accepted,
  },
  samples,
}, null, 2));

agent.destroy();
