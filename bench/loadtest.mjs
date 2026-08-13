// Standalone load generator for POST /logs.
//
// Models the official harness: a fixed pool of keep-alive connections issuing
// batched POSTs at a target rate, with a ceiling on in-flight requests so a
// slow service produces backpressure (and therefore a lower achieved rate)
// rather than an unbounded queue.
//
//   node bench/loadtest.mjs --rate 15000 --duration 60 --batch 50 --conns 64
import http from 'node:http';
import { performance } from 'node:perf_hooks';

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : Number(process.argv[i + 1]);
}
function argStr(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const RATE = arg('rate', 15000);          // target logs/sec
const DURATION = arg('duration', 60);     // seconds
const BATCH = arg('batch', 50);           // logs per POST
const CONNS = arg('conns', 64);           // max in-flight requests
const HOST = argStr('host', '127.0.0.1');
const PORT = arg('port', 8080);
const MARKER = argStr('marker', '');      // stamped into attributes for read-after-write

const SERVICES = ['checkout', 'auth', 'payments', 'search', 'inventory', 'shipping', 'notify', 'gateway'];
const LEVELS = ['debug', 'info', 'warn', 'error'];
const REGIONS = ['eu-west', 'us-east', 'ap-south', 'sa-east'];
const MESSAGES = [
  'payment declined', 'user login succeeded', 'cache miss for key',
  'upstream timeout after 3000ms', 'order committed to ledger',
  'rate limit applied to tenant', 'index rebuild finished', 'connection reset by peer',
];

const agent = new http.Agent({ keepAlive: true, maxSockets: CONNS, maxFreeSockets: CONNS });

// Latency samples, kept as a plain array of numbers and sorted once at the end.
const latencies = [];
let sent = 0, completed = 0, accepted = 0, rejected = 0, errors = 0, nonOk = 0;
let inFlight = 0;
const statusCounts = new Map();

/** Builds one batch body. Timestamps are "now" so rows land in the live partition. */
function buildBody(seq) {
  const logs = new Array(BATCH);
  const now = Date.now();
  for (let i = 0; i < BATCH; i++) {
    const n = seq * BATCH + i;
    logs[i] = {
      timestamp: new Date(now - (i % 50)).toISOString(),
      level: LEVELS[n % LEVELS.length],
      service: SERVICES[n % SERVICES.length],
      message: `${MESSAGES[n % MESSAGES.length]} #${n}`,
      attributes: {
        user_id: String(n % 10000),
        region: REGIONS[n % REGIONS.length],
        retries: n % 5,
        cached: (n & 1) === 0,
        ...(MARKER ? { marker: MARKER } : {}),
      },
    };
  }
  return Buffer.from(JSON.stringify({ logs }));
}

// Pre-serialise a ring of bodies: JSON.stringify at 15k logs/s would otherwise
// make the generator itself the bottleneck and understate the service.
const BODY_POOL = 64;
const bodies = Array.from({ length: BODY_POOL }, (_, i) => buildBody(i));
let bodyCursor = 0;

function fire() {
  const body = bodies[bodyCursor++ % BODY_POOL];
  const started = performance.now();
  sent++;
  inFlight++;
  const req = http.request(
    { host: HOST, port: PORT, path: '/logs', method: 'POST', agent,
      headers: { 'content-type': 'application/json', 'content-length': body.length } },
    (res) => {
      let chunks = '';
      res.setEncoding('utf8');
      res.on('data', (c) => { chunks += c; });
      res.on('end', () => {
        inFlight--; completed++;
        latencies.push(performance.now() - started);
        statusCounts.set(res.statusCode, (statusCounts.get(res.statusCode) ?? 0) + 1);
        if (res.statusCode !== 200) { nonOk++; return; }
        try {
          const parsed = JSON.parse(chunks);
          accepted += parsed.accepted ?? 0;
          rejected += (parsed.rejected ?? []).length;
        } catch { nonOk++; }
      });
    }
  );
  req.on('error', () => { inFlight--; completed++; errors++; });
  req.setTimeout(60000, () => { req.destroy(new Error('timeout')); });
  req.end(body);
}

const batchesPerSec = RATE / BATCH;
const tickMs = 5;
const startedAt = performance.now();
const endAt = startedAt + DURATION * 1000;
let dispatched = 0;

console.log(`[loadgen] target=${RATE} logs/s batch=${BATCH} (${batchesPerSec.toFixed(1)} req/s) conns=${CONNS} duration=${DURATION}s`);

// Wall-clock paced, not tick-counted: under load setInterval drifts badly, and
// counting ticks would stretch a 60s run into several minutes.
const timer = setInterval(() => {
  const now = performance.now();
  const elapsed = (now - startedAt) / 1000;
  const due = Math.floor(elapsed * batchesPerSec);
  while (dispatched < due && inFlight < CONNS) { fire(); dispatched++; }
  // Requests we could not dispatch because every connection was busy are lost
  // rather than deferred, so a slow service shows up as a lower achieved rate.
  if (dispatched < due) dispatched = due;
  if (now >= endAt) { clearInterval(timer); finish(); }
}, tickMs);

function pct(sorted, p) {
  if (sorted.length === 0) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

async function finish() {
  const drainDeadline = Date.now() + 70000;
  while (inFlight > 0 && Date.now() < drainDeadline) {
    await new Promise((r) => setTimeout(r, 100));
  }
  const wall = (performance.now() - startedAt) / 1000;
  latencies.sort((a, b) => a - b);
  const out = {
    target_logs_per_sec: RATE,
    wall_seconds: Number(wall.toFixed(2)),
    http_requests_sent: sent,
    http_requests_completed: completed,
    accepted_logs: accepted,
    rejected_logs: rejected,
    achieved_logs_per_sec: Number((accepted / wall).toFixed(1)),
    achieved_req_per_sec: Number((completed / wall).toFixed(1)),
    transport_errors: errors,
    non_200_or_unparseable: nonOk,
    statuses: Object.fromEntries(statusCounts),
    latency_ms: {
      p50: Number(pct(latencies, 50).toFixed(1)),
      p95: Number(pct(latencies, 95).toFixed(1)),
      p99: Number(pct(latencies, 99).toFixed(1)),
      max: Number((latencies.at(-1) ?? 0).toFixed(1)),
    },
  };
  console.log(JSON.stringify(out, null, 2));
  agent.destroy();
}
