// Post-ingestion read recovery curve.
//
// The same GET /logs URL sequence serves ~40-54k rows/s on an idle server and
// ~13-17k immediately after sustained ingestion. This replays one fixed URL list
// at increasing delays after writes stop, to see whether read throughput
// recovers and what recovers with it.
//
// Two details matter for the measurement to mean anything:
//
//   * The URL list is built from SQL, not by walking the API. A warm-up walk
//     would pull the pages being measured into cache and destroy the T+0 sample,
//     which is the whole point of the experiment. Cursors are constructed the
//     same way the route encodes them, from rows sampled at page boundaries.
//   * Every delay replays the identical list, so page count, byte count and
//     query plans are constant and only database state varies.
//
//   node bench/recovery.mjs --limit 1000 --pages 30 --warmup 0
import http from 'node:http';
import pg from 'pg';

const argv = process.argv;
const num = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : Number(argv[i + 1]); };
const str = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };

const HOST = str('host', '127.0.0.1');
const PORT = num('port', 8080);
const PG = str('pg', 'postgres://loguser:logpass@localhost:55432/logs');
const LIMIT = num('limit', 1000);
const PAGES = num('pages', 30);
const WARMUP = num('warmup', 0);          // 1 = sequential-scan warm-up before T+0
const DELAYS = str('delays', '0,5,15,30,60').split(',').map(Number);

const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
const client = new pg.Client({ connectionString: PG });
await client.connect();

function get(path) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = http.request({ host: HOST, port: PORT, path, agent }, (res) => {
      let bytes = 0;
      res.on('data', (c) => { bytes += c.length; });
      res.on('end', () => resolve({ ms: performance.now() - t0, bytes, status: res.statusCode }));
    });
    req.on('error', () => resolve({ ms: 0, bytes: 0, status: 0 }));
    req.setTimeout(120000, () => req.destroy(new Error('timeout')));
    req.end();
  });
}

/** Same encoding the route uses, so these cursors are indistinguishable from real ones. */
const encodeCursor = (timestamp, id) =>
  Buffer.from(JSON.stringify({ timestamp, id }), 'utf8').toString('base64');

/**
 * Page-boundary cursors taken straight from the table.
 *
 * row_number() over the API's own ordering gives the row that starts each page;
 * formatting the timestamp exactly as the response does keeps the cursor's
 * timestamp identical to what a client would have echoed back.
 */
async function buildUrls() {
  const { rows } = await client.query(
    `SELECT to_char("timestamp" AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS ts, id::text AS id
     FROM (SELECT "timestamp", id, row_number() OVER (ORDER BY "timestamp" DESC, id DESC) AS rn
           FROM logs) t
     WHERE rn % $1 = 0 AND rn <= $2
     ORDER BY rn`,
    [LIMIT, LIMIT * PAGES]
  );
  const urls = [`/logs?limit=${LIMIT}`];
  for (const r of rows.slice(0, PAGES - 1)) {
    urls.push(`/logs?limit=${LIMIT}&cursor=${encodeURIComponent(encodeCursor(r.ts, r.id))}`);
  }
  return urls;
}

async function dbState() {
  const { rows } = await client.query(`
    SELECT (SELECT blks_hit FROM pg_stat_database WHERE datname='logs') hit,
           (SELECT blks_read FROM pg_stat_database WHERE datname='logs') rd,
           (SELECT checkpoints_timed FROM pg_stat_bgwriter) ck_timed,
           (SELECT checkpoints_req FROM pg_stat_bgwriter) ck_req,
           (SELECT buffers_checkpoint FROM pg_stat_bgwriter) buf_ck,
           (SELECT buffers_clean FROM pg_stat_bgwriter) buf_clean,
           (SELECT buffers_backend FROM pg_stat_bgwriter) buf_backend,
           (SELECT count(*) FROM pg_stat_progress_vacuum) vac_active,
           (SELECT COALESCE(sum(n_dead_tup),0) FROM pg_stat_user_tables WHERE relname LIKE 'logs%') dead,
           (SELECT COALESCE(sum(autovacuum_count),0) FROM pg_stat_user_tables WHERE relname LIKE 'logs%') vac_count,
           pg_current_wal_lsn()::text lsn`);
  return rows[0];
}

const pct = (a, p) => (a.length ? Number([...a].sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(p / 100 * a.length))].toFixed(2)) : 0);

async function replay(urls) {
  const before = await dbState();
  const t = [];
  let bytes = 0, rows = 0;
  const w0 = performance.now();
  for (const u of urls) {
    const r = await get(u);
    if (r.status !== 200) break;
    t.push(r.ms); bytes += r.bytes; rows += LIMIT;
  }
  const wall = (performance.now() - w0) / 1000;
  const after = await dbState();
  return {
    rows_per_sec: Math.round(rows / wall),
    mb_per_sec: Number((bytes / wall / 1048576).toFixed(1)),
    page_p50: pct(t, 50), page_p95: pct(t, 95),
    blks_hit: Number(after.hit) - Number(before.hit),
    blks_read: Number(after.rd) - Number(before.rd),
    hit_ratio: Number((( Number(after.hit) - Number(before.hit)) /
      Math.max(Number(after.hit) - Number(before.hit) + Number(after.rd) - Number(before.rd), 1) * 100).toFixed(1)),
    ck_timed: Number(after.ck_timed), ck_req: Number(after.ck_req),
    buf_ck: Number(after.buf_ck) - Number(before.buf_ck),
    buf_backend: Number(after.buf_backend) - Number(before.buf_backend),
    vac_active: Number(after.vac_active), dead: Number(after.dead), vac_count: Number(after.vac_count),
  };
}

const urls = await buildUrls();
console.log(`[recovery] limit=${LIMIT} pages=${urls.length} warmup=${WARMUP} delays=${DELAYS.join(',')}s`);

if (WARMUP) {
  // Read-only touch of the partition the walk will read, to separate passive
  // recovery from cache warming.
  const t0 = performance.now();
  await client.query(`SELECT count(*) FROM (SELECT * FROM logs ORDER BY "timestamp" DESC, id DESC LIMIT $1) w`,
    [LIMIT * PAGES]);
  console.log(`  warm-up read took ${((performance.now() - t0) / 1000).toFixed(1)}s`);
}

console.log('delay'.padStart(7) + ['rows/s', 'MB/s', 'p50ms', 'p95ms', 'blks_hit', 'blks_read', 'hit%', 'buf_ck', 'buf_bknd', 'ck_t/r', 'vac', 'dead'].map((h) => h.padStart(10)).join(''));

let prev = 0;
for (const d of DELAYS) {
  if (d > prev) await new Promise((r) => setTimeout(r, (d - prev) * 1000));
  prev = d;
  const m = await replay(urls);
  console.log(String(d + 's').padStart(7) +
    String(m.rows_per_sec).padStart(10) + String(m.mb_per_sec).padStart(10) +
    String(m.page_p50).padStart(10) + String(m.page_p95).padStart(10) +
    String(m.blks_hit).padStart(10) + String(m.blks_read).padStart(10) +
    String(m.hit_ratio).padStart(10) + String(m.buf_ck).padStart(10) +
    String(m.buf_backend).padStart(10) +
    `${m.ck_timed}/${m.ck_req}`.padStart(10) + String(m.vac_active).padStart(10) + String(m.dead).padStart(10));
}

await client.end();
agent.destroy();
