// Raw-vs-rollup equivalence for GET /logs/aggregate.
//
//   node test/rollup.mjs [--port 8080] [--pg postgres://...]
//
// The hybrid aggregate answers part of a request from log_rollups and part from
// the raw table. That split is invisible in the response, so a boundary error
// does not surface as an HTTP error — it surfaces as a count that is quietly
// wrong by the number of rows sitting on the wrong side of the boundary.
//
// Every assertion here therefore compares the API's answer against a ground
// truth computed directly in SQL over the raw rows, with no application code in
// between. If the two ever disagree the rollup is lying.
import http from 'node:http';
import pg from 'pg';

const argv = process.argv;
const num = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : Number(argv[i + 1]); };
const str = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };

const PORT = num('port', 8080);
// 55432, not 5432: see the port comment in docker-compose.yml.
const PG   = str('pg', 'postgres://loguser:logpass@localhost:55432/logs');

const agent = new http.Agent({ keepAlive: true, maxSockets: 4 });
const client = new pg.Client({ connectionString: PG });

let passed = 0;
const failures = [];

function check(name, condition, detail) {
  if (condition) { passed++; return; }
  failures.push(`${name}${detail ? ` — ${detail}` : ''}`);
  console.log(`  FAIL  ${name}${detail ? ` — ${detail}` : ''}`);
}

function request(method, path, body) {
  const payload = body === undefined ? null : Buffer.from(JSON.stringify(body));
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host: '127.0.0.1', port: PORT, path, method, agent,
        headers: payload ? { 'content-type': 'application/json', 'content-length': payload.length } : {} },
      (res) => {
        let raw = '';
        res.setEncoding('utf8');
        res.on('data', (c) => { raw += c; });
        res.on('end', () => {
          let json = null;
          try { json = JSON.parse(raw); } catch { /* left null */ }
          resolve({ status: res.statusCode, json, raw });
        });
      }
    );
    req.on('error', reject);
    req.setTimeout(120000, () => req.destroy(new Error('timeout')));
    req.end(payload);
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Runs the refresh job by hand rather than waiting for pg_cron's next tick. */
async function refresh() {
  const { rows } = await client.query('SELECT refresh_log_rollups() AS msg');
  return rows[0].msg;
}

/** Lets the app's 1s rollup-state cache pick up a boundary the refresh just moved. */
async function settle() {
  await sleep(1300);
}

/**
 * Waits out the ingest floor.
 *
 * Accepting a backdated entry pulls the aggregate boundary down to its minute
 * for INGEST_FLOOR_WINDOW_SECONDS, which is what keeps such an entry visible
 * until it has been folded. Everything this file seeds is backdated by several
 * minutes, so without waiting for that window to lapse every assertion below
 * would be answered from raw rows and the rollup path would never be exercised
 * at all — the tests would pass while proving nothing.
 */
const FLOOR_WINDOW_MS = num('floor-window', 30) * 1000;
async function floorLapses() {
  process.stdout.write(`  waiting ${FLOOR_WINDOW_MS / 1000}s for the ingest floor to lapse... `);
  await sleep(FLOOR_WINDOW_MS + 2000);
  console.log('done');
}

/** Snapshot of which source has answered aggregates so far. */
async function paths() {
  return (await request('GET', '/internal/stats')).json.aggregate_path;
}

const WIDTHS = { '1m': 60, '5m': 300, '1h': 3600, '1d': 86400 };

/**
 * The answer the pre-rollup implementation would have given, computed straight
 * from the raw rows. Mirrors the raw query in aggregateRepository exactly,
 * including the half-open range and the epoch bin origin.
 */
async function groundTruth({ since, until, bucket, groupBy, service, level, q }) {
  const params = [since, until];
  const conds = ['"timestamp" >= $1::timestamptz', '"timestamp" < $2::timestamptz'];
  if (service !== undefined) { params.push(service); conds.push(`service = $${params.length}`); }
  if (level !== undefined)   { params.push(level);   conds.push(`level = $${params.length}`); }
  if (q !== undefined)       { params.push(`%${q}%`); conds.push(`message ILIKE $${params.length}`); }

  const grp = groupBy === undefined ? 'NULL::text' : groupBy;
  const { rows } = await client.query(
    `SELECT to_char(b AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS start,
            grp AS group, n::int AS count
     FROM (
       SELECT date_bin(make_interval(secs => ${WIDTHS[bucket]}), "timestamp", TIMESTAMPTZ 'epoch') AS b,
              ${grp} AS grp, count(*) AS n
       FROM logs WHERE ${conds.join(' AND ')} GROUP BY 1, 2
     ) t`,
    params
  );
  return sortBuckets(rows);
}

function sortBuckets(rows) {
  return [...rows].sort((a, b) => {
    if (a.start !== b.start) return a.start < b.start ? -1 : 1;
    const l = a.group ?? '', r = b.group ?? '';
    return l === r ? 0 : l < r ? -1 : 1;
  });
}

function queryString({ since, until, bucket, groupBy, service, level, q, attr }) {
  const parts = [`since=${encodeURIComponent(since)}`, `until=${encodeURIComponent(until)}`, `bucket=${bucket}`];
  if (groupBy !== undefined) parts.push(`group_by=${encodeURIComponent(groupBy)}`);
  if (service !== undefined) parts.push(`service=${encodeURIComponent(service)}`);
  if (level !== undefined) parts.push(`level=${level}`);
  if (q !== undefined) parts.push(`q=${encodeURIComponent(q)}`);
  for (const [k, v] of Object.entries(attr ?? {})) parts.push(`attr.${k}=${encodeURIComponent(v)}`);
  return `/logs/aggregate?${parts.join('&')}`;
}

/** Asserts the endpoint and the raw ground truth agree, row for row. */
async function assertEquivalent(label, spec) {
  const [api, truth] = await Promise.all([request('GET', queryString(spec)), groundTruth(spec)]);

  if (api.status !== 200) {
    check(label, false, `HTTP ${api.status}: ${api.raw.slice(0, 160)}`);
    return;
  }

  const got = sortBuckets(api.json.buckets);
  if (got.length !== truth.length) {
    check(label, false, `bucket count ${got.length} != ${truth.length}`);
    return;
  }

  for (let i = 0; i < truth.length; i++) {
    const a = got[i], b = truth[i];
    if (a.start !== b.start || (a.group ?? null) !== (b.group ?? null) || a.count !== b.count) {
      check(label, false,
        `row ${i}: got ${JSON.stringify(a)} want ${JSON.stringify(b)}`);
      return;
    }
    if (typeof a.count !== 'number') {
      check(label, false, `count is ${typeof a.count}, must be a JSON number`);
      return;
    }
  }
  check(label, true);
}

/** Posts entries in chunks and waits for them to be committed. */
async function ingest(entries) {
  for (let i = 0; i < entries.length; i += 500) {
    const r = await request('POST', '/logs', { logs: entries.slice(i, i + 500) });
    if (r.status !== 200) throw new Error(`ingest failed: ${r.status} ${r.raw.slice(0, 200)}`);
  }
}

// Deterministic generator, so a failure can be reproduced exactly.
let seed = 0x5eed1234;
function rnd() {
  seed ^= seed << 13; seed >>>= 0;
  seed ^= seed >> 17;
  seed ^= seed << 5;  seed >>>= 0;
  return seed / 0x100000000;
}
const pick = (arr) => arr[Math.floor(rnd() * arr.length)];

const RUN      = `rl-${Date.now()}`;
const SERVICES = [`${RUN}-alpha`, `${RUN}-beta`, `${RUN}-gamma`];
const LEVELS   = ['debug', 'info', 'warn', 'error'];
const iso      = (ms) => new Date(ms).toISOString();

await client.connect();

// lag_seconds normally holds the boundary ~30s behind now so that a log stamped
// just under it cannot be committed just over it. The tests below insert rows at
// timestamps they choose and refresh by hand, so that protection is not needed
// and would only mean waiting a minute for each assertion. Restored at the end.
const [{ lag_seconds: originalLag }] = (await client.query('SELECT lag_seconds FROM rollup_config WHERE id')).rows;
await client.query('UPDATE rollup_config SET lag_seconds = 0 WHERE id');

try {
  // Minute boundary a few minutes back: old enough that everything written
  // against it lands below the refresh boundary and is served from the rollup.
  const base = Math.floor((Date.now() - 8 * 60_000) / 60_000) * 60_000;

  console.log('\n=== seeding: multiple services, levels, collisions, boundaries ===');
  {
    const entries = [];

    // Spread across six minutes, every service/level combination represented.
    for (let i = 0; i < 1800; i++) {
      entries.push({
        timestamp: iso(base + Math.floor(rnd() * 6 * 60_000)),
        level: pick(LEVELS),
        service: pick(SERVICES),
        message: `seeded entry ${i}`,
        attributes: { run: RUN, shard: String(i % 7) },
      });
    }

    // Exact bucket boundaries, including the first and last instant of a minute.
    for (const offset of [0, 59_999, 60_000, 299_999, 300_000, 3_599_999]) {
      entries.push({
        timestamp: iso(base + offset),
        level: 'error',
        service: SERVICES[0],
        message: `boundary entry at +${offset}ms`,
        attributes: { run: RUN, boundary: 'yes' },
      });
    }

    // 300 rows sharing one timestamp to the millisecond.
    for (let i = 0; i < 300; i++) {
      entries.push({
        timestamp: iso(base + 120_000),
        level: 'warn',
        service: SERVICES[1],
        message: `collision ${i}`,
        attributes: { run: RUN, collision: 'yes' },
      });
    }

    // Deliberately out of timestamp order relative to everything above.
    for (let i = 0; i < 200; i++) {
      entries.push({
        timestamp: iso(base + 360_000 - i * 997),
        level: 'debug',
        service: SERVICES[2],
        message: `reverse ordered ${i}`,
        attributes: { run: RUN },
      });
    }

    await ingest(entries);
    console.log(`  ingested ${entries.length} entries at ${iso(base)} .. +6m`);
    console.log(`  refresh: ${await refresh()}`);
    await settle();
    await floorLapses();
  }

  const from = iso(base);
  const to   = iso(base + 7 * 60_000);
  const pathsBefore = await paths();

  console.log('\n=== equivalence: every bucket width x every group_by ===');
  for (const bucket of ['1m', '5m', '1h', '1d']) {
    for (const groupBy of [undefined, 'service', 'level']) {
      await assertEquivalent(
        `bucket=${bucket} group_by=${groupBy ?? 'none'}`,
        { since: from, until: to, bucket, groupBy }
      );
    }
  }

  {
    // Every request in the sweep spans whole minutes entirely below the
    // boundary, so all of them must have come from the rollup. Falling back to
    // raw would still produce correct numbers, which is exactly why it has to
    // be asserted rather than assumed — a routing bug that quietly disabled the
    // rollup would otherwise leave every test in this file green.
    const after = await paths();
    check('the sweep used the rollup', after.rollup - pathsBefore.rollup === 12,
      `${JSON.stringify(pathsBefore)} -> ${JSON.stringify(after)}`);
    check('the sweep never touched raw rows', after.raw === pathsBefore.raw && after.hybrid === pathsBefore.hybrid,
      `${JSON.stringify(pathsBefore)} -> ${JSON.stringify(after)}`);
  }

  console.log('\n=== equivalence: filters the rollup can serve ===');
  await assertEquivalent('service filter', { since: from, until: to, bucket: '1m', service: SERVICES[0] });
  await assertEquivalent('level filter', { since: from, until: to, bucket: '1m', level: 'error' });
  await assertEquivalent('service + level', { since: from, until: to, bucket: '5m', service: SERVICES[1], level: 'warn' });
  await assertEquivalent('service filter + group_by=level',
    { since: from, until: to, bucket: '1m', service: SERVICES[2], groupBy: 'level' });
  await assertEquivalent('no matching rows',
    { since: from, until: to, bucket: '1m', service: `${RUN}-absent`, groupBy: 'level' });

  console.log('\n=== equivalence: ranges that do not sit on bucket edges ===');
  const pathsBeforeEdges = await paths();
  // The rollup stores whole minutes, so a range starting mid-minute must take
  // its first partial minute from raw rows. Getting this wrong over-counts by
  // however many rows fall in the excluded fragment.
  await assertEquivalent('since mid-minute',
    { since: iso(base + 30_000), until: to, bucket: '1m', groupBy: 'service' });
  await assertEquivalent('until mid-minute',
    { since: from, until: iso(base + 210_500), bucket: '1m', groupBy: 'service' });
  await assertEquivalent('both mid-minute',
    { since: iso(base + 30_500), until: iso(base + 210_500), bucket: '5m', groupBy: 'level' });
  await assertEquivalent('sub-second precision',
    { since: iso(base + 30_123), until: iso(base + 210_987), bucket: '1m' });
  await assertEquivalent('range narrower than one bucket',
    { since: iso(base + 10_000), until: iso(base + 20_000), bucket: '1m', groupBy: 'service' });
  await assertEquivalent('empty range',
    { since: from, until: from, bucket: '1m', groupBy: 'service' });

  {
    // A range starting or ending mid-minute has to take its partial minute from
    // raw rows and the rest from the rollup, which is the hybrid path. Two of
    // the six cases above cannot use the rollup at all — one narrower than a
    // single bucket, one empty — so those are the only raw ones.
    const after = await paths();
    check('unaligned ranges combined both sources', after.hybrid - pathsBeforeEdges.hybrid === 4,
      `${JSON.stringify(pathsBeforeEdges)} -> ${JSON.stringify(after)}`);
    check('sub-bucket and empty ranges went raw', after.raw - pathsBeforeEdges.raw === 2,
      `${JSON.stringify(pathsBeforeEdges)} -> ${JSON.stringify(after)}`);
  }

  console.log('\n=== raw fallback: q and attr.* are not in the rollup ===');
  await assertEquivalent('q filter', { since: from, until: to, bucket: '1m', q: 'collision', groupBy: 'service' });
  await assertEquivalent('q filter, no group', { since: from, until: to, bucket: '5m', q: 'boundary entry' });
  {
    // attr.* cannot be expressed in the ground-truth helper's SQL, so it is
    // checked against a count computed from the same predicate the API uses.
    const r = await request('GET', queryString({
      since: from, until: to, bucket: '1d', attr: { collision: 'yes', run: RUN },
    }));
    const { rows } = await client.query(
      `SELECT count(*)::int AS n FROM logs
       WHERE "timestamp" >= $1::timestamptz AND "timestamp" < $2::timestamptz
         AND attributes @> '{"collision":"yes"}'::jsonb AND attributes ->> 'collision' = 'yes'
         AND attributes ->> 'run' = $3`,
      [from, to, RUN]
    );
    const total = r.json.buckets.reduce((a, b) => a + b.count, 0);
    check('attr filter total matches raw', total === rows[0].n, `${total} vs ${rows[0].n}`);
    check('attr filter found the collision rows', rows[0].n === 300, `${rows[0].n}`);
  }
  {
    const stats = await request('GET', '/internal/stats');
    check('q and attr requests took the raw path', stats.json.aggregate_path.raw >= 3,
      JSON.stringify(stats.json.aggregate_path));
  }

  console.log('\n=== late and out-of-order arrivals ===');
  {
    const targetMinute = base + 60_000;
    const before = await groundTruth({ since: from, until: to, bucket: '1m', groupBy: 'service' });

    // Backdated by roughly eight minutes: these belong to a minute that has
    // already been rolled up and whose bucket the refresh considers closed.
    const late = [];
    for (let i = 0; i < 250; i++) {
      late.push({
        timestamp: iso(targetMinute + (i % 60) * 1000),
        level: 'info',
        service: SERVICES[0],
        message: `late arrival ${i}`,
        attributes: { run: RUN, late: 'yes' },
      });
    }
    await ingest(late);

    // Before the refresh sees them they must still be counted. They sit under a
    // boundary the refresh has already passed, so the only thing that can save
    // them is the ingest floor dragging that boundary back below their minute.
    await assertEquivalent('late rows visible before refresh',
      { since: from, until: to, bucket: '1m', groupBy: 'service' });

    console.log(`  refresh: ${await refresh()}`);
    await settle();

    await assertEquivalent('late rows exact after refresh',
      { since: from, until: to, bucket: '1m', groupBy: 'service' });

    const after = await groundTruth({ since: from, until: to, bucket: '1m', groupBy: 'service' });
    const delta = after.reduce((a, r) => a + r.count, 0) - before.reduce((a, r) => a + r.count, 0);
    check('all 250 late rows landed', delta === 250, `${delta}`);
  }

  console.log('\n=== refresh is idempotent ===');
  {
    const snapshotOf = async () =>
      (await client.query('SELECT bucket_start, service, level, entry_count FROM log_rollups ORDER BY 1,2,3')).rows;

    const before = JSON.stringify(await snapshotOf());
    check('extra refresh reports nothing to do', (await refresh()).includes('folded 0 rows'));
    await refresh();
    await refresh();
    check('rollup unchanged after three extra refreshes', JSON.stringify(await snapshotOf()) === before);
    await settle();
    await assertEquivalent('counts still exact after repeated refresh',
      { since: from, until: to, bucket: '1m', groupBy: 'service' });
  }

  console.log('\n=== a failed refresh cannot corrupt counts ===');
  {
    // The fold and the watermark advance are one transaction. Rolling it back
    // must leave both untouched, so the next run redoes exactly that work.
    const watermarkOf = async () =>
      (await client.query('SELECT watermark_id::text AS w FROM rollup_state WHERE id')).rows[0].w;
    const totalOf = async () =>
      (await client.query('SELECT COALESCE(sum(entry_count), 0)::text AS t FROM log_rollups')).rows[0].t;

    await ingest(Array.from({ length: 120 }, (_, i) => ({
      timestamp: iso(base + 240_000 + i * 100),
      level: 'warn',
      service: SERVICES[1],
      message: `rollback probe ${i}`,
      attributes: { run: RUN },
    })));

    const watermarkBefore = await watermarkOf();
    const totalBefore = await totalOf();

    await client.query('BEGIN');
    await client.query('SELECT refresh_log_rollups()');
    await client.query('ROLLBACK');

    check('watermark unchanged after rollback', (await watermarkOf()) === watermarkBefore);
    check('counts unchanged after rollback', (await totalOf()) === totalBefore);

    console.log(`  refresh: ${await refresh()}`);
    await settle();
    await assertEquivalent('retried refresh counts each row once',
      { since: from, until: to, bucket: '1m', groupBy: 'service' });
  }

  console.log('\n=== hot tail: data newer than the boundary ===');
  {
    // Put the boundary back where it normally sits, so freshly written rows are
    // above it and have to come from the raw table.
    await client.query('UPDATE rollup_config SET lag_seconds = 30 WHERE id');
    await refresh();
    await settle();
    await floorLapses();

    const hotBefore = await paths();
    const now = Date.now();
    await ingest(Array.from({ length: 400 }, (_, i) => ({
      timestamp: iso(now - (i % 20) * 100),
      level: pick(LEVELS),
      service: pick(SERVICES),
      message: `hot tail ${i}`,
      attributes: { run: RUN, hot: 'yes' },
    })));

    await assertEquivalent('range spanning rollup and hot tail',
      { since: from, until: iso(now + 60_000), bucket: '1m', groupBy: 'service' });
    await assertEquivalent('hot tail only',
      { since: iso(now - 5_000), until: iso(now + 60_000), bucket: '1m', groupBy: 'level' });
    await assertEquivalent('whole span, 1h buckets',
      { since: from, until: iso(now + 60_000), bucket: '1h', groupBy: 'service' });

    const stats = await request('GET', '/internal/stats');
    const hotAfter = stats.json.aggregate_path;
    check('a range spanning the boundary used both sources',
      hotAfter.hybrid > hotBefore.hybrid, `${JSON.stringify(hotBefore)} -> ${JSON.stringify(hotAfter)}`);
    check('hot tail is bounded', stats.json.rollup.hot_tail_seconds !== null &&
      stats.json.rollup.hot_tail_seconds < 180, `${stats.json.rollup.hot_tail_seconds}s`);
  }

  console.log('\n=== rollup retention ===');
  {
    // A one-day window leaves everything seeded above intact but must clear
    // anything older, and must not scan the whole table to do it.
    const { rows: [{ enforce_rollup_retention: msg }] } =
      await client.query('SELECT enforce_rollup_retention()');
    check('retention reports a cutoff', /rollup retention: cutoff=/.test(msg), msg);

    const { rows: [{ n }] } = await client.query(
      `SELECT count(*)::int AS n FROM log_rollups
       WHERE bucket_start < now() - (SELECT make_interval(days => retention_days) FROM rollup_config WHERE id)`
    );
    check('no rollup rows survive past the window', n === 0, `${n} rows`);

    await settle();
    await assertEquivalent('counts still exact after retention',
      { since: from, until: to, bucket: '1m', groupBy: 'service' });
  }

  console.log('\n=== injection attempts reach nothing ===');
  {
    for (const hostile of [
      "'; DROP TABLE log_rollups; --",
      "x' OR '1'='1",
      'service"; DELETE FROM logs WHERE ""=""',
    ]) {
      const r = await request('GET', queryString({ since: from, until: to, bucket: '1m', service: hostile }));
      check(`hostile service filter is inert: ${hostile.slice(0, 20)}`,
        r.status === 200 && r.json.buckets.length === 0, `${r.status} ${r.raw.slice(0, 120)}`);
    }
    for (const hostile of ['service; DROP TABLE log_rollups', '1;--']) {
      const r = await request('GET', queryString({ since: from, until: to, bucket: '1m', groupBy: hostile }));
      check(`hostile group_by is rejected: ${hostile.slice(0, 20)}`, r.status === 400, `${r.status}`);
    }
    const { rows: [{ n }] } = await client.query(`SELECT count(*)::int AS n FROM log_rollups`);
    check('log_rollups survived', n > 0, `${n}`);
  }
} finally {
  await client.query('UPDATE rollup_config SET lag_seconds = $1 WHERE id', [originalLag]);
  await client.end();
  agent.destroy();
}

console.log('');
if (failures.length > 0) {
  console.log(`FAILED: ${failures.length} of ${passed + failures.length} checks`);
  for (const f of failures) console.log(`  - ${f}`);
  process.exit(1);
}
console.log(`ALL PASSED: ${passed} checks passed, 0 failed`);
