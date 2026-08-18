// Exactness of `attr.<key>=<value>` against the raw table, at whatever backlog
// the sidecar indexer happens to be at.
//
//   node test/attributes.mjs [--port 8080] [--pg postgres://...] [--limit 100]
//
// Attribute filters are answered from two sources that have to add up: hashed
// tokens in log_attr_tokens for ids below the indexer's watermark, and the raw
// rows themselves at or above it. The split is invisible in the response, so a
// boundary error does not surface as an error — it surfaces as a page that is
// quietly missing the rows that were sitting on the wrong side of it, or as
// duplicates for rows counted on both.
//
// Every assertion here compares the API against ground truth computed directly
// in SQL over `logs`, with no application code in between, and prints the
// watermark and backlog alongside so the result can be read as "exact at N
// unindexed ids" rather than just "exact".
import http from 'node:http';
import pg from 'pg';

const argv = process.argv;
const num = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : Number(argv[i + 1]); };
const str = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };

const PORT = num('port', 8080);
// 55432, not 5432: see the port comment in docker-compose.yml.
const PG = str('pg', 'postgres://loguser:logpass@localhost:55432/logs');
const LIMIT = num('limit', 100);

const agent = new http.Agent({ keepAlive: true, maxSockets: 4 });
const client = new pg.Client({ connectionString: PG });

let passed = 0;
const failures = [];
const timings = [];

function get(path) {
  return new Promise((resolve, reject) => {
    const started = performance.now();
    const req = http.request({ host: '127.0.0.1', port: PORT, path, method: 'GET', agent }, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (c) => { raw += c; });
      res.on('end', () => {
        let json = null;
        try { json = JSON.parse(raw); } catch { /* left null */ }
        resolve({ status: res.statusCode, json, ms: performance.now() - started });
      });
    });
    req.on('error', reject);
    req.end();
  });
}

function post(body) {
  const payload = Buffer.from(JSON.stringify(body));
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host: '127.0.0.1', port: PORT, path: '/logs', method: 'POST', agent,
        headers: { 'content-type': 'application/json', 'content-length': payload.length } },
      (res) => {
        let raw = '';
        res.setEncoding('utf8');
        res.on('data', (c) => { raw += c; });
        res.on('end', () => resolve({ status: res.statusCode, json: JSON.parse(raw || 'null') }));
      }
    );
    req.on('error', reject);
    req.end(payload);
  });
}

function check(name, condition, detail) {
  if (condition) { passed++; return; }
  failures.push(detail === undefined ? name : `${name}: ${detail}`);
}

/** The predicate the API is contractually computing, in SQL. */
function attrSql(attributes) {
  return Object.entries(attributes)
    .map(([key, value], i) => `attributes ->> $${i * 2 + 1} = $${i * 2 + 2}`)
    .join(' AND ');
}

function attrParams(attributes) {
  return Object.entries(attributes).flat();
}

function attrQuery(attributes) {
  return Object.entries(attributes).map(([k, v]) => `attr.${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join('&');
}

async function state() {
  const { rows } = await client.query(`
    SELECT s.watermark_id::text AS w,
           GREATEST(0, COALESCE(pg_sequence_last_value('logs_id_seq'::regclass), 0) + 1 - s.watermark_id) AS backlog,
           (SELECT count(*) FROM log_attr_tokens) AS sidecar_rows
    FROM attr_index_state s WHERE s.id
  `);
  return rows[0];
}

/** GET /logs with an attribute filter, compared row for row against SQL. */
async function comparePage(name, { attributes, service, level, since, until }) {
  const conditions = [attrSql(attributes)];
  const params = attrParams(attributes);
  if (service !== undefined) { params.push(service); conditions.push(`service = $${params.length}`); }
  if (level !== undefined) { params.push(level); conditions.push(`level = $${params.length}`); }
  if (since !== undefined) { params.push(since); conditions.push(`"timestamp" >= $${params.length}::timestamptz`); }
  if (until !== undefined) { params.push(until); conditions.push(`"timestamp" < $${params.length}::timestamptz`); }

  // `AS log_id`, not `AS id`. ORDER BY binds to an output column alias in
  // preference to a table column, so `id::text AS id` would make the tiebreak
  // sort ids as text and put '97' after '101' — the very mistake the query
  // under test is written to avoid. Two rows sharing a timestamp to the
  // microsecond are enough to expose it, and the seeded data has them.
  const truth = await client.query(
    `SELECT id::text AS log_id FROM logs WHERE ${conditions.join(' AND ')}
     ORDER BY "timestamp" DESC, id DESC LIMIT ${LIMIT}`,
    params
  );

  const qs = [
    attrQuery(attributes),
    `limit=${LIMIT}`,
    service === undefined ? null : `service=${encodeURIComponent(service)}`,
    level === undefined ? null : `level=${encodeURIComponent(level)}`,
    since === undefined ? null : `since=${encodeURIComponent(since)}`,
    until === undefined ? null : `until=${encodeURIComponent(until)}`,
  ].filter((p) => p !== null).join('&');

  const res = await get(`/logs?${qs}`);
  timings.push({ name, ms: Math.round(res.ms * 10) / 10, rows: res.json?.logs?.length ?? -1 });

  check(`${name}: status`, res.status === 200, `got ${res.status}`);
  const got = (res.json?.logs ?? []).map((l) => l.id);
  const want = truth.rows.map((r) => r.log_id);
  check(
    `${name}: exact match`,
    got.length === want.length && got.every((id, i) => id === want[i]),
    `api ${got.length} rows vs sql ${want.length}; first divergence at ${got.findIndex((id, i) => id !== want[i])}`
  );
  return res.ms;
}

/** GET /logs/aggregate with an attribute filter, compared bucket for bucket. */
async function compareAggregate(name, { attributes, since, until, bucket, groupBy, service, level }) {
  const seconds = { '1m': 60, '5m': 300, '1h': 3600, '1d': 86400 }[bucket];
  const conditions = [attrSql(attributes)];
  const params = attrParams(attributes);
  params.push(since); conditions.push(`"timestamp" >= $${params.length}::timestamptz`);
  params.push(until); conditions.push(`"timestamp" < $${params.length}::timestamptz`);
  if (service !== undefined) { params.push(service); conditions.push(`service = $${params.length}`); }
  if (level !== undefined) { params.push(level); conditions.push(`level = $${params.length}`); }

  const group = groupBy === undefined ? 'NULL::text' : groupBy;
  const truth = await client.query(
    `SELECT to_char(date_bin(make_interval(secs => ${seconds}), "timestamp", TIMESTAMPTZ 'epoch') AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS start,
            ${group} AS grp, count(*)::bigint AS n
     FROM logs WHERE ${conditions.join(' AND ')} GROUP BY 1, 2 ORDER BY 1, 2`,
    params
  );

  const qs = [
    attrQuery(attributes),
    `since=${encodeURIComponent(since)}`,
    `until=${encodeURIComponent(until)}`,
    `bucket=${bucket}`,
    groupBy === undefined ? null : `group_by=${groupBy}`,
    service === undefined ? null : `service=${encodeURIComponent(service)}`,
    level === undefined ? null : `level=${encodeURIComponent(level)}`,
  ].filter((p) => p !== null).join('&');

  const res = await get(`/logs/aggregate?${qs}`);
  timings.push({ name, ms: Math.round(res.ms * 10) / 10, rows: res.json?.buckets?.length ?? -1 });

  check(`${name}: status`, res.status === 200, `got ${res.status}`);
  const got = (res.json?.buckets ?? []).map((b) => `${b.start}|${b.group ?? ''}|${b.count}`).sort();
  const want = truth.rows.map((r) => `${r.start}|${r.grp ?? ''}|${r.n}`).sort();
  check(
    `${name}: exact match`,
    got.length === want.length && got.every((row, i) => row === want[i]),
    `api ${got.length} buckets vs sql ${want.length}`
  );
  return res.ms;
}

/** Walks every page of an attribute filter and compares the whole set. */
async function comparePagination(name, attributes, since) {
  const truth = await client.query(
    `SELECT id::text AS log_id FROM logs
     WHERE ${attrSql(attributes)} AND "timestamp" >= $${Object.keys(attributes).length * 2 + 1}::timestamptz
     ORDER BY "timestamp" DESC, id DESC`,
    [...attrParams(attributes), since]
  );

  const seen = [];
  let cursor = null;
  for (let page = 0; page < 200; page++) {
    const qs = `${attrQuery(attributes)}&since=${encodeURIComponent(since)}&limit=25${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
    const res = await get(`/logs?${qs}`);
    if (res.status !== 200) { check(`${name}: page status`, false, `got ${res.status}`); return; }
    for (const l of res.json.logs) seen.push(l.id);
    cursor = res.json.next_cursor;
    if (cursor === null) break;
  }

  const want = truth.rows.map((r) => r.log_id);
  check(`${name}: walked every row once`,
    seen.length === want.length && seen.every((id, i) => id === want[i]),
    `walked ${seen.length} vs ${want.length} in SQL`);
  check(`${name}: no duplicates`, new Set(seen).size === seen.length,
    `${seen.length - new Set(seen).size} duplicate ids`);
}

async function main() {
  await client.connect();

  const before = await state();
  console.log(`[attributes] watermark=${before.w} backlog=${before.backlog} sidecar_rows=${before.sidecar_rows}`);

  // Values that exist in the data at very different frequencies, discovered
  // rather than assumed, so this works against a benchmarked database and
  // against a hand-seeded one.
  const { rows: common } = await client.query(`
    SELECT e.key, e.value, count(*) AS n
    FROM (SELECT attributes FROM logs ORDER BY id DESC LIMIT 20000) s,
         LATERAL jsonb_each_text(s.attributes) e
    WHERE e.value IS NOT NULL
    GROUP BY 1, 2 ORDER BY n DESC LIMIT 1
  `);
  const { rows: rare } = await client.query(`
    SELECT e.key, e.value, count(*) AS n
    FROM (SELECT attributes FROM logs ORDER BY id DESC LIMIT 20000) s,
         LATERAL jsonb_each_text(s.attributes) e
    WHERE e.value IS NOT NULL
    GROUP BY 1, 2 ORDER BY n ASC LIMIT 1
  `);

  if (common.length === 0) {
    console.log('[attributes] no attribute data present; nothing to compare');
    await client.end();
    return;
  }

  const frequent = { [common[0].key]: common[0].value };
  const scarce = { [rare[0].key]: rare[0].value };
  console.log(`[attributes] frequent=${JSON.stringify(frequent)} (${common[0].n}/20000)  ` +
              `scarce=${JSON.stringify(scarce)} (${rare[0].n}/20000)`);

  const { rows: bounds } = await client.query(
    `SELECT min("timestamp")::text AS oldest, max("timestamp")::text AS newest,
            (SELECT service FROM logs ORDER BY id DESC LIMIT 1) AS service FROM logs`
  );
  const newest = new Date(bounds[0].newest);
  const since = new Date(newest.getTime() - 5 * 60_000).toISOString();
  const until = new Date(newest.getTime() + 60_000).toISOString();

  // --- the required matrix, at whatever backlog the indexer is currently at
  await comparePage('GET attr only (frequent)', { attributes: frequent });
  await comparePage('GET attr only (scarce)', { attributes: scarce });
  await comparePage('GET attr + service', { attributes: frequent, service: bounds[0].service });
  await comparePage('GET attr + service + time', { attributes: frequent, service: bounds[0].service, since, until });
  await comparePage('GET attr + level + time', { attributes: frequent, level: 'error', since, until });
  await comparePage('GET attr + attr (AND)', { attributes: { ...frequent, ...scarce } });

  await compareAggregate('aggregate + attr', { attributes: frequent, since, until, bucket: '1m' });
  await compareAggregate('aggregate + attr + group_by', { attributes: frequent, since, until, bucket: '1m', groupBy: 'service' });
  await compareAggregate('aggregate + attr (scarce)', { attributes: scarce, since, until, bucket: '1m' });
  await compareAggregate('aggregate + attr + level', { attributes: frequent, since, until, bucket: '5m', level: 'error', groupBy: 'level' });

  await comparePagination('pagination over attr', scarce, since);

  // --- a row written now: unindexed by definition, and queryable regardless
  const marker = `attrtest-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const write = await post({
    logs: [{
      timestamp: new Date().toISOString(),
      level: 'warn',
      service: 'attr-test',
      message: `fresh ${marker}`,
      attributes: { probe: marker, shared: 'yes' },
    }],
  });
  check('fresh write accepted', write.status === 200 && write.json.accepted === 1, JSON.stringify(write.json));
  const fresh = await get(`/logs?attr.probe=${marker}&limit=10`);
  check('fresh row readable through the fallback',
    fresh.status === 200 && fresh.json.logs.length === 1 && fresh.json.logs[0].message === `fresh ${marker}`,
    `${fresh.status} ${JSON.stringify(fresh.json?.logs?.length)}`);

  // --- a backdated row: old timestamp, new id, so it is behind the sidecar's
  // time ordering but ahead of its watermark
  const backdated = `backdate-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const backTs = new Date(Date.now() - 45 * 60_000).toISOString();
  const backWrite = await post({
    logs: [{
      timestamp: backTs,
      level: 'error',
      service: 'attr-test',
      message: `old ${backdated}`,
      attributes: { probe: backdated },
    }],
  });
  check('backdated write accepted', backWrite.status === 200 && backWrite.json.accepted === 1);
  const backRead = await get(`/logs?attr.probe=${backdated}&limit=10`);
  check('backdated row readable immediately',
    backRead.status === 200 && backRead.json.logs.length === 1,
    `${backRead.status} ${backRead.json?.logs?.length} rows`);

  // --- and still exactly once after the indexer has taken ownership of it
  const target = BigInt(await client.query(`SELECT max(id) AS id FROM logs`).then((r) => r.rows[0].id));
  const deadline = Date.now() + 60_000;
  let crossed = false;
  while (Date.now() < deadline) {
    const now = await state();
    if (BigInt(now.w) > target) { crossed = true; break; }
    await new Promise((r) => setTimeout(r, 500));
  }

  if (!crossed) {
    console.log('[attributes] watermark did not reach the probe rows within 60s; ownership transition not exercised');
  } else {
    const after = await state();
    console.log(`[attributes] watermark advanced past the probes: ${after.w} (backlog ${after.backlog})`);
    for (const [label, probe] of [['fresh', marker], ['backdated', backdated]]) {
      const res = await get(`/logs?attr.probe=${probe}&limit=10`);
      check(`${label} row readable after indexing`, res.status === 200 && res.json.logs.length === 1,
        `${res.status} ${res.json?.logs?.length} rows`);
    }
    // The same page again, now that the rows have moved from the fallback to
    // the indexed side: still exactly one, so ownership handed over without
    // dropping or duplicating them.
    await comparePage('GET attr after ownership transition', { attributes: { probe: marker } });
  }

  const end = await state();
  console.log(`[attributes] watermark=${end.w} backlog=${end.backlog} sidecar_rows=${end.sidecar_rows}`);
  console.log('[attributes] latency');
  for (const t of timings) console.log(`  ${String(t.ms).padStart(9)} ms  ${String(t.rows).padStart(5)} rows  ${t.name}`);

  await client.end();

  console.log(`\n[attributes] ${passed} passed, ${failures.length} failed`);
  for (const f of failures) console.log(`  FAIL ${f}`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
