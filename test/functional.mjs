// Functional check of the whole API surface against the contract.
//   node test/functional.mjs [--port 8080]
//
// Exits non-zero if anything fails. Prints the actual JSON for the response
// shapes so they can be diffed against the spec by eye.
import http from 'node:http';

const PORT = (() => {
  const i = process.argv.indexOf('--port');
  return i === -1 ? 8080 : Number(process.argv[i + 1]);
})();

const agent = new http.Agent({ keepAlive: true, maxSockets: 8 });
let passed = 0;
const failures = [];

function request(method, path, body) {
  const payload = body === undefined ? null : Buffer.from(typeof body === 'string' ? body : JSON.stringify(body));
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
    req.end(payload);
  });
}

function check(name, condition, detail) {
  if (condition) { passed++; return; }
  failures.push(`${name}${detail ? ` — ${detail}` : ''}`);
  console.log(`  FAIL  ${name}${detail ? ` — ${detail}` : ''}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const iso = (offsetMs = 0) => new Date(Date.now() + offsetMs).toISOString();

const RUN = `fn-${Date.now()}`;
function entry(over = {}) {
  return {
    timestamp: iso(-1000),
    level: 'info',
    service: 'checkout',
    message: 'baseline message',
    attributes: { run: RUN, user_id: '42' },
    ...over,
  };
}

console.log('\n=== POST /logs: validation and batch semantics ===');
{
  const r = await request('POST', '/logs', { logs: [entry(), entry({ level: 'error' })] });
  check('all-valid batch returns 200', r.status === 200, `got ${r.status}`);
  check('accepted count is 2', r.json?.accepted === 2, JSON.stringify(r.json));
  check('rejected is an empty array', Array.isArray(r.json?.rejected) && r.json.rejected.length === 0);

  const partial = await request('POST', '/logs', {
    logs: [
      entry(),
      entry({ level: 'critical' }),
      entry({ service: '' }),
      entry({ message: '   ' }),
      entry({ timestamp: 'not-a-date' }),
      entry({ timestamp: iso(10 * 60 * 1000) }),
      entry({ attributes: { nested: { a: 1 } } }),
      entry({ attributes: { arr: [1, 2] } }),
      entry({ timestamp: undefined }),
      'not-an-object',
    ],
  });
  check('partial batch returns 200', partial.status === 200, `got ${partial.status}`);
  check('partial batch accepted 1', partial.json?.accepted === 1, JSON.stringify(partial.json?.accepted));
  check('partial batch rejected 9', partial.json?.rejected?.length === 9, `got ${partial.json?.rejected?.length}`);
  const idx = (partial.json?.rejected ?? []).map((r) => r.index);
  check('rejection indices are the offending positions',
    JSON.stringify(idx) === JSON.stringify([1, 2, 3, 4, 5, 6, 7, 8, 9]), JSON.stringify(idx));
  check('every rejection carries a reason string',
    (partial.json?.rejected ?? []).every((r) => typeof r.reason === 'string' && r.reason.length > 0));
  console.log('  rejected sample:', JSON.stringify(partial.json?.rejected?.slice(0, 3)));

  const allBad = await request('POST', '/logs', { logs: [entry({ level: 'nope' })] });
  check('all-rejected batch returns 400', allBad.status === 400, `got ${allBad.status}`);
  check('all-rejected body has error', typeof allBad.json?.error === 'string');

  const malformed = await request('POST', '/logs', '{"logs": [');
  check('malformed JSON returns 400', malformed.status === 400, `got ${malformed.status}`);

  for (const [label, body] of [
    ['array top level', [entry()]],
    ['logs not an array', { logs: 'x' }],
    ['logs missing', { entries: [] }],
    ['empty logs array', { logs: [] }],
  ]) {
    const r2 = await request('POST', '/logs', body);
    check(`wrong top-level structure (${label}) returns 400`, r2.status === 400, `got ${r2.status}`);
  }

  // A batch of one is valid, and boundary values must be accepted.
  const one = await request('POST', '/logs', {
    logs: [entry({ attributes: { s: 'x', n: 3, b: true, f: 1.5, neg: -2 } })],
  });
  check('batch of one with mixed scalar attributes accepted', one.status === 200 && one.json.accepted === 1, JSON.stringify(one.json));

  const noAttrs = await request('POST', '/logs', { logs: [{ timestamp: iso(-500), level: 'warn', service: 'auth', message: 'no attrs' }] });
  check('attributes omitted is accepted', noAttrs.status === 200 && noAttrs.json.accepted === 1, JSON.stringify(noAttrs.json));

  const nearFuture = await request('POST', '/logs', { logs: [entry({ timestamp: iso(4 * 60 * 1000) })] });
  check('timestamp 4 minutes ahead accepted', nearFuture.status === 200 && nearFuture.json.accepted === 1);
}

// Seed a known, isolated dataset for the read tests.
console.log('\n=== seeding query fixtures ===');
const SEED_SERVICE = `svc-${RUN}`;
{
  const base = Date.now() - 60_000;
  const logs = [];
  for (let i = 0; i < 250; i++) {
    logs.push({
      // Deliberate timestamp collisions (5 rows share each value) so the
      // tiebreaker in the ordering is actually exercised by pagination.
      timestamp: new Date(base + Math.floor(i / 5) * 1000).toISOString(),
      level: ['debug', 'info', 'warn', 'error'][i % 4],
      service: SEED_SERVICE,
      message: i % 3 === 0 ? `NeedleCase ${i}` : `ordinary line ${i}`,
      attributes: { run: RUN, bucket: String(i % 7), flag: i % 2 === 0, count: i },
    });
  }
  const r = await request('POST', '/logs', { logs });
  check('seed batch of 250 accepted', r.status === 200 && r.json.accepted === 250, JSON.stringify(r.json));
  await sleep(1500); // well inside the 20s visibility requirement
}

console.log('\n=== GET /logs: response shape ===');
{
  const r = await request('GET', `/logs?service=${SEED_SERVICE}&limit=2`);
  console.log('  ', JSON.stringify(r.json, null, 2).split('\n').slice(0, 14).join('\n  '));
  check('status 200', r.status === 200);
  check('logs is an array', Array.isArray(r.json?.logs));
  check('next_cursor key present', r.json && 'next_cursor' in r.json);
  const row = r.json?.logs?.[0];
  check('id is a string', typeof row?.id === 'string', `got ${typeof row?.id}`);
  check('timestamp is ISO-8601 with ms and Z',
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(row?.timestamp ?? ''), row?.timestamp);
  check('level/service/message are strings',
    ['level', 'service', 'message'].every((k) => typeof row?.[k] === 'string'));
  check('attributes is a plain object', row?.attributes && typeof row.attributes === 'object' && !Array.isArray(row.attributes));
  check('no unexpected keys on a log row',
    JSON.stringify(Object.keys(row ?? {}).sort()) === JSON.stringify(['attributes', 'id', 'level', 'message', 'service', 'timestamp']),
    JSON.stringify(Object.keys(row ?? {})));
  check('next_cursor is a string when more pages exist', typeof r.json?.next_cursor === 'string');

  const last = await request('GET', `/logs?service=${SEED_SERVICE}&limit=1000`);
  check('next_cursor is null on the final page', last.json?.next_cursor === null, JSON.stringify(last.json?.next_cursor));

  const empty = await request('GET', '/logs?service=does-not-exist-anywhere');
  check('empty result still returns logs:[] and next_cursor:null',
    Array.isArray(empty.json?.logs) && empty.json.logs.length === 0 && empty.json.next_cursor === null,
    JSON.stringify(empty.json));

  // A page that is exactly `limit` rows long but has nothing after it must
  // still report a null cursor.
  const exact = await request('GET', `/logs?service=${SEED_SERVICE}&limit=250`);
  check('exactly-full final page reports next_cursor null',
    exact.json?.logs?.length === 250 && exact.json?.next_cursor === null,
    `rows=${exact.json?.logs?.length} cursor=${exact.json?.next_cursor}`);
}

console.log('\n=== GET /logs: ordering and cursor pagination ===');
{
  const seen = [];
  let cursor = null;
  let pages = 0;
  do {
    const url = `/logs?service=${SEED_SERVICE}&limit=37${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
    const r = await request('GET', url);
    if (r.status !== 200) { check('pagination page returns 200', false, `status ${r.status} on page ${pages}`); break; }
    seen.push(...r.json.logs);
    cursor = r.json.next_cursor;
    pages++;
  } while (cursor && pages < 40);

  check('walked more than one page', pages > 1, `pages=${pages}`);
  check('walk collected exactly the 250 seeded rows', seen.length === 250, `got ${seen.length}`);
  check('no duplicate ids across pages', new Set(seen.map((l) => l.id)).size === seen.length);
  const ordered = seen.every((row, i) => {
    if (i === 0) return true;
    const prev = seen[i - 1];
    if (prev.timestamp > row.timestamp) return true;
    return prev.timestamp === row.timestamp && BigInt(prev.id) > BigInt(row.id);
  });
  check('strictly ordered by (timestamp DESC, id DESC) across page boundaries', ordered);
}

console.log('\n=== GET /logs: filters ===');
{
  const cases = [
    [`service=${SEED_SERVICE}&level=error`, (rows) => rows.every((r) => r.level === 'error'), 'level filter'],
    [`service=${SEED_SERVICE}&attr.bucket=3`, (rows) => rows.every((r) => r.attributes.bucket === '3'), 'attr string equality'],
    [`service=${SEED_SERVICE}&attr.flag=true`, (rows) => rows.every((r) => r.attributes.flag === true), 'attr boolean compared as string'],
    [`service=${SEED_SERVICE}&attr.count=7`, (rows) => rows.every((r) => r.attributes.count === 7), 'attr number compared as string'],
    [`service=${SEED_SERVICE}&q=needlecase`, (rows) => rows.every((r) => /NeedleCase/.test(r.message)), 'q is case-insensitive'],
    [`service=${SEED_SERVICE}&attr.run=${RUN}&level=warn&q=ordinary`,
      (rows) => rows.every((r) => r.level === 'warn' && /ordinary/.test(r.message)), 'combined filters'],
  ];
  for (const [qs, predicate, label] of cases) {
    const r = await request('GET', `/logs?${qs}&limit=1000`);
    check(`${label} returns 200`, r.status === 200, `got ${r.status}`);
    check(`${label} returns matching rows only`, r.json?.logs?.length > 0 && predicate(r.json.logs),
      `n=${r.json?.logs?.length}`);
  }

  const since = new Date(Date.now() - 30_000).toISOString();
  const rangeAll = await request('GET', `/logs?service=${SEED_SERVICE}&limit=1000`);
  const inRange = await request('GET', `/logs?service=${SEED_SERVICE}&since=${since}&limit=1000`);
  check('since narrows the result set', inRange.json.logs.length < rangeAll.json.logs.length,
    `${inRange.json.logs.length} vs ${rangeAll.json.logs.length}`);
  check('since is inclusive of its own boundary', inRange.json.logs.every((r) => r.timestamp >= since.replace(/(\.\d{3})\d*Z$/, '$1Z')));

  const until = new Date(Date.now() - 55_000).toISOString();
  const untilRes = await request('GET', `/logs?service=${SEED_SERVICE}&until=${until}&limit=1000`);
  check('until is exclusive', untilRes.json.logs.every((r) => r.timestamp < until));

  const q = await request('GET', `/logs?q=${encodeURIComponent('%')}&limit=5`);
  check('a LIKE wildcard in q is treated literally', q.status === 200 && q.json.logs.length === 0,
    `status=${q.status} n=${q.json?.logs?.length}`);
}

console.log('\n=== GET /logs: 400 cases ===');
{
  const bad = [
    ['since=not-a-date', 'invalid since'],
    ['until=nonsense', 'invalid until'],
    [`since=${iso(0)}&until=${iso(-60000)}`, 'until earlier than since'],
    ['level=critical', 'unsupported level'],
    ['limit=abc', 'non-numeric limit'],
    ['limit=0', 'limit below range'],
    ['limit=1001', 'limit above range'],
    ['limit=-5', 'negative limit'],
    ['cursor=not-base64!!', 'malformed cursor'],
    [`cursor=${Buffer.from('{"id":"1"}').toString('base64')}`, 'cursor missing timestamp'],
    [`cursor=${Buffer.from('[]').toString('base64')}`, 'cursor is not an object'],
    ['attr.=x', 'empty attribute key'],
  ];
  for (const [qs, label] of bad) {
    const r = await request('GET', `/logs?${qs}`);
    check(`${label} returns 400`, r.status === 400, `got ${r.status}`);
    check(`${label} body is {error: string}`, typeof r.json?.error === 'string', JSON.stringify(r.json));
  }
}

console.log('\n=== GET /logs/aggregate ===');
{
  const until = new Date(Date.now() + 60_000).toISOString();
  const since = new Date(Date.now() - 3600_000).toISOString();

  for (const bucket of ['1m', '5m', '1h', '1d']) {
    const r = await request('GET', `/logs/aggregate?since=${since}&until=${until}&bucket=${bucket}`);
    check(`bucket=${bucket} returns 200`, r.status === 200, `got ${r.status}`);
    check(`bucket=${bucket} returns buckets array`, Array.isArray(r.json?.buckets));
    const b = r.json?.buckets?.[0];
    check(`bucket=${bucket} start has no fractional seconds`,
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(b?.start ?? ''), b?.start);
    check(`bucket=${bucket} count is a JSON number`, typeof b?.count === 'number', `got ${typeof b?.count}`);
    check(`bucket=${bucket} group is null without group_by`, b?.group === null, JSON.stringify(b?.group));
    check(`bucket=${bucket} row has exactly start/group/count`,
      JSON.stringify(Object.keys(b ?? {}).sort()) === JSON.stringify(['count', 'group', 'start']),
      JSON.stringify(Object.keys(b ?? {})));
    const starts = (r.json?.buckets ?? []).map((x) => x.start);
    check(`bucket=${bucket} ordered ascending by start`,
      starts.every((s, i) => i === 0 || starts[i - 1] <= s));
  }
  const one = await request('GET', `/logs/aggregate?since=${since}&until=${until}&bucket=1m`);
  console.log('  no group_by:', JSON.stringify(one.json.buckets.slice(0, 2)));

  for (const groupBy of ['service', 'level']) {
    const r = await request('GET', `/logs/aggregate?since=${since}&until=${until}&bucket=1h&group_by=${groupBy}`);
    check(`group_by=${groupBy} returns 200`, r.status === 200);
    check(`group_by=${groupBy} populates group`, r.json.buckets.every((b) => typeof b.group === 'string'));
    check(`group_by=${groupBy} counts are numbers`, r.json.buckets.every((b) => typeof b.count === 'number'));
    console.log(`  group_by=${groupBy}:`, JSON.stringify(r.json.buckets.slice(0, 3)));
  }

  const filtered = await request('GET',
    `/logs/aggregate?since=${since}&until=${until}&bucket=1h&service=${SEED_SERVICE}&level=error&attr.run=${RUN}`);
  check('aggregate honours service/level/attr filters', filtered.status === 200 && filtered.json.buckets.length > 0);
  const total = filtered.json.buckets.reduce((s, b) => s + b.count, 0);
  check('filtered aggregate total matches the seeded error rows', total === 62, `got ${total}`);

  const qAgg = await request('GET', `/logs/aggregate?since=${since}&until=${until}&bucket=1h&service=${SEED_SERVICE}&q=needlecase`);
  check('aggregate honours q', qAgg.status === 200 && qAgg.json.buckets.reduce((s, b) => s + b.count, 0) === 84,
    `got ${qAgg.json?.buckets?.reduce((s, b) => s + b.count, 0)}`);

  const bad = [
    [`until=${until}&bucket=1m`, 'missing since'],
    [`since=${since}&bucket=1m`, 'missing until'],
    [`since=${since}&until=${until}`, 'missing bucket'],
    [`since=${since}&until=${until}&bucket=30s`, 'unsupported bucket'],
    [`since=${since}&until=${until}&bucket=1m&group_by=message`, 'unsupported group_by'],
    [`since=bad&until=${until}&bucket=1m`, 'invalid since'],
    [`since=${until}&until=${since}&bucket=1m`, 'until earlier than since'],
    [`since=${since}&until=${until}&bucket=1m&level=critical`, 'unsupported level'],
  ];
  for (const [qs, label] of bad) {
    const r = await request('GET', `/logs/aggregate?${qs}`);
    check(`aggregate ${label} returns 400`, r.status === 400, `got ${r.status}`);
    check(`aggregate ${label} body is {error: string}`, typeof r.json?.error === 'string');
  }
}

console.log('\n=== cursor round-trip ===');
{
  const first = await request('GET', `/logs?service=${SEED_SERVICE}&limit=10`);
  const cursor = first.json.next_cursor;
  const a = await request('GET', `/logs?service=${SEED_SERVICE}&limit=10&cursor=${encodeURIComponent(cursor)}`);
  const b = await request('GET', `/logs?service=${SEED_SERVICE}&limit=10&cursor=${encodeURIComponent(cursor)}`);
  check('cursor passed back verbatim is accepted', a.status === 200, `got ${a.status}`);
  check('cursor is stable across identical requests', JSON.stringify(a.json.logs) === JSON.stringify(b.json.logs));
  check('cursor page does not overlap the first page',
    !a.json.logs.some((r) => first.json.logs.some((f) => f.id === r.id)));
}

console.log('\n=== health ===');
{
  const r = await request('GET', '/health');
  check('health returns 200', r.status === 200, `got ${r.status}`);
}

console.log(`\n${failures.length === 0 ? 'ALL PASSED' : 'FAILURES'}: ${passed} checks passed, ${failures.length} failed`);
for (const f of failures) console.log(`  - ${f}`);
agent.destroy();
process.exit(failures.length === 0 ? 0 : 1);
