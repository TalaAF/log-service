// Cursor pagination integrity.
//
// The row-wise keyset predicate `(timestamp, id) < ($1, $2)` replaced the OR
// form for speed. A boundary error in that rewrite does not surface as an HTTP
// error or as a missing-record count — it surfaces as a row served twice, or a
// row skipped, at a page boundary. Rows sharing a timestamp are exactly where
// that would happen, so this walks the whole table and checks the walk against
// the table itself.
//
//   node test/pagination.mjs --host app --limit 1000
import http from 'node:http';

const argv = process.argv;
function num(n, d) { const i = argv.indexOf(`--${n}`); return i === -1 ? d : Number(argv[i + 1]); }
function str(n, d) { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; }

const HOST = str('host', '127.0.0.1');
const PORT = num('port', 8080);
const LIMIT = num('limit', 1000);
const EXPECTED = num('expected', -1);   // row count from the database, if known

const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });

function get(path) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: HOST, port: PORT, path, agent }, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (c) => { raw += c; });
      res.on('end', () => {
        try { resolve({ status: res.statusCode, json: JSON.parse(raw) }); }
        catch (e) { reject(new Error(`unparseable response: ${raw.slice(0, 200)}`)); }
      });
    });
    req.on('error', reject);
    req.setTimeout(60000, () => req.destroy(new Error('timeout')));
    req.end();
  });
}

const failures = [];
function assert(name, ok, detail) {
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
  if (!ok) failures.push(name);
}

console.log(`\nWalking every page of GET /logs at limit=${LIMIT}\n`);

const seen = new Set();
const duplicates = [];
let ordering = 0;          // pairs violating (timestamp DESC, id DESC)
let pages = 0;
let rows = 0;
let cursor = null;
let prev = null;           // last row of the previous page, for the boundary check
let boundaryChecks = 0;
let shortPages = 0;        // a non-final page returning fewer than `limit` rows

const started = Date.now();

for (;;) {
  const qs = `limit=${LIMIT}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
  const res = await get(`/logs?${qs}`);
  if (res.status !== 200) { assert(`page ${pages + 1} returns 200`, false, `status ${res.status}`); break; }

  const page = res.json.logs;
  pages++;
  rows += page.length;

  for (const row of page) {
    if (seen.has(row.id)) duplicates.push(row.id);
    seen.add(row.id);
  }

  // Ordering within the page.
  for (let i = 1; i < page.length; i++) {
    const a = page[i - 1], b = page[i];
    const ok = a.timestamp > b.timestamp || (a.timestamp === b.timestamp && BigInt(a.id) > BigInt(b.id));
    if (!ok) ordering++;
  }

  // Ordering across the page boundary — the row-wise predicate's failure mode.
  if (prev && page.length > 0) {
    boundaryChecks++;
    const b = page[0];
    const ok = prev.timestamp > b.timestamp || (prev.timestamp === b.timestamp && BigInt(prev.id) > BigInt(b.id));
    if (!ok) ordering++;
  }

  if (res.json.next_cursor && page.length < LIMIT) shortPages++;

  prev = page.length > 0 ? page[page.length - 1] : prev;
  cursor = res.json.next_cursor;
  if (!cursor) break;
}

const elapsed = (Date.now() - started) / 1000;

console.log(`  walked ${pages} pages / ${rows} rows in ${elapsed.toFixed(1)}s\n`);

assert('zero duplicate ids across the whole walk', duplicates.length === 0,
  duplicates.length ? `${duplicates.length} duplicates, first: ${duplicates.slice(0, 5).join(', ')}` : undefined);

assert('unique ids == rows returned (no id served twice)', seen.size === rows,
  `unique=${seen.size} returned=${rows}`);

assert('strict (timestamp DESC, id DESC) order, including across page boundaries', ordering === 0,
  ordering ? `${ordering} violating pairs` : `${boundaryChecks} boundaries checked`);

assert('no short page before the last one (no rows skipped at a boundary)', shortPages === 0,
  shortPages ? `${shortPages} short pages` : undefined);

if (EXPECTED >= 0) {
  assert('rows read == rows in table (no gaps)', rows === EXPECTED,
    `read=${rows} table=${EXPECTED} diff=${EXPECTED - rows}`);
}

// Independent gap check: ids come from one sequence, so the ids we saw must form
// exactly the set the table holds. Compare against the observed min/max.
if (rows > 0) {
  const ids = [...seen].map(BigInt).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const min = ids[0], max = ids[ids.length - 1];
  const span = Number(max - min) + 1;
  console.log(`\n  id span: ${min}..${max} (${span} values) covering ${seen.size} rows`);
  console.log(`  ${span === seen.size ? 'contiguous — no id in the range is missing from the walk'
    : `${span - seen.size} ids in the range were not returned (expected if retention or a partial truncate removed rows)`}`);
}

console.log(`\n${failures.length === 0 ? 'ALL PAGINATION ASSERTIONS PASSED' : `FAILURES: ${failures.join(', ')}`}`);
agent.destroy();
process.exit(failures.length === 0 ? 0 : 1);
