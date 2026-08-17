// Serialization A/B for the GET /logs page, on realistic wide rows.
//
// The read path spends most of its time above the driver, and the expensive part
// is JSON.stringify walking the 24-key attributes object once per row. Holding
// attributes as raw JSON text made a page-level stringify ~35% cheaper, but that
// output is wrong (attributes come out as a quoted string), so the question is
// whether a correct raw-JSON emission can keep the saving.
//
// An earlier hand-built serializer was 14% slower — on 30-byte messages with 4
// attributes, and because it called JSON.stringify once per field, six times a
// row. That is the mistake to avoid, not the idea. Variant C therefore makes
// exactly one native stringify per row, over the metadata object, and splices
// the attributes text in. Native escaping still handles message, service and the
// rest; nothing is hand-escaped.
//
//   node bench/serialprobe.mjs --limits 100,1000 --n 40
import pg from 'pg';

const argv = process.argv;
const num = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : Number(argv[i + 1]); };
const str = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };

const PG = str('pg', 'postgres://loguser:logpass@localhost:55432/logs');
const LIMITS = str('limits', '100,250,500,1000').split(',').map(Number);
const N = num('n', 40);

const client = new pg.Client({ connectionString: PG });
await client.connect();
const { rows: top } = await client.query(
  `SELECT "timestamp", id::text AS id FROM logs ORDER BY "timestamp" DESC, id DESC LIMIT 1`
);
const ts = top[0].timestamp, id = top[0].id;

const TS = `to_char("timestamp" AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;
const pageSql = (attr) => `
  SELECT id::text AS id, ${TS} AS timestamp, level, service, message, ${attr} AS attributes
  FROM (SELECT * FROM logs WHERE "timestamp" <= $1 AND ("timestamp", id) < ($1, $2::bigint)
        ORDER BY "timestamp" DESC, id DESC LIMIT $3) p`;

// ------------------------------------------------------------ the three paths
/** A: current production shape — attributes is a parsed object. */
function serializeA(rows, cursor) {
  return JSON.stringify({ logs: rows, next_cursor: cursor });
}

/** B: diagnostic only. attributes stays a string, so the JSON is *wrong*. */
function serializeB(rows, cursor) {
  return JSON.stringify({ logs: rows, next_cursor: cursor });
}

/**
 * C: one native stringify per row plus a splice.
 *
 * The metadata object goes through JSON.stringify, so quotes, backslashes,
 * control characters and Unicode in `message`/`service` are escaped by the
 * engine rather than by hand. The closing brace is replaced with the attributes
 * member, whose value is the text Postgres produced for a jsonb column and is
 * therefore already valid JSON.
 */
function serializeC(rows, cursor) {
  const parts = new Array(rows.length);
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const head = JSON.stringify({
      id: r.id, timestamp: r.timestamp, level: r.level, service: r.service, message: r.message,
    });
    parts[i] = head.slice(0, -1) + ',"attributes":' + r.attributes + '}';
  }
  return '{"logs":[' + parts.join(',') + '],"next_cursor":' +
    (cursor === null ? 'null' : JSON.stringify(cursor)) + '}';
}

const rawTypes = {
  getTypeParser: (oid, fmt) =>
    oid === 3802 || oid === 114 ? ((v) => v) : pg.types.getTypeParser(oid, fmt),
};

function med(a) { const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length / 2)]; }
function p95(a) { const s = [...a].sort((x, y) => x - y); return s[Math.floor(0.95 * s.length)]; }

console.log('variant'.padEnd(26) + ['limit', 'ser p50', 'ser p95', 'bytes', 'rows/s(ser)'].map((h) => h.padStart(12)).join(''));

const results = {};
for (const limit of LIMITS) {
  const objRows = (await client.query(pageSql('attributes'), [ts, id, limit])).rows;
  const rawRows = (await client.query({ text: pageSql('attributes'), values: [ts, id, limit], types: rawTypes })).rows;

  for (const [label, rows, fn] of [
    ['A parsed obj + stringify', objRows, serializeA],
    ['B raw string (wrong JSON)', rawRows, serializeB],
    ['C raw JSON spliced', rawRows, serializeC],
  ]) {
    for (let i = 0; i < 10; i++) fn(rows, null);
    const t = [];
    for (let i = 0; i < N; i++) {
      const s = process.hrtime.bigint();
      fn(rows, null);
      t.push(Number(process.hrtime.bigint() - s) / 1e6);
    }
    const m = med(t), bytes = fn(rows, null).length;
    results[`${label}|${limit}`] = m;
    console.log(label.padEnd(26) + String(limit).padStart(12) + m.toFixed(3).padStart(12) +
      p95(t).toFixed(3).padStart(12) + String(bytes).padStart(12) +
      String(Math.round(limit / (m / 1000))).padStart(12));
  }
  const a = results[`A parsed obj + stringify|${limit}`], c = results[`C raw JSON spliced|${limit}`];
  console.log(`  -> C vs A at limit=${limit}: ${(a / c).toFixed(2)}x faster serialization\n`);
}

// ----------------------------------------------------------- correctness gate
// C must produce output semantically identical to A over adversarial values.
const probe = await client.query(`
  SELECT '1'::text AS id, '2026-01-01T00:00:00.000Z'::text AS timestamp,
         'info'::text AS level, $1::text AS service, $2::text AS message, $3::jsonb AS attributes`,
  ['svc"with\\quote', 'msg "q" \\ back \t tab \n newline \r cr ünïcode 日本語 🚀 عربى Кириллица',
   JSON.stringify({
     empty: {}, nested: { a: { b: [1, 2, { c: null }] } }, arr: [1, 'two', false, null],
     s: 'quote " backslash \\ tab \t newline \n cr \r', uni: 'É 日本語 🚀 عربى', num: -1.5e10,
     big: 'x'.repeat(5000), t: true, f: false, z: null, 'key"with\\quote': 'v',
   })]);
const objProbe = (await client.query({
  text: `SELECT '1'::text AS id, '2026-01-01T00:00:00.000Z'::text AS timestamp,
         'info'::text AS level, $1::text AS service, $2::text AS message, $3::jsonb AS attributes`,
  values: [probe.rows[0].service, probe.rows[0].message, probe.rows[0].attributes],
})).rows;
const rawProbe = (await client.query({
  text: `SELECT '1'::text AS id, '2026-01-01T00:00:00.000Z'::text AS timestamp,
         'info'::text AS level, $1::text AS service, $2::text AS message, $3::jsonb AS attributes`,
  values: [probe.rows[0].service, probe.rows[0].message, probe.rows[0].attributes],
  types: rawTypes,
})).rows;

const outA = serializeA(objProbe, 'cur"sor');
const outC = serializeC(rawProbe, 'cur"sor');
console.log('C output parses            :', (() => { try { JSON.parse(outC); return true; } catch { return false; } })());
console.log('C deep-equals A            :', JSON.stringify(JSON.parse(outC)) === JSON.stringify(JSON.parse(outA)));
console.log('C byte-identical to A      :', outC === outA);

await client.end();
