// Does removing driver-side jsonb parsing actually make the read path cheaper?
//
// pg-types registers JSON.parse for OID 3802, so every attributes value becomes
// a JavaScript object on the way in, and Fastify turns it straight back into the
// same text on the way out. The column decomposition showed attributes costing
// ~3.5x what the similarly-sized message column costs, which points at that
// round trip rather than at the bytes.
//
// Three ways of getting the same value are compared at the driver boundary,
// before any HTTP is involved:
//
//   A  current          jsonb, parsed by pg-types into an object
//   B  ::text cast      SQL returns text, so the driver has nothing to parse
//   C  per-query types  pg's documented per-query parser override, no SQL change
//
// D and E then add back the work the response actually needs, so the comparison
// is honest about what a real endpoint would still have to do:
//
//   D  B + JSON.parse in application code (proves the cost merely moved)
//   E  A + JSON.stringify of the whole page (what the response path really does)
//
//   node bench/jsonbprobe.mjs --limit 100
import pg from 'pg';

const argv = process.argv;
const num = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : Number(argv[i + 1]); };
const str = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };

const PG = str('pg', 'postgres://loguser:logpass@localhost:55432/logs');
const LIMITS = str('limits', '100,250,500,1000').split(',').map(Number);
const N = num('n', 30);

const client = new pg.Client({ connectionString: PG });
await client.connect();

const { rows: top } = await client.query(
  `SELECT "timestamp", id::text AS id FROM logs ORDER BY "timestamp" DESC, id DESC LIMIT 1`
);
const ts = top[0].timestamp, id = top[0].id;

const TS_FMT = `to_char("timestamp" AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;
const page = (attrExpr) => `
  SELECT id::text AS id, ${TS_FMT} AS timestamp, level, service, message, ${attrExpr} AS attributes
  FROM (SELECT * FROM logs WHERE "timestamp" <= $1 AND ("timestamp", id) < ($1, $2::bigint)
        ORDER BY "timestamp" DESC, id DESC LIMIT $3) p`;

// pg's per-query override: hand back the raw text for jsonb instead of parsing.
// Scoped to this query only — the pool's global parsers are untouched.
const rawJsonbTypes = {
  getTypeParser: (oid, format) =>
    oid === 3802 || oid === 114 ? ((v) => v) : pg.types.getTypeParser(oid, format),
};

function med(a) { const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length / 2)]; }

async function bench(label, run, limit) {
  for (let i = 0; i < 5; i++) await run(limit);
  const t = [];
  for (let i = 0; i < N; i++) {
    const s = process.hrtime.bigint();
    await run(limit);
    t.push(Number(process.hrtime.bigint() - s) / 1e6);
  }
  const p50 = med(t);
  return { label, p50, rows_per_sec: Math.round(limit / (p50 / 1000)) };
}

console.log('variant'.padEnd(34) + ['limit', 'p50ms', 'rows/s'].map((h) => h.padStart(9)).join(''));

for (const limit of LIMITS) {
  const variants = [
    ['A jsonb parsed (current)', async (l) =>
      client.query(page('attributes'), [ts, id, l])],
    ['B ::text cast, no parse', async (l) =>
      client.query(page('attributes::text'), [ts, id, l])],
    ['C per-query types override', async (l) =>
      client.query({ text: page('attributes'), values: [ts, id, l], types: rawJsonbTypes })],
    ['D B + JSON.parse in app', async (l) => {
      const r = await client.query(page('attributes::text'), [ts, id, l]);
      for (const row of r.rows) row.attributes = JSON.parse(row.attributes);
      return r;
    }],
    ['E A + JSON.stringify page', async (l) => {
      const r = await client.query(page('attributes'), [ts, id, l]);
      JSON.stringify({ logs: r.rows, next_cursor: null });
      return r;
    }],
    ['F C + JSON.stringify page', async (l) => {
      const r = await client.query({ text: page('attributes'), values: [ts, id, l], types: rawJsonbTypes });
      // attributes is raw JSON text here, so stringifying the page would quote
      // it. Measured only to size the stringify cost, not as a usable response.
      JSON.stringify({ logs: r.rows, next_cursor: null });
      return r;
    }],
  ];

  for (const [label, run] of variants) {
    const r = await bench(label, run, limit);
    console.log(label.padEnd(34) + String(limit).padStart(9) + r.p50.toFixed(2).padStart(9) + String(r.rows_per_sec).padStart(9));
  }
  console.log('');
}

// Sanity: the override must yield the same data, just unparsed.
const a = (await client.query(page('attributes'), [ts, id, 3])).rows;
const c = (await client.query({ text: page('attributes'), values: [ts, id, 3], types: rawJsonbTypes })).rows;
console.log('override returns raw text :', typeof c[0].attributes === 'string');
console.log('same logical value        :',
  a.every((r, i) => JSON.stringify(r.attributes) === JSON.stringify(JSON.parse(c[i].attributes))));

await client.end();
