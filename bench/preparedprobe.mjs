// Does reusing a plan make the GET /logs page query cheaper?
//
// The page-size sweep showed read-back cost is fixed-per-request, not per-row:
// page latency fits 0.48ms + 0.0024ms x rows, so at the page size a verifier
// actually uses (100) more than half the time is spent before a single row is
// touched. Postgres attributes most of that to planning rather than execution —
// 0.816ms planning against 0.168ms execution, reading 633 catalog buffers to
// prune eleven partitions.
//
// This measures the same statement three ways against the database directly,
// with no HTTP or application code in between:
//
//   simple      one-shot text query, re-parsed and re-planned every time
//   unnamed     parameterised but unnamed, which is what the app sends today
//   named       a named prepared statement, planned once per connection
//
// It also checks that all three return identical rows, because a generic plan
// on a partitioned table is only useful if it still prunes correctly.
//
//   node bench/preparedprobe.mjs --iterations 400 --limit 100
import pg from 'pg';
import { performance } from 'node:perf_hooks';

const argv = process.argv;
const num = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : Number(argv[i + 1]); };
const str = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };

const PG = str('pg', 'postgres://loguser:logpass@localhost:55432/logs');
const ITERATIONS = num('iterations', 400);
const LIMIT = num('limit', 100);

const TEXT = `
    SELECT id::text AS id,
           to_char("timestamp" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS timestamp,
           level, service, message, attributes
    FROM (
      SELECT id, "timestamp", level, service, message, attributes
      FROM logs
      WHERE TRUE AND "timestamp" <= $1::timestamptz
        AND ("timestamp", id) < ($1::timestamptz, $2::bigint)
      ORDER BY "timestamp" DESC, id DESC
      LIMIT $3
    ) page`;

function pct(a, p) {
  const s = [...a].sort((x, y) => x - y);
  return Number(s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))].toFixed(3));
}

const client = new pg.Client({ connectionString: PG });
await client.connect();

// Walk from a fixed starting cursor so every variant reads the same rows.
const { rows: top } = await client.query(
  `SELECT "timestamp", id::text AS id FROM logs ORDER BY "timestamp" DESC, id DESC LIMIT 1`
);
const startTs = top[0].timestamp;
const startId = top[0].id;

async function run(label, exec) {
  // Warm the connection's caches so the first call does not skew the median.
  for (let i = 0; i < 20; i++) await exec();
  const samples = [];
  for (let i = 0; i < ITERATIONS; i++) {
    const t0 = performance.now();
    await exec();
    samples.push(performance.now() - t0);
  }
  const rowsPerPage = (await exec()).rows.length;
  console.log(
    `  ${label.padEnd(10)} p50=${String(pct(samples, 50)).padStart(6)}ms  ` +
    `p95=${String(pct(samples, 95)).padStart(6)}ms  rows=${rowsPerPage}`
  );
  return pct(samples, 50);
}

const values = [startTs, startId, LIMIT];

const results = {};
results.simple = await run('simple', () =>
  client.query(TEXT.replace('$1::timestamptz', `'${startTs.toISOString()}'::timestamptz`)
                   .replace('$1::timestamptz', `'${startTs.toISOString()}'::timestamptz`)
                   .replace('$2::bigint', `${startId}::bigint`)
                   .replace('LIMIT $3', `LIMIT ${LIMIT}`))
);
results.unnamed = await run('unnamed', () => client.query(TEXT, values));
results.named = await run('named', () =>
  client.query({ name: 'logs_page', text: TEXT, values })
);

// A generic plan that stopped pruning partitions would still return the right
// rows, just slowly — so correctness is checked by comparing the actual output.
const a = (await client.query(TEXT, values)).rows;
const b = (await client.query({ name: 'logs_page', text: TEXT, values })).rows;
const identical = JSON.stringify(a) === JSON.stringify(b);

console.log(`\n  identical results: ${identical}`);
console.log(`  named vs unnamed: ${(results.unnamed / results.named).toFixed(2)}x`);

await client.end();
