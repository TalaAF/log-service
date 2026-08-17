// Read-back decomposition: how much of the GET /logs ceiling is the client?
//
// The wide-row ceiling measures ~13-17k rows/s end to end, but only ~2.6 us/row
// of that is accounted for by the database, driver and serialization. The rest
// was attributed to "HTTP/framework/network/client" without separating them —
// and the harness parses every ~190KB page and then walks it to verify ordering
// and duplicates, which is real client CPU counted against the server.
//
// Three client modes over the identical request sequence:
//
//   B   discard   drain the socket fully, never parse or inspect
//   A1  buffer    collect into Buffers, concat once, parse, verify
//   A2  string    collect by string concatenation (what the current harness
//                 does), parse, verify
//
// Every phase is timed inside the same request — receive to last byte, then
// parse, then verify — so nothing is derived by subtracting percentiles, which
// has already produced two wrong conclusions in this investigation.
//
//   node bench/readmodes.mjs --host app --limits 100,1000 --pages 60
import http from 'node:http';

const argv = process.argv;
const num = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : Number(argv[i + 1]); };
const str = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };

const HOST = str('host', '127.0.0.1');
const PORT = num('port', 8080);
const LIMITS = str('limits', '100,250,500,1000').split(',').map(Number);
const PAGES = num('pages', 60);

const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });

/** Verification comparable to the drain check: shape, ordering, duplicate ids. */
function verify(parsed, seen) {
  let last = null;
  for (const l of parsed.logs) {
    if (typeof l.id !== 'string') return 'id type';
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(l.timestamp)) return 'timestamp';
    if (l.attributes === null || typeof l.attributes !== 'object') return 'attributes';
    if (seen.has(l.id)) return 'duplicate';
    seen.add(l.id);
    const key = l.timestamp + l.id.padStart(20, '0');
    if (last !== null && key > last) return 'order';
    last = key;
  }
  return null;
}

function request(path, mode, seen) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = http.request({ host: HOST, port: PORT, path, agent }, (res) => {
      let bytes = 0;
      const chunks = mode === 'A1' ? [] : null;
      let text = '';

      if (mode === 'B') {
        // Fully drain without touching the payload.
        res.on('data', (c) => { bytes += c.length; });
      } else if (mode === 'A1') {
        res.on('data', (c) => { bytes += c.length; chunks.push(c); });
      } else {
        res.setEncoding('utf8');
        res.on('data', (c) => { bytes += Buffer.byteLength(c); text += c; });
      }

      res.on('end', () => {
        const tRecvEnd = performance.now();
        let parseMs = 0, verifyMs = 0, cursor = null, problem = null;

        if (mode !== 'B') {
          const body = mode === 'A1' ? Buffer.concat(chunks).toString('utf8') : text;
          const p0 = performance.now();
          let parsed;
          try { parsed = JSON.parse(body); } catch { problem = 'unparseable'; }
          parseMs = performance.now() - p0;
          if (parsed) {
            const v0 = performance.now();
            problem = verify(parsed, seen);
            verifyMs = performance.now() - v0;
            cursor = parsed.next_cursor;
          }
        }
        resolve({
          totalMs: performance.now() - t0, recvMs: tRecvEnd - t0,
          parseMs, verifyMs, bytes, cursor, problem, status: res.statusCode,
        });
      });
    });
    req.on('error', () => resolve({ totalMs: performance.now() - t0, recvMs: 0, parseMs: 0, verifyMs: 0, bytes: 0, cursor: null, problem: 'error', status: 0 }));
    req.setTimeout(120000, () => req.destroy(new Error('timeout')));
    req.end();
  });
}

const pct = (a, p) => (a.length ? Number([...a].sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(p / 100 * a.length))].toFixed(3)) : 0);

console.log('mode'.padEnd(11) + ['limit', 'rows/s', 'MB/s', 'page p50', 'page p95', 'recv p50', 'parse p50', 'parse p95', 'verify p50', 'KB/page'].map((h) => h.padStart(11)).join(''));

const summary = {};
for (const limit of LIMITS) {
  // Mode B cannot read next_cursor, so it could not walk the same pages and
  // would re-request page 1 repeatedly — against warm cache and, here, against
  // the newest rows, which are narrow test rows rather than the xlarge body of
  // the table. The URL list is therefore built once and replayed identically by
  // every mode, so all three read exactly the same pages and the same bytes.
  const urls = [];
  {
    let cursor = null;
    for (let p = 0; p < PAGES; p++) {
      const path = `/logs?limit=${limit}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
      urls.push(path);
      const r = await request(path, 'A1', new Set());
      if (r.status !== 200 || !r.cursor) break;
      cursor = r.cursor;
    }
  }

  for (const mode of ['B', 'A1', 'A2']) {
    const seen = new Set();
    const tot = [], recv = [], parse = [], ver = [];
    let bytes = 0, rows = 0, problem = null;
    const wall0 = performance.now();

    for (const path of urls) {
      const r = await request(path, mode, seen);
      if (r.status !== 200) { problem = `status ${r.status}`; break; }
      tot.push(r.totalMs); recv.push(r.recvMs); parse.push(r.parseMs); ver.push(r.verifyMs);
      bytes += r.bytes; rows += limit;
      // Duplicate ids are expected when the same list is replayed per mode.
      if (r.problem && r.problem !== 'duplicate') problem = r.problem;
    }
    const wall = (performance.now() - wall0) / 1000;
    const rps = Math.round(rows / wall), mbs = bytes / wall / 1048576;
    summary[`${mode}|${limit}`] = { rps, mbs };
    console.log(
      mode.padEnd(11) + String(limit).padStart(11) + String(rps).padStart(11) + mbs.toFixed(1).padStart(11) +
      pct(tot, 50).toFixed(2).padStart(11) + pct(tot, 95).toFixed(2).padStart(11) +
      pct(recv, 50).toFixed(2).padStart(11) + pct(parse, 50).toFixed(2).padStart(11) +
      pct(parse, 95).toFixed(2).padStart(11) + pct(ver, 50).toFixed(2).padStart(11) +
      (bytes / Math.max(tot.length, 1) / 1024).toFixed(0).padStart(11) +
      (problem ? `  problem=${problem}` : '')
    );
  }
  const b = summary[`B|${limit}`], a = summary[`A1|${limit}`], a2 = summary[`A2|${limit}`];
  console.log(`  -> at limit=${limit}: discard ${b.rps} rows/s vs buffer-parse ${a.rps} vs string-parse ${a2.rps}` +
              `  (client overhead costs ${(1 - a.rps / b.rps) * 100 | 0}% / ${(1 - a2.rps / b.rps) * 100 | 0}%)\n`);
}

agent.destroy();
