#!/usr/bin/env bash
# Workload-shape sweep: same logical load, different HTTP shape.
#
# 15,000 logs/s can arrive as 150 requests of 100 logs or as 15,000 requests of
# one. Those are the same number of rows and completely different amounts of
# HTTP, validation, and — because every POST waits for the flush carrying its
# rows — completely different amounts of waiting.
#
# The official benchmark's fingerprint is low application CPU, high Postgres
# CPU and low achieved throughput, which is not what this service produces at
# batch=33. This sweeps batch size and client concurrency to find the shape
# that reproduces it.
#
# Per configuration it records the request-level numbers the scenario harness
# reports, plus two things only the server knows: how many COPY operations the
# rows were written in, and how many transactions Postgres committed. Those give
# the amplification metrics — COPY operations and transactions per 1,000 logs —
# which distinguish "the batch never reached the database" from "the database
# was asked to do more work per row".
#
#   bench/shapesweep.sh --batch 1 --vus 20 --label b1v20
#   bench/shapesweep.sh --batch 1 --vus 20 --agg-rate 4 --get-rate 1 --label b1v20q
set -uo pipefail

BATCH=33; VUS=20; RATE=15000; DURATION=60; AGG_RATE=0; GET_RATE=0; LABEL=""
OUT_DIR="${OUT_DIR:-/tmp/shapesweep}"

while [ $# -gt 0 ]; do
  case "$1" in
    --batch) BATCH="$2"; shift 2 ;;
    --vus) VUS="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --agg-rate) AGG_RATE="$2"; shift 2 ;;
    --get-rate) GET_RATE="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[ -n "$LABEL" ] || LABEL="b${BATCH}v${VUS}"

mkdir -p "$OUT_DIR"
OUT_DIR_PY=$(cygpath -m "$OUT_DIR" 2>/dev/null || echo "$OUT_DIR")

# Server-side counters. ingest.flushes is the number of COPY operations; Postgres
# xact_commit is the number of transactions those rows actually cost.
snap() {
  local ing pg
  ing=$(curl -s --max-time 10 http://localhost:8080/internal/stats)
  pg=$(MSYS_NO_PATHCONV=1 docker exec log-service-postgres-1 psql -U loguser -d logs -tAc \
       "SELECT xact_commit || '|' || tup_inserted || '|' || blks_hit || '|' || blks_read
        FROM pg_stat_database WHERE datname='logs'" 2>/dev/null)
  echo "$ing" | python -c "
import json,sys
d=json.load(sys.stdin)
print('%s|%s|%s' % (d['ingest']['flushes'], d['ingest']['rowsWritten'], d['entries']['accepted']), end='')
"
  echo -n "|$pg"
}

echo "[shape] batch=$BATCH vus=$VUS rate=$RATE agg=$AGG_RATE get=$GET_RATE label=$LABEL"

BEFORE=$(snap)
bash bench/stats.sh $((DURATION - 5)) > "$OUT_DIR/$LABEL.cpu" 2>&1 &
STATS_PID=$!

MSYS_NO_PATHCONV=1 docker exec loadgen node //bench/scenario.mjs --host app \
  --rate "$RATE" --duration "$DURATION" --vus "$VUS" --batch "$BATCH" \
  --agg-rate "$AGG_RATE" --get-rate "$GET_RATE" --drain 20 --drain-limit 100 \
  > "$OUT_DIR/$LABEL.json" 2>&1

wait $STATS_PID 2>/dev/null
AFTER=$(snap)

# Pool occupancy is only meaningful while load is running, so it is sampled at
# the end of the run rather than derived from the deltas above.
POOLS=$(curl -s --max-time 10 http://localhost:8080/internal/stats | python -c "
import json,sys
d=json.load(sys.stdin)
print('%s|%s' % (d['pools']['write']['total'], d['db']['active_connections'] if d.get('db') else 0), end='')
")

python - "$OUT_DIR_PY" "$LABEL" "$BATCH" "$VUS" "$RATE" "$DURATION" "$BEFORE" "$AFTER" "$POOLS" <<'PY'
import json, os, re, sys

out_dir, label, batch, vus, rate, duration, before, after, pools = sys.argv[1:10]
batch, vus, rate, duration = int(batch), int(vus), int(rate), int(duration)

names = ['flushes', 'rows_written', 'accepted', 'xact_commit', 'tup_inserted', 'blks_hit', 'blks_read']
b = dict(zip(names, [int(x) for x in before.split('|')]))
a = dict(zip(names, [int(x) for x in after.split('|')]))
d = {k: a[k] - b[k] for k in names}

raw = open(os.path.join(out_dir, label + '.json'), encoding='utf-8', errors='replace').read()
s = json.loads(raw[raw.index('{', raw.index('load phase complete')):])
t, l, e, ops = s['throughput'], s['latency_ms'], s['eventual_consistency'], s['operations']

cpu = open(os.path.join(out_dir, label + '.cpu'), encoding='utf-8', errors='replace').read()
def cpu_of(c, g):
    m = re.search(rf'{c}\s+CPU avg\s+([\d.]+)%\s+max\s+([\d.]+)%.*?MEM avg\s+([\d.]+)MiB', cpu)
    return float(m.group(g)) if m else None

rows = max(d['rows_written'], 1)
wall = t['accepted_logs'] / t['achieved_logs_per_sec'] if t['achieved_logs_per_sec'] else duration

row = {
    'label': label, 'batch': batch, 'vus': vus,
    'offered_logs_per_sec': rate,
    'achieved_logs_per_sec': t['achieved_logs_per_sec'],
    'post_requests': ops['post'],
    'post_per_sec': round(ops['post'] / wall, 1) if wall else None,
    'post_p50': l['post']['p50'], 'post_p95': l['post']['p95'], 'post_p99': l['post']['p99'],
    'aggregate_p95': l['aggregate']['p95'] if ops['aggregate'] else None,
    'get_logs_p95': l['getlogs']['p95'] if ops['getlogs'] else None,
    # Amplification: the whole point of the sweep.
    'copy_ops': d['flushes'],
    'copy_per_1k_logs': round(d['flushes'] / rows * 1000, 2),
    'xact_per_1k_logs': round(d['xact_commit'] / rows * 1000, 2),
    'rows_per_copy': round(rows / max(d['flushes'], 1), 1),
    'pg_cpu': cpu_of('log-service-postgres-1', 1),
    'app_cpu': cpu_of('log-service-app-1', 1),
    'pg_mem_mib': cpu_of('log-service-postgres-1', 3),
    'write_pool_total': int(pools.split('|')[0]),
    'pg_active_backends': int(pools.split('|')[1]),
    'errors': s['errors']['http_errors'],
    'ec_passed': e['passed'], 'ec_missing': e['missing_records'],
    # Closed-loop model: with VUs blocking on each response, achievable request
    # rate is bounded by concurrency divided by latency.
    'model_logs_per_sec': round(vus * batch / (l['post']['p50'] / 1000), 0) if l['post']['p50'] else None,
}
open(os.path.join(out_dir, label + '.row.json'), 'w', encoding='utf-8').write(json.dumps(row, indent=1))
print(json.dumps(row))
PY
