#!/usr/bin/env bash
# Aggregate queue-wait vs database-execution split.
#
# Aggregate latency shows a ~20x p50/p95 gap (about 300ms against 6,000ms) under
# the xlarge payload. That shape is consistent with two very different causes:
# slow SQL, or fast SQL waiting behind other requests at the admission gate.
# They call for opposite fixes, so they have to be separated before anything is
# changed.
#
# No production instrumentation is needed for that. The route captures its start
# timestamp before entering the gate and records the total afterwards, while the
# gate records the time spent waiting for a slot. So:
#
#   queue wait        = metrics.latency.aggregate_wait
#   db + postprocess  = metrics.latency.aggregate - aggregate_wait
#
# Both are already exposed by /internal/stats. This script sweeps the admission
# limit and the arrival rate and reports the two components side by side.
#
#   bench/queuesweep.sh --conc 2 --agg-rate 4 --label c2r4
set -uo pipefail

CONC=2; SCAN_CONC=1; AGG_RATE=4; GET_RATE=1; LABEL=""; DURATION=90
RATE=15000; VUS=20; MSG_BYTES=1000; ATTR_COUNT=24; AGG_Q=""; FRESH=1
OUT_DIR="${OUT_DIR:-/tmp/queuesweep}"

while [ $# -gt 0 ]; do
  case "$1" in
    --conc) CONC="$2"; shift 2 ;;
    --scan-conc) SCAN_CONC="$2"; shift 2 ;;
    --agg-rate) AGG_RATE="$2"; shift 2 ;;
    --get-rate) GET_RATE="$2"; shift 2 ;;
    --agg-q) AGG_Q="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --keep) FRESH=0; shift ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[ -n "$LABEL" ] || LABEL="c${CONC}r${AGG_RATE}"

mkdir -p "$OUT_DIR"
OUT_PY=$(cygpath -m "$OUT_DIR" 2>/dev/null || echo "$OUT_DIR")

[ "$FRESH" = "1" ] && docker compose down -v > /dev/null 2>&1
AGGREGATE_CONCURRENCY="$CONC" AGGREGATE_SCAN_CONCURRENCY="$SCAN_CONC" docker compose up -d > /dev/null 2>&1
until curl -sf http://localhost:8080/health > /dev/null 2>&1; do sleep 1; done
docker cp bench/. loadgen://bench > /dev/null 2>&1

echo "[queue] conc=$CONC scan=$SCAN_CONC agg-rate=$AGG_RATE q='${AGG_Q}' dur=${DURATION}s"
QOPT=""; [ -n "$AGG_Q" ] && QOPT="--agg-q $AGG_Q"

bash bench/stats.sh $((DURATION - 5)) > "$OUT_DIR/$LABEL.cpu" 2>&1 &
STATS_PID=$!

MSYS_NO_PATHCONV=1 docker exec loadgen node //bench/scenario.mjs --host app --rate "$RATE" \
  --duration "$DURATION" --vus "$VUS" --batch 33 --msg-bytes "$MSG_BYTES" --attr-count "$ATTR_COUNT" \
  --agg-rate "$AGG_RATE" --get-rate "$GET_RATE" $QOPT --drain 30 --drain-limit 100 \
  > "$OUT_DIR/$LABEL.json" 2>&1

wait $STATS_PID 2>/dev/null
curl -s --max-time 15 http://localhost:8080/internal/stats > "$OUT_DIR/$LABEL.stats" 2>/dev/null

python - "$OUT_PY" "$LABEL" "$CONC" "$SCAN_CONC" "$AGG_RATE" "$AGG_Q" <<'PY'
import json, os, re, sys
out, label, conc, scan_conc, agg_rate, agg_q = sys.argv[1:7]

raw = open(os.path.join(out, label + '.json'), encoding='utf-8', errors='replace').read()
d = json.loads(raw[raw.index('{', raw.index('load phase complete')):])
t, l, e, ops = d['throughput'], d['latency_ms'], d['eventual_consistency'], d['operations']

st = json.load(open(os.path.join(out, label + '.stats'), encoding='utf-8'))
srv = st['latency_ms']
wait, total = srv['aggregate_wait'], srv['aggregate']

cpu = open(os.path.join(out, label + '.cpu'), encoding='utf-8', errors='replace').read()
def c(name, g):
    m = re.search(rf'{name}\s+CPU avg\s+([\d.]+)%\s+max\s+([\d.]+)%.*?MEM avg\s+([\d.]+)MiB', cpu)
    return float(m.group(g)) if m else None

# Server-side total minus server-side wait leaves database plus postprocessing.
# Percentiles are not additive, so this is reported as a decomposition of the
# same reservoir rather than an exact per-request subtraction.
def db(p): return round(max(total[p] - wait[p], 0.0), 1)

row = {
 'label': label, 'bounded_limit': int(conc), 'scan_limit': int(scan_conc),
 'agg_rate': float(agg_rate), 'agg_class': 'scan(q)' if agg_q else 'bounded(simple)',
 'aggregates': ops['aggregate'],
 'queue_p50': wait['p50'], 'queue_p95': wait['p95'], 'queue_p99': wait['p99'],
 'db_p50': db('p50'), 'db_p95': db('p95'), 'db_p99': db('p99'),
 'total_p50': total['p50'], 'total_p95': total['p95'], 'total_p99': total['p99'],
 'client_agg_p95': l['aggregate']['p95'],
 'queue_share_p95_pct': round(wait['p95'] / total['p95'] * 100, 1) if total['p95'] else None,
 'gate': st['aggregate_gate'], 'admission': st['aggregate_admission'],
 'logs_per_sec': t['achieved_logs_per_sec'], 'post_p95': l['post']['p95'],
 'get_logs_p95': l['getlogs']['p95'], 'get_logs_server_p95': srv['get_logs']['p95'],
 'pg_cpu': c('log-service-postgres-1', 1), 'pg_cpu_max': c('log-service-postgres-1', 2),
 'app_cpu': c('log-service-app-1', 1),
 'drain_rows_per_sec': e['read_rows_per_sec'], 'ec_missing': e['missing_records'],
 'ec_passed': e['passed'], 'drain_s': e['drain_seconds'],
 'errors': d['errors']['http_errors'],
}
open(os.path.join(out, label + '.row.json'), 'w', encoding='utf-8').write(json.dumps(row, indent=1))
print(json.dumps(row))
PY
