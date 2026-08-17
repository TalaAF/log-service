#!/usr/bin/env bash
# Hot-tail configuration sweep.
#
# The aggregate answers everything below a boundary from the rollup and
# everything above it from raw rows. With wide payloads the raw part dominates,
# so the boundary's distance from now() is the lever. That distance is a min() of
# three terms:
#
#   safe_before   align(taken_at - ROLLUP_LAG_SECONDS), published by the refresh
#   ingest floor  align(oldest timestamp accepted within INGEST_FLOOR_WINDOW_SECONDS)
#   refresh age   how long ago the last refresh ran, bounded by the cron interval
#
# Only the smallest matters, so sweeping one knob is misleading unless it is the
# binding one. This runs a fixed workload against a given (lag, floor, interval)
# triple and reports the tail actually achieved next to what it cost — including
# refresh duty cycle, because shrinking the tail by refreshing more often moves
# the cost rather than removing it.
#
#   bench/tailsweep.sh --lag 10 --floor 12 --interval 5 --label base
set -uo pipefail

LAG=10; FLOOR=12; INTERVAL=5; LABEL=""; DURATION=60; RATE=15000; VUS=20
MSG_BYTES=1000; ATTR_COUNT=24; AGG_RATE=4; GET_RATE=1; FRESH=0
OUT_DIR="${OUT_DIR:-/tmp/tailsweep}"

while [ $# -gt 0 ]; do
  case "$1" in
    --lag) LAG="$2"; shift 2 ;;
    --floor) FLOOR="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --msg-bytes) MSG_BYTES="$2"; shift 2 ;;
    --attr-count) ATTR_COUNT="$2"; shift 2 ;;
    --fresh) FRESH=1; shift ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[ -n "$LABEL" ] || LABEL="lag${LAG}_fl${FLOOR}_int${INTERVAL}"

mkdir -p "$OUT_DIR"
OUT_PY=$(cygpath -m "$OUT_DIR" 2>/dev/null || echo "$OUT_DIR")
PSQL() { MSYS_NO_PATHCONV=1 docker exec log-service-postgres-1 psql -U loguser -d logs "$@"; }

[ "$FRESH" = "1" ] && docker compose down -v > /dev/null 2>&1

# The floor is an application env var; lag and the cron interval live in the
# database, so the experiment sets them without editing production defaults.
INGEST_FLOOR_WINDOW_SECONDS="$FLOOR" docker compose up -d > /dev/null 2>&1
until curl -sf http://localhost:8080/health > /dev/null 2>&1; do sleep 1; done
PSQL -tAc "UPDATE rollup_config SET lag_seconds = $LAG WHERE id" > /dev/null 2>&1
PSQL -tAc "SELECT cron.unschedule('log-rollup-refresh')" > /dev/null 2>&1
PSQL -tAc "SELECT cron.schedule('log-rollup-refresh','$INTERVAL seconds','SELECT refresh_log_rollups()')" > /dev/null 2>&1
docker cp bench/. loadgen://bench > /dev/null 2>&1

echo "[tail] lag=${LAG}s floor=${FLOOR}s interval=${INTERVAL}s payload=${MSG_BYTES}B/${ATTR_COUNT}"
rm -f "$OUT_DIR/$LABEL.tail"
BEFORE=$(PSQL -tAc "SELECT refreshes||'|'||skipped FROM rollup_state WHERE id" 2>/dev/null)

bash bench/stats.sh $((DURATION - 5)) > "$OUT_DIR/$LABEL.cpu" 2>&1 &
STATS_PID=$!
# The boundary moves continuously, so it is sampled rather than read once.
( for _ in $(seq 1 $(( (DURATION - 10) / 3 )) ); do
    curl -s --max-time 5 http://localhost:8080/internal/stats | python -c "
import json,sys
try:
  r=json.load(sys.stdin)['rollup']
  print(r['hot_tail_seconds'], r['last_duration_ms'], r['last_rows'])
except Exception: pass" >> "$OUT_DIR/$LABEL.tail" 2>/dev/null
    sleep 3
  done ) &
TAIL_PID=$!

MSYS_NO_PATHCONV=1 docker exec loadgen node //bench/scenario.mjs --host app --rate "$RATE" \
  --duration "$DURATION" --vus "$VUS" --batch 33 --msg-bytes "$MSG_BYTES" --attr-count "$ATTR_COUNT" \
  --agg-rate "$AGG_RATE" --get-rate "$GET_RATE" --drain 25 --drain-limit 100 \
  > "$OUT_DIR/$LABEL.json" 2>&1

wait $STATS_PID 2>/dev/null; kill $TAIL_PID 2>/dev/null; wait $TAIL_PID 2>/dev/null
AFTER=$(PSQL -tAc "SELECT refreshes||'|'||skipped FROM rollup_state WHERE id" 2>/dev/null)

# Physical tail footprint, measured from the table rather than inferred.
TAILSIZE=$(PSQL -tAc "
  WITH b AS (SELECT safe_before FROM rollup_state WHERE id)
  SELECT (SELECT count(*) FROM logs, b WHERE \"timestamp\" >= b.safe_before)
      || '|' || round((SELECT count(*) FROM logs, b WHERE \"timestamp\" >= b.safe_before)
         * (pg_relation_size('logs_2026_w33')::numeric / GREATEST((SELECT count(*) FROM logs),1))
         / 1048576, 1)" 2>/dev/null)
EXACT=$(PSQL -tAc "SELECT (SELECT count(*) FROM logs)||'|'||(SELECT COALESCE(sum(entry_count),0) FROM log_rollups)" 2>/dev/null)

python - "$OUT_PY" "$LABEL" "$LAG" "$FLOOR" "$INTERVAL" "$BEFORE" "$AFTER" "$TAILSIZE" "$EXACT" <<'PY'
import json, os, re, sys, statistics as st
out, label, lag, floor, interval, before, after, tailsize, exact = sys.argv[1:10]

raw = open(os.path.join(out, label + '.json'), encoding='utf-8', errors='replace').read()
d = json.loads(raw[raw.index('{', raw.index('load phase complete')):])
t, l, e = d['throughput'], d['latency_ms'], d['eventual_consistency']

cpu = open(os.path.join(out, label + '.cpu'), encoding='utf-8', errors='replace').read()
def c(name, g):
    m = re.search(rf'{name}\s+CPU avg\s+([\d.]+)%\s+max\s+([\d.]+)%.*?MEM avg\s+([\d.]+)MiB', cpu)
    return float(m.group(g)) if m else None

tails, durs, rws = [], [], []
tp = os.path.join(out, label + '.tail')
if os.path.exists(tp):
    for line in open(tp):
        p = line.split()
        if len(p) == 3 and p[0] not in ('None', 'null'):
            tails.append(float(p[0])); durs.append(float(p[1])); rws.append(float(p[2]))

rb, sb = map(int, before.split('|')); ra, sa = map(int, after.split('|'))
trows, tmb = tailsize.split('|'); rawt, rolt = map(int, exact.split('|'))

row = {
 'label': label, 'lag_s': int(lag), 'floor_s': int(floor), 'interval_s': int(interval),
 'tail_min_s': min(tails) if tails else None,
 'tail_med_s': round(st.median(tails), 1) if tails else None,
 'tail_max_s': max(tails) if tails else None,
 'tail_rows': int(float(trows)), 'tail_mb': float(tmb),
 'logs_per_sec': t['achieved_logs_per_sec'],
 'post_p95': l['post']['p95'],
 'agg_p50': l['aggregate']['p50'], 'agg_p95': l['aggregate']['p95'], 'agg_p99': l['aggregate']['p99'],
 'get_p95': l['getlogs']['p95'],
 'refreshes': ra - rb, 'skipped': sa - sb,
 'refresh_ms_med': round(st.median(durs), 1) if durs else None,
 'refresh_ms_max': max(durs) if durs else None,
 'refresh_rows_med': int(st.median(rws)) if rws else None,
 # Stability: median duration as a share of the interval. Approaching 100% means
 # the refresh cannot keep up and the configuration is invalid.
 'refresh_duty_pct': round(st.median(durs) / (int(interval) * 1000) * 100, 1) if durs else None,
 'pg_cpu': c('log-service-postgres-1', 1), 'pg_cpu_max': c('log-service-postgres-1', 2),
 'pg_mem_mib': c('log-service-postgres-1', 3), 'app_cpu': c('log-service-app-1', 1),
 'errors': d['errors']['http_errors'], 'ec_passed': e['passed'], 'ec_missing': e['missing_records'],
 'drain_rows_per_sec': e['read_rows_per_sec'],
 'raw_total': rawt, 'rolled_total': rolt, 'exact_delta': rawt - rolt,
}
open(os.path.join(out, label + '.row.json'), 'w', encoding='utf-8').write(json.dumps(row, indent=1))
print(json.dumps(row))
PY
