#!/usr/bin/env bash
# Cold-volume vs warm-volume benchmark harness.
#
# A benchmark run against a freshly created Docker volume measured 9,790 logs/s
# where the identical code on an already-used volume measured 14,994. If that is
# real, every warm local measurement overstates what a grader starting from a
# clean volume would see, and the gap is a storage-initialisation cost rather
# than anything in the query path.
#
# One cold run against one warm run cannot establish that, so this script makes
# the pair repeatable and records what would distinguish the candidate causes:
# WAL bytes and segment churn, buffer reads against hits, checkpoint activity,
# and the per-file growth of the data directory.
#
#   bench/coldwarm.sh --mode cold --label baseline-cold
#   bench/coldwarm.sh --mode warm --label baseline-warm
#   WAL_INIT_ZERO=off bench/coldwarm.sh --mode cold --label initzero-off
#
# --mode cold   destroy the volume first, so Postgres re-runs initdb and every
#               file the workload touches is allocated for the first time
# --mode warm   reuse the existing volume and simply restart the stack, so the
#               same files are rewritten in place
set -uo pipefail

MODE=warm
LABEL=run
DURATION=120
RATE=15000
VUS=20
AGG_RATE=4
STAGES=""
OUT_DIR="${OUT_DIR:-/tmp/coldwarm}"
mkdir -p "$OUT_DIR"
# Mixed form (C:/...) so both the shell and Windows Python can open these.
OUT_DIR_PY=$(cygpath -m "$OUT_DIR" 2>/dev/null || echo "$OUT_DIR")

while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --vus) VUS="$2"; shift 2 ;;
    --agg-rate) AGG_RATE="$2"; shift 2 ;;
    --stages) STAGES="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

mkdir -p "$OUT_DIR"
PSQL=(docker exec log-service-postgres-1 psql -U loguser -d logs -tAc)

# Counter snapshot. Everything here is cumulative since the stats were last
# reset, so the run's own consumption is the difference between two calls.
pg_counters() {
  MSYS_NO_PATHCONV=1 "${PSQL[@]}" "
    SELECT (SELECT wal_records FROM pg_stat_wal)
        || '|' || (SELECT wal_bytes FROM pg_stat_wal)
        || '|' || (SELECT wal_fpi FROM pg_stat_wal)
        || '|' || (SELECT wal_sync FROM pg_stat_wal)
        || '|' || (SELECT blks_read FROM pg_stat_database WHERE datname='logs')
        || '|' || (SELECT blks_hit FROM pg_stat_database WHERE datname='logs')
        || '|' || (SELECT checkpoints_timed FROM pg_stat_bgwriter)
        || '|' || (SELECT checkpoints_req FROM pg_stat_bgwriter)
        || '|' || (SELECT buffers_checkpoint FROM pg_stat_bgwriter)" 2>/dev/null
}

# Size of the pieces of the data directory that ingestion actually extends.
pg_files() {
  MSYS_NO_PATHCONV=1 docker exec log-service-postgres-1 sh -c '
    echo -n "pg_wal_bytes="; du -sb /var/lib/postgresql/data/pg_wal 2>/dev/null | cut -f1
    echo -n "pg_wal_segments="; ls -1 /var/lib/postgresql/data/pg_wal 2>/dev/null | grep -c "^[0-9A-F]\{24\}$"
    echo -n "base_bytes="; du -sb /var/lib/postgresql/data/base 2>/dev/null | cut -f1' 2>/dev/null | tr '\n' ' '
}

echo "[coldwarm] mode=$MODE label=$LABEL wal_init_zero=${WAL_INIT_ZERO:-on} wal_recycle=${WAL_RECYCLE:-on}"

if [ "$MODE" = "cold" ]; then
  docker compose down -v > /dev/null 2>&1
else
  docker compose down > /dev/null 2>&1
fi

BOOT_START=$(date +%s.%N)
docker compose up -d > /dev/null 2>&1

# T0..T3: how long the stack takes to become ready. On a cold volume this covers
# initdb, the migrations and partition provisioning; on a warm one it does not.
until curl -sf http://localhost:8080/health > /dev/null 2>&1; do sleep 1; done
READY_S=$(awk -v a="$(date +%s.%N)" -v b="$BOOT_START" 'BEGIN{printf "%.1f", a-b}')

docker cp bench/. loadgen://bench > /dev/null 2>&1

FILES_BEFORE=$(pg_files)
COUNTERS_BEFORE=$(pg_counters)

# Sample the container while the load runs, then reconcile afterwards.
bash bench/stats.sh $((DURATION - 5)) > "$OUT_DIR/$LABEL.cpu" 2>&1 &
STATS_PID=$!

if [ -n "$STAGES" ]; then
  MSYS_NO_PATHCONV=1 docker exec loadgen node //bench/scenario.mjs --host app --vus "$VUS" \
    --agg-rate "$AGG_RATE" --stages "$STAGES" --drain 30 --drain-limit 100 \
    > "$OUT_DIR/$LABEL.json" 2>&1
else
  MSYS_NO_PATHCONV=1 docker exec loadgen node //bench/scenario.mjs --host app --rate "$RATE" \
    --duration "$DURATION" --vus "$VUS" --agg-rate "$AGG_RATE" --drain 30 --drain-limit 100 \
    > "$OUT_DIR/$LABEL.json" 2>&1
fi

wait $STATS_PID 2>/dev/null

COUNTERS_AFTER=$(pg_counters)
FILES_AFTER=$(pg_files)

python - "$OUT_DIR_PY" "$LABEL" "$MODE" "$READY_S" "$COUNTERS_BEFORE" "$COUNTERS_AFTER" \
         "$FILES_BEFORE" "$FILES_AFTER" "${WAL_INIT_ZERO:-on}" "${WAL_RECYCLE:-on}" <<'PY'
import json, sys, os, re

out_dir, label, mode, ready_s, c_before, c_after, f_before, f_after, init_zero, recycle = sys.argv[1:11]

raw = open(os.path.join(out_dir, label + '.json'), encoding='utf-8', errors='replace').read()
i = raw.index('{', raw.index('load phase complete'))
d = json.loads(raw[i:])
samples = d.get('samples', [])

def window(seconds):
    """Mean achieved rate over the first `seconds` of load."""
    picked = [s['logs_per_sec'] for s in samples if s['t'] <= seconds]
    return round(sum(picked) / len(picked)) if picked else None

names = ['wal_records', 'wal_bytes', 'wal_fpi', 'wal_sync',
         'blks_read', 'blks_hit', 'ckpt_timed', 'ckpt_req', 'buffers_ckpt']
before = dict(zip(names, [int(x) for x in c_before.split('|')]))
after = dict(zip(names, [int(x) for x in c_after.split('|')]))
delta = {k: after[k] - before[k] for k in names}

def files(blob):
    return {k: int(v) for k, v in re.findall(r'(\w+)=(\d+)', blob)}
fb, fa = files(f_before), files(f_after)

t, l, e = d['throughput'], d['latency_ms'], d['eventual_consistency']
cpu = open(os.path.join(out_dir, label + '.cpu'), encoding='utf-8', errors='replace').read()
def cpu_of(container, field):
    m = re.search(rf'{container}\s+CPU avg\s+([\d.]+)%\s+max\s+([\d.]+)%.*?MEM avg\s+([\d.]+)MiB\s+max\s+([\d.]+)MiB', cpu)
    return float(m.group(field)) if m else None

summary = {
    'label': label, 'mode': mode,
    'wal_init_zero': init_zero, 'wal_recycle': recycle,
    'ready_seconds': round(float(ready_s), 1),
    'throughput': {
        'first_10s': window(10), 'first_30s': window(30),
        'overall': t['achieved_logs_per_sec'],
        'second_half': t['second_half_avg_logs_per_sec'],
        'accepted': t['accepted_logs'],
    },
    'per_stage': d.get('per_stage'),
    'latency_ms': {
        'post_p95': l['post']['p95'], 'aggregate_p95': l['aggregate']['p95'],
        'get_logs_p95': l['getlogs']['p95'],
    },
    'postgres': {'cpu_avg': cpu_of('log-service-postgres-1', 1), 'cpu_max': cpu_of('log-service-postgres-1', 2),
                 'mem_avg_mib': cpu_of('log-service-postgres-1', 3)},
    'app': {'cpu_avg': cpu_of('log-service-app-1', 1), 'mem_avg_mib': cpu_of('log-service-app-1', 3)},
    'wal': {'bytes': delta['wal_bytes'], 'records': delta['wal_records'],
            'full_page_images': delta['wal_fpi'], 'syncs': delta['wal_sync']},
    'buffers': {'read': delta['blks_read'], 'hit': delta['blks_hit']},
    'checkpoints': {'timed': delta['ckpt_timed'], 'requested': delta['ckpt_req'],
                    'buffers_written': delta['buffers_ckpt']},
    'files': {
        'pg_wal_mib_before': round(fb.get('pg_wal_bytes', 0) / 1048576, 1),
        'pg_wal_mib_after': round(fa.get('pg_wal_bytes', 0) / 1048576, 1),
        'pg_wal_segments_before': fb.get('pg_wal_segments'),
        'pg_wal_segments_after': fa.get('pg_wal_segments'),
        'base_mib_before': round(fb.get('base_bytes', 0) / 1048576, 1),
        'base_mib_after': round(fa.get('base_bytes', 0) / 1048576, 1),
    },
    'eventual_consistency': {
        'drain_s': e['drain_seconds'], 'rows_per_sec': e['read_rows_per_sec'],
        'missing': e['missing_records'], 'passed': e['passed'],
    },
    'errors': d['errors']['http_errors'],
    'shape_valid': d['shape']['valid'],
}
open(os.path.join(out_dir, label + '.summary.json'), 'w', encoding='utf-8').write(json.dumps(summary, indent=1))
print(json.dumps(summary, indent=1))
PY
