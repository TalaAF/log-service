#!/usr/bin/env bash
# Correction 1 experiment: hold VUs fixed, vary the group-commit flush interval,
# and see whether closed-loop throughput really does scale inversely with POST
# latency. Each run starts from an equivalent database state.
#
# Usage: bash bench/sweep-flush.sh "40 20 10 5 2" [vus] [duration]
set -u
export MSYS_NO_PATHCONV=1

INTERVALS=${1:-"40 20 10 5 2"}
VUS=${2:-20}
DUR=${3:-45}

printf '%-9s %-11s %-11s %-11s %-13s %s\n' \
  "flush_ms" "post_p50" "post_p95" "logs/s" "predicted" "ratio(actual/pred)"

BASE_LAT=""
BASE_TPUT=""

for MS in $INTERVALS; do
  FLUSH_INTERVAL_MS=$MS docker compose up -d app >/dev/null 2>&1
  until curl -sf http://localhost:8080/health >/dev/null 2>&1; do sleep 1; done
  docker compose exec -T postgres psql -U loguser -d logs -qc "TRUNCATE logs;" >/dev/null 2>&1
  sleep 2

  OUT=$(docker exec loadgen node /bench/scenario.mjs --host app --rate 2000000 \
        --duration "$DUR" --batch 33 --vus "$VUS" --agg-rate 0 --get-rate 0 --drain 1 2>&1)

  echo "$OUT" > "/tmp/sweep_${MS}.json"

  P50=$(echo "$OUT" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const j=JSON.parse(s.slice(s.indexOf("{")));console.log(j.latency_ms.post.p50)})')
  P95=$(echo "$OUT" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const j=JSON.parse(s.slice(s.indexOf("{")));console.log(j.latency_ms.post.p95)})')
  TP=$(echo "$OUT"  | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const j=JSON.parse(s.slice(s.indexOf("{")));console.log(j.throughput.achieved_logs_per_sec)})')

  if [ -z "$BASE_LAT" ]; then
    BASE_LAT=$P50; BASE_TPUT=$TP
    printf '%-9s %-11s %-11s %-11s %-13s %s\n' "$MS" "${P50}ms" "${P95}ms" "$TP" "(baseline)" "1.000"
  else
    # If throughput is purely latency-bound, it should scale as base_lat/this_lat.
    PRED=$(node -e "console.log((${BASE_TPUT}*(${BASE_LAT}/${P50})).toFixed(0))")
    RATIO=$(node -e "console.log((${TP}/${PRED}).toFixed(3))")
    printf '%-9s %-11s %-11s %-11s %-13s %s\n' "$MS" "${P50}ms" "${P95}ms" "$TP" "$PRED" "$RATIO"
  fi
done

# Leave the service on its shipped default.
FLUSH_INTERVAL_MS=40 docker compose up -d app >/dev/null 2>&1
until curl -sf http://localhost:8080/health >/dev/null 2>&1; do sleep 1; done
echo "(restored FLUSH_INTERVAL_MS=40)"
