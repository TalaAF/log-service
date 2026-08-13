#!/usr/bin/env bash
# Samples container CPU/memory while a run is in progress and prints avg/max.
#
# Uses streaming `docker stats` (roughly one sample per second) rather than
# repeated --no-stream calls, which each cost about two seconds and badly
# undersample a short run. The stream redraws the terminal, so ANSI control
# sequences are stripped before parsing.
#
# Usage: bench/stats.sh <seconds>
DUR=${1:-60}
TMP=$(mktemp)

timeout "$DUR" docker stats --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' --no-trunc 2>/dev/null \
  | tr -d '\r' \
  | perl -pe 's/\e\[[0-9;?]*[a-zA-Z]//g' \
  | sed 's/%//' \
  | grep -oE 'log-service-[a-z]+-[0-9]+ +[0-9.]+ +[0-9.]+' > "$TMP"

awk '
  { name=$1; cpu=$2+0; mem=$3+0;
    n[name]++; sum[name]+=cpu; if (cpu>max[name]) max[name]=cpu;
    msum[name]+=mem; if (mem>mmax[name]) mmax[name]=mem }
  END { for (k in n) printf "%-26s CPU avg %6.2f%%  max %6.2f%%  |  MEM avg %6.1fMiB  max %6.1fMiB  (n=%d)\n",
        k, sum[k]/n[k], max[k], msum[k]/n[k], mmax[k], n[k] }
' "$TMP" | sort
rm -f "$TMP"
