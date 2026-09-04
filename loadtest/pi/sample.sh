#!/usr/bin/env bash
# Sample host + container resources during a load run. One CSV row per container
# per tick, long format, so `awk` can group by service without a wide header that
# changes whenever a service is added.
#
#   ENGINE_URL=http://172.20.0.3:8787 ./sample.sh out.csv [interval_secs]
#
# Runs until killed. `docker stats --no-stream` costs about a second on a Pi, so
# the interval is a floor, not a period — the timestamp column is authoritative.
#
# Memory does NOT come from `docker stats`: the Raspberry Pi kernel ships with the
# memory cgroup controller disabled (`memory … 0` in /proc/cgroups — it needs
# `cgroup_enable=memory` on the boot cmdline, which a hosted Pi does not let you
# set), so every MemUsage field reads `0B / 0B`. Summing VmRSS over the container
# cgroup's pids gives the real number with no kernel change and no reboot, and it
# counts the task subprocesses EXECUTOR=local forks inside the engine — which is
# exactly the memory that grows under load.
set -u
OUT="${1:?usage: sample.sh OUT.csv [interval]}"
INTERVAL="${2:-5}"
ENGINE_URL="${ENGINE_URL:-}"
CG=/sys/fs/cgroup/system.slice

# Resolve container ids once; docker inspect per tick is a second we would be
# charging to the measurement rather than the load.
declare -A CID
for n in $(docker ps --format '{{.Names}}'); do
  CID[$n]=$(docker inspect -f '{{.Id}}' "$n" 2>/dev/null || echo "")
done

# A Pi 4 throttles at 80 C, and a throttled board silently halves every number
# a load test produces — so a run with no temperature column carries an unstated
# assumption that it never throttled. Both reads degrade to empty off a Pi (no
# thermal zone, no vcgencmd), which keeps this script portable.
#
# `vcgencmd get_throttled` is a bitmask: bit 0 = throttled NOW, bit 2 = ARM
# frequency capped now, bits 16/18 = it happened at some point since boot. A
# non-zero value at any point invalidates the run, so the raw hex is recorded
# rather than a boolean — which bit tripped is the difference between "hot right
# now" and "hot ten minutes ago".
temp_c() {
  local z=/sys/class/thermal/thermal_zone0/temp
  [ -r "$z" ] && awk '{ printf "%.1f", $1/1000 }' "$z" || echo ""
}
throttled() { vcgencmd get_throttled 2>/dev/null | cut -d= -f2 || echo ""; }

rss_mb() {  # $1 = container name → summed RSS of its cgroup, MB
  local p="$CG/docker-${CID[$1]:-}.scope/cgroup.procs"
  [ -r "$p" ] || { echo 0; return; }
  awk '{ f="/proc/" $0 "/status"
         while ((getline l < f) > 0) if (l ~ /^VmRSS:/) { split(l, a, " "); s += a[2] }
         close(f) }
       END { printf "%.1f", s/1024 }' "$p"
}

echo "ts,load1,host_mem_used_mb,temp_c,throttled,inflight_runs,container,cpu_pct,mem_mb" > "$OUT"
while :; do
  ts=$(date +%s)
  load1=$(cut -d' ' -f1 /proc/loadavg)
  mem=$(free -m | awk '/^Mem:/{print $3}')
  temp=$(temp_c)
  thr=$(throttled)
  # In-flight runs is the number that matters against MAX_INFLIGHT_RUNS — it is
  # what starts producing 429s, and it separates "the Pi is full" from "the
  # admission cap is doing its job".
  inflight=$(curl -fsS --max-time 3 "$ENGINE_URL/runs?status=running&limit=200" 2>/dev/null \
             | jq '.runs | length' 2>/dev/null || echo "")
  while IFS=$'\t' read -r name cpu; do
    [ -n "$name" ] || continue
    # Nine values for the nine columns in the header. `temp` and `thr` were being
    # computed and dropped, which shifted every field after them left by two: the
    # reader's `$5` throttle check landed on the container name (never empty, never
    # "0x0"), so the "not comparable" warning fired on every run, `peak SoC temp`
    # reported the in-flight count, and the per-container table was keyed on RSS.
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$ts" "$load1" "$mem" "$temp" "$thr" "$inflight" "$name" "${cpu%\%}" "$(rss_mb "$name")" >> "$OUT"
  done < <(docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}' 2>/dev/null)
  sleep "$INTERVAL"
done
