#!/usr/bin/env bash
# Drive a load profile at the Pi's dagron and report throughput + headroom.
#
#   ./loadtest/pi/run.sh ramp        # find the knee
#   ./loadtest/pi/run.sh steady      # hold below it
#
# Runs ON the Pi (the engine's :8787 is not published to the host). Expects the
# quickstart stack up — scripts/rpi-smoke.sh leaves it that way.
set -euo pipefail
PROFILE="${1:-ramp}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT="${OUT_DIR:-$HERE/results}"
mkdir -p "$OUT"

command -v python3 >/dev/null || { echo "python3 missing" >&2; exit 1; }
python3 -c 'import yaml' 2>/dev/null || {
  echo "installing python3-yaml…"
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3-yaml
}

# The engine's management API is on the compose network only, so ask docker
# where it is rather than publishing a port the quickstart deliberately does not.
# Resolved by compose *label*, not by container name. Compose derives the project
# name — and so the `<project>-engine-1` prefix — from the directory the compose
# file sits in, and `compose.quickstart.yaml` pins neither `name:` nor
# `container_name:`. So `root-engine-1` is only correct when the stack was brought
# up from a directory called `root`; the documented recipe copies it to /tmp and
# gets `tmp-engine-1`, and this line then exits under `set -e` before any profile
# runs. The label is the same wherever it was started from.
ENGINE_CID=$(docker ps -q --filter 'label=com.docker.compose.service=engine' | head -n1)
[ -n "$ENGINE_CID" ] || { echo "no running compose container for service 'engine' — is the stack up?" >&2; exit 1; }
EIP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$ENGINE_CID")
BASE="http://$EIP:8787"
curl -fsS --max-time 5 "$BASE/runs?limit=1" >/dev/null || { echo "engine not reachable at $BASE" >&2; exit 1; }
echo "engine: $BASE   profile: $PROFILE"

# Engine limits, printed with the results — a throughput number without the
# concurrency it was measured at is not comparable to anything.
docker exec "$ENGINE_CID" env 2>/dev/null | grep -E '^(WORKER_COUNT|MAX_INFLIGHT|EXECUTOR)=' || true
echo "WORKER_COUNT/MAX_INFLIGHT_RUNS unset above = defaults 16 / 64"

CSV="$OUT/resources-$PROFILE.csv"
ENGINE_URL="$BASE" bash "$HERE/sample.sh" "$CSV" 5 &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null || true' EXIT

python3 "$ROOT/loadtest/harness/run_fleet.py" \
  --config "$HERE/profiles.yaml" --profile "$PROFILE" --base-url "$BASE" \
  2>&1 | tee "$OUT/fleet-$PROFILE.log"

kill $SAMPLER 2>/dev/null || true; trap - EXIT
sleep 1

echo
echo "=== resource peaks (from $CSV) ==="
awk -F, 'NR>1 {
    if ($2+0 > peak_load) peak_load = $2+0
    if ($3+0 > peak_mem)  peak_mem  = $3+0
    if ($4 != "" && $4+0 > peak_temp) peak_temp = $4+0
    # Any non-zero throttle word at any sample invalidates the run: a board that
    # capped its own clock was measuring its heatsink, not dagron.
    if ($5 != "" && $5 != "0x0") throttled = $5
    if ($6+0 > peak_inf)  peak_inf  = $6+0
    cpu[$7] = ($8+0 > cpu[$7]) ? $8+0 : cpu[$7]
    mem[$7] = ($9+0 > mem[$7]) ? $9+0 : mem[$7]
    n++
  }
  END {
    printf "samples: %d   peak load1: %.2f (4 cores)   peak host mem: %d MB   peak in-flight runs: %d\n",
           n, peak_load, peak_mem, peak_inf
    if (peak_temp > 0) printf "peak SoC temp: %.1f C\n", peak_temp
    if (throttled != "") printf "WARNING: board reported throttling (%s) - these numbers are not comparable\n", throttled
    printf "\n%-18s %10s %10s\n", "container", "peak cpu%", "peak MB"
    for (c in cpu) printf "%-18s %10.1f %10.1f\n", c, cpu[c], mem[c]
  }' "$CSV"
