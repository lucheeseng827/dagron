#!/bin/sh
# Sensor producer — appends one temperature reading every 2 s to the stream
# file (plain data lines, consumed by the scheduled window workflow — not by
# the engine's source).   ./04_sensor_producer.sh data/sensors.ndjson &
set -eu

OUT="${1:-data/sensors.ndjson}"
mkdir -p "$(dirname "$OUT")"
while :; do
  # 18.0–27.9 °C, deterministic-ish wobble off the epoch second.
  now=$(date +%s)
  temp="$((18 + now % 10)).$((now % 9))"
  echo "{\"ts\":$now,\"celsius\":$temp}" >> "$OUT"
  sleep 2
done
