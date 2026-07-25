#!/bin/sh
# Clickstream producer — emits N click events (default 25) as NDJSON workflow
# submissions on stdout. Each line is a complete workflow: enrich the click,
# then fold it into a per-page counter ledger. Append to the stream the engine
# follows:   ./01_clickstream_producer.sh 25 >> data/clicks.ndjson
set -eu

N="${1:-25}"
i=0
while [ "$i" -lt "$N" ]; do
  i=$((i + 1))
  user="u-$((i % 7))"
  case $((i % 4)) in
    0) page="/checkout" ;;
    1) page="/home" ;;
    2) page="/product/42" ;;
    3) page="/search" ;;
  esac
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # One line = one workflow submission (JSON is YAML): enrich → aggregate.
  # The aggregate's append→rebuild runs under flock (fd 9) so concurrent runs
  # serialize the whole read/append/rebuild transaction, and the rebuilt
  # counts file lands atomically (tmp + mv).
  printf '{"name":"clickstream_enrich","parameters":{"user":"%s","page":"%s","ts":"%s"},"tasks":[{"name":"enrich","command":["sh","-c","echo enriched user={{ user }} page={{ page }} at {{ ts }}"]},{"name":"aggregate","depends_on":["enrich"],"command":["sh","-c","mkdir -p data && { flock 9; echo \\"{{ page }}\\" >> data/click_counts.log; sort data/click_counts.log | uniq -c | sort -rn > data/click_counts.tsv.tmp; mv data/click_counts.tsv.tmp data/click_counts.tsv; } 9>>data/click_counts.lock && echo folded {{ page }}"]}]}\n' \
    "$user" "$page" "$ts"
done
