#!/bin/sh
# App-log producer — 10 log events with one poison (non-JSON) line in the
# middle, exactly like a real log feed with a torn write. Error-level events
# grow an `alert` task at run creation (`when:` over the event's parameters);
# the poison line is dead-lettered (durable row + .dlq mirror) and the stream
# keeps flowing.   ./03_log_producer.sh >> data/applog.ndjson
set -eu

emit() { # level service msg
  printf '{"name":"log_guard","parameters":{"level":"%s","service":"%s","msg":"%s"},"tasks":[{"name":"classify","command":["sh","-c","echo [{{ level }}] {{ service }}: {{ msg }}"]},{"name":"alert","depends_on":["classify"],"when":"{{ level }} == error","command":["sh","-c","mkdir -p data && echo ALERT {{ service }}: {{ msg }} | tee -a data/alerts.log"]}]}\n' \
    "$1" "$2" "$3"
}

emit info  checkout "session started"
emit info  search   "query=running shoes"
emit warn  checkout "retrying payment gateway"
emit error checkout "payment gateway timeout"
emit info  search   "query=trail mix"
# ── the poison: a torn line that is not a workflow spec ──────────────────────
echo 'Jul 24 15:03:11 host kernel: EXT4-fs warning (device sda1): torn log line'
emit info  profile  "avatar updated"
emit error inventory "stock sync failed for sku-99"
emit warn  search   "slow query 1400ms"
emit info  checkout "order placed"
