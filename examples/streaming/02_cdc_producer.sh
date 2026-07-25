#!/bin/sh
# CDC producer — a fixed change history for an `orders` table, emitted as
# Debezium-style envelopes flattened to one NDJSON workflow per change:
# INSERTs, UPDATEs, and a DELETE, in commit order. The apply task is
# idempotent (exact-row dedup), so replaying the feed (delete the .offset
# file) converges to the same replica ledger.
#   ./02_cdc_producer.sh > data/orders_cdc.ndjson
set -eu

emit() { # op pk status amount lsn
  printf '{"name":"cdc_apply_orders","parameters":{"op":"%s","pk":"%s","status":"%s","amount":"%s","lsn":"%s"},"tasks":[{"name":"apply","command":["sh","-c","mkdir -p data && row={{ lsn }},{{ op }},{{ pk }},{{ status }},{{ amount }} && touch data/orders_replica.csv && grep -qxF \\"$row\\" data/orders_replica.csv || echo \\"$row\\" >> data/orders_replica.csv"]},{"name":"audit","depends_on":["apply"],"command":["sh","-c","echo applied lsn={{ lsn }} {{ op }} {{ pk }}"]}]}\n' \
    "$1" "$2" "$3" "$4" "$5"
}

emit INSERT o-1001 created  120.00 0001
emit INSERT o-1002 created   35.50 0002
emit UPDATE o-1001 paid     120.00 0003
emit INSERT o-1003 created  310.99 0004
emit UPDATE o-1002 paid      35.50 0005
emit UPDATE o-1001 shipped  120.00 0006
emit DELETE o-1003 void       0.00 0007
emit INSERT o-1004 created   89.90 0008
emit UPDATE o-1004 paid      89.90 0009
