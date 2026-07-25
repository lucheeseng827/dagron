#!/bin/sh
# Payment producer — N capture events (default 10). The capture task is
# idempotent BY KEY (pay_id): an already-applied payment is a no-op, so
# at-least-once delivery (a crash between run-create and offset-commit
# redelivers one line) still yields an exactly-once LEDGER.
#   ./05_payments_producer.sh 10 >> data/payments.ndjson
set -eu

N="${1:-10}"
i=0
while [ "$i" -lt "$N" ]; do
  i=$((i + 1))
  pay="pay-$(printf '%04d' "$i")"
  amount="$((i * 7)).25"
  printf '{"name":"payment_capture","parameters":{"pay_id":"%s","amount":"%s"},"tasks":[{"name":"capture","command":["sh","-c","mkdir -p data && touch data/payments_ledger.csv && grep -q \\"^{{ pay_id }},\\" data/payments_ledger.csv && echo {{ pay_id }} already captured — no-op || { echo {{ pay_id }},{{ amount }} >> data/payments_ledger.csv; echo captured {{ pay_id }} amount={{ amount }}; }"]},{"name":"receipt","depends_on":["capture"],"command":["sh","-c","echo receipt issued for {{ pay_id }}"]}]}\n' \
    "$pay" "$amount"
done
