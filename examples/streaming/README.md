# Streaming case studies — five live pipelines on `SOURCE=stream`

Five end-to-end, laptop-runnable streaming pipelines. Each is a real scenario
(the kind teams run on Kafka + a stream processor) reproduced with nothing but
the dagron binary, a shell producer, and an NDJSON stream — because the
built-in `stream` source speaks the same contract as the managed broker
connectors (at-least-once, ack-after-durable-run, dead-lettering). Semantics
reference: [`docs/STREAMING.md`](../../docs/STREAMING.md).

| # | Case study | What it demonstrates |
|---|---|---|
| [`01`](#01--clickstream-micro-batches) | Clickstream enrichment | event → run-per-event, live tailing, backpressure |
| [`02`](#02--change-data-capture-replication) | CDC order replication | ordered upserts, replay from offset zero |
| [`03`](#03--log-anomaly-alerting--dead-letters) | Log anomaly alerting | poison events → DLQ file + durable rows, redrive |
| [`04`](#04--sensor-micro-batch-windows) | Sensor windows | schedule-driven micro-batches, `{{ scheduled_time }}`, catch-up |
| [`05`](#05--exactly-once-payment-effects) | Payment processing | kill-safe at-least-once delivery + idempotent effects |

## One-time setup

```bash
cargo build --release                       # → ./target/release/dagron
cd examples/streaming
export DAGRON=../../target/release/dagron
mkdir -p data                               # streams + offsets live here (gitignored)
```

Every walkthrough uses two terminals: **T1** runs the engine on a stream, **T2**
produces events. `STREAM_FOLLOW=false` variants run single-terminal (produce
first, then drain the backlog — same pipeline as a reproducible batch).

---

## 01 — Clickstream micro-batches

**Scenario.** Product analytics: every click event becomes a durable
enrich→aggregate run. When a burst outruns the engine, `MAX_INFLIGHT_RUNS`
holds admission and the *file* buffers the burst — the queue-shaped behaviour,
no queue required.

```bash
# T1 — engine, following the click stream (management API on :8787):
touch data/clicks.ndjson
SOURCE=stream STREAM_PATH=data/clicks.ndjson API_ADDR=127.0.0.1:8787 \
  MAX_INFLIGHT_RUNS=8 $DAGRON data/clicks.ndjson

# T2 — emit 25 click events:
./01_clickstream_producer.sh 25 >> data/clicks.ndjson

# T2 — watch runs stream in, then tail one:
curl -s 'localhost:8787/runs?limit=5' | head -c 600; echo
```

Kill the engine (Ctrl-C) mid-burst and restart it: consumption resumes at
`data/clicks.ndjson.offset` — nothing lost, nothing double-consumed after ack.

## 02 — Change-data-capture replication

**Scenario.** Keep a reporting replica fresh from a change feed. The producer
emits CDC-shaped envelopes (`op`/`table`/`pk`/`after`) exactly like a
Debezium-style feed flattened to NDJSON; each becomes an upsert run appending
to a replica ledger (`data/orders_replica.csv`) with an audit trail per key.

```bash
# Single terminal (drain mode): produce a change history, then replicate it.
./02_cdc_producer.sh > data/orders_cdc.ndjson
SOURCE=stream STREAM_PATH=data/orders_cdc.ndjson STREAM_FOLLOW=false \
  $DAGRON data/orders_cdc.ndjson

column -s, -t data/orders_replica.csv    # the replica after applying the feed

# Rewind the consumer: the cursor is DB-committed (exactly-once) with the
# .offset file as a mirror — clear both (or use a fresh workflow.db):
sqlite3 workflow.db "DELETE FROM source_offsets WHERE source_name='stream'"
rm data/orders_cdc.ndjson.offset
# … re-run the engine line above: the feed replays idempotently (same ledger).
```

Run creation is **exactly-once** — each line's offset commits in the same
transaction as its run, so a crash-restart re-creates nothing; the deliberate
rewind above is the recover-a-corrupted-replica move. (A native Postgres
change-data-capture connector with LSN offsets on this same substrate ships
elsewhere.)

## 03 — Log anomaly alerting + dead letters

**Scenario.** An error-rate monitor over an app log feed. Well-formed events
run a threshold→alert DAG; a torn/garbage line (every real log feed has them)
must not wedge the stream — it is **dead-lettered** and consumption continues.

```bash
# T1:
touch data/applog.ndjson
SOURCE=stream STREAM_PATH=data/applog.ndjson API_ADDR=127.0.0.1:8787 \
  $DAGRON data/applog.ndjson

# T2 — 10 log events with a poison line in the middle:
./03_log_producer.sh >> data/applog.ndjson

curl -s localhost:8787/dead-letters | head -c 400; echo   # the poison, parked
cat data/applog.ndjson.dlq                                # …and file-mirrored
```

The durable row is inspect/redrive/delete-able (API + UI); the `.dlq` NDJSON
mirror is the broker-native-DLQ analog for shell tooling.

## 04 — Sensor micro-batch windows

**Scenario.** IoT readings accumulate continuously; a *scheduled* workflow
processes each 1-minute window — the batching half of streaming (aggregation
wants windows, not events). Here the stream file is the task's **data**, not
the engine's source: cron fires the windower, and `{{ scheduled_time }}` names
the window, so every run aggregates *its* interval, not "now".

```bash
# T2 — continuous producer, one reading every 2 s (leave running):
./04_sensor_producer.sh data/sensors.ndjson &

# T1 — cron fires the windower each minute (plus one immediate run):
CRON_CONFIG=04_cron.yaml API_ADDR=127.0.0.1:8787 $DAGRON 04_sensor_window.yaml

cat data/sensor_windows.tsv                  # one min/avg/max row per window
```

Because runs are keyed by `{{ scheduled_time }}`, the same workflow replays
any historical window unchanged — that is what the schedule **backfill /
catch-up** machinery drives at scale
([`docs/BACKFILL_USECASES.md`](../../docs/BACKFILL_USECASES.md)).

## 05 — Exactly-once payment effects

**Scenario.** The delivery-semantics test that matters: payment events where a
crash must never double-charge. Run creation is **exactly-once** (the line's
offset commits in the same transaction as its run), and the effect task is
additionally **idempotent by key** — an already-applied payment id is a no-op.
That second layer is deliberate defense in depth: it also protects retries
*within* a run and redelivery on brokers whose acks live outside the
datastore.

```bash
# T1:
touch data/payments.ndjson
SOURCE=stream STREAM_PATH=data/payments.ndjson API_ADDR=127.0.0.1:8787 \
  $DAGRON data/payments.ndjson

# T2 — 10 payments:
./05_payments_producer.sh 10 >> data/payments.ndjson

# Chaos drill: Ctrl-C the engine WHILE runs are in flight, then restart it.
# The un-acked line redelivers; the ledger still holds each payment exactly once:
cut -d, -f1 data/payments_ledger.csv | sort | uniq -d    # → empty (no double-charge)
```

---

**Moving any of these to a managed broker** (events already on Kafka/NATS/SQS/
Redis, or signed webhooks via CloudEvents) is an environment-variable change on
the same workflows — the connector suite ships with
[not in this build](../../README.md#what-this-build-does-not-do).
