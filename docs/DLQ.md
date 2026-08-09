# dagron — broker-native dead-letter routing

> Status: **implemented** (Feature 2) — broker-native DLQ for SQS / Kafka / Redis
> + a new NATS JetStream source.
> Companion to [`ARCHITECTURE.md`](ARCHITECTURE.md) (the `WorkflowSource` seam).
> Last updated: 2026-06-15

## 0. Goal

A poison submission (one that never parses, or that persistently fails to become a
run) should land somewhere an operator and the surrounding infra can act on it.
Before this change the dead-letter store was **Postgres-only**: the bad payload was
written to the `dead_letters` table and the broker message was acked away — so any
broker-native tooling (an SQS DLQ alarm, a Kafka DLT consumer, a Redis DLQ list, a
NATS DLQ subject) never saw it. This feature adds **broker-native DLQ routing** as
a mirror of the Postgres row, and adds **NATS JetStream** as a fourth ingestion
backend.

The Postgres `dead_letters` table stays the **source of truth** (UI list / redrive
/ discard via dagron-api). Broker DLQ routing is an additional, best-effort mirror.

## 1. The `WorkflowSource::dead_letter` hook

DLQ routing is a new optional method on the ingestion trait
([`src/source.rs`](../src/source.rs)):

```rust
async fn dead_letter(&mut self, _payload: &str, _error: &str) -> Result<()> {
    Ok(()) // default: no-op (file/channel; queue backends with no DLQ configured)
}
```

Each broker backend overrides it to publish to its native DLQ **only when a DLQ
destination is configured**; otherwise it stays a no-op and the backend remains
Postgres-only (no behaviour change).

| Backend | DLQ destination | Mechanism | Config |
|---|---|---|---|
| Redis | a separate list | `LPUSH <dlq> <payload>` | `REDIS_DLQ_QUEUE` |
| SQS | a DLQ queue | `SendMessage` (payload body + `dagron-error` attribute) | `SQS_DLQ_URL` |
| Kafka | a dead-letter topic | produce (`dagron-error` header) | `KAFKA_DLQ_TOPIC` |
| NATS | a DLQ subject | JetStream `publish` | `NATS_DLQ_SUBJECT` |

> The SQS send is a **dagron-managed** DLQ publish — independent of, and
> composable with, any SQS native `RedrivePolicy` on the source queue.

## 2. Event flow

The ingest actor ([`src/ingest.rs`](../src/ingest.rs)) decides *when* to
dead-letter (unchanged); the new step is the broker mirror, between recording the
Postgres row and acking the original message off the source.

```
recv() ──► DagGraph::from_yaml ──┬── ok ──► create_run ──┬── ok ──► ack (offset/delete/LREM)
                                 │                        └── err (transient) ──► nack, retry N×
                                 └── parse err (deterministic) ─┐                      │
                                                                ▼                      ▼ (after N)
                                                  ┌──────────── dead_letter(payload, error) ───────────┐
                                                  │ 1. record_dead_letter()  → Postgres dead_letters    │  (source of truth)
                                                  │ 2. source.dead_letter()  → broker DLQ (best-effort) │  (native mirror)
                                                  │ 3. source.ack()          → remove from source       │
                                                  └─────────────────────────────────────────────────────┘
```

Ordering rationale:

1. **Postgres first** — the durable record is authoritative; if it fails the
   message is nacked (retried later), never silently lost.
2. **Broker DLQ mirror** — best-effort; a publish failure is logged and ingestion
   continues (the Postgres row already exists). It must not stall the pipeline.
3. **Ack** — only after the poison is recorded, so it leaves the source for good.

## 3. NATS JetStream source

A fourth ingestion backend ([`src/source/nats_source.rs`](../src/source/nats_source.rs)),
`SOURCE=nats`, `--features nats` (pure-Rust `async-nats`, rustls — no C deps).

- Consumes from a JetStream **durable pull consumer**; the per-message ack is the
  lease (at-least-once, like the other backends).
- `ack` → double-ack; `nack` → `NAK` (immediate redelivery); `dead_letter` →
  publish to `NATS_DLQ_SUBJECT`.
- Stream/consumer are created idempotently (`get_or_create_*`) on startup. The
  pull consumer sets `filter_subject` to the **ingest** subject so it never
  re-consumes the DLQ subject it publishes to (both live in the same stream).
  Without that filter a poison message loops forever: dead-lettered → republished
  to the DLQ subject → re-consumed → re-dead-lettered. (Caught in live testing —
  see `test/dlq-k3s/`.)
- One in-flight message is held internally (the ingest actor is strictly
  one-at-a-time), so NATS needs no change to the `AckHandle` enum.

Config: `NATS_URL` (default `nats://127.0.0.1:4222`), `NATS_STREAM` (default
`WORKFLOWS`), `NATS_SUBJECT` (default `workflows`), `NATS_DURABLE` (default
`module-54-scheduler`), `NATS_DLQ_SUBJECT` (optional).

## 4. Configuration summary

Ingestion backend is chosen by `SOURCE` (`file` | `redis` | `sqs` | `kafka` |
`nats`); each non-file backend needs its Cargo feature. DLQ routing is **opt-in**
per backend via the `*_DLQ_*` var — unset means Postgres-only.

| Env | Backend | Purpose |
|-----|---------|---------|
| `REDIS_URL`, `REDIS_QUEUE`, `REDIS_DLQ_QUEUE` | redis | source list + DLQ list |
| `SQS_QUEUE_URL`, `SQS_DLQ_URL` | sqs | source queue + DLQ queue |
| `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_GROUP`, `KAFKA_DLQ_TOPIC` | kafka | source topic + DLT |
| `NATS_URL`, `NATS_STREAM`, `NATS_SUBJECT`, `NATS_DURABLE`, `NATS_DLQ_SUBJECT` | nats | stream/subject + DLQ subject |
| `DEAD_LETTER_MAX_ATTEMPTS` | all | transient-failure retries before dead-lettering |

## 5. Notes & limitations

- **Mirror, not migration.** The Postgres `dead_letters` table remains the UI's
  source of truth (list / redrive / discard). Broker DLQ is an additional sink for
  broker-side alerting/tooling; dagron does not consume its own DLQ.
- **Best-effort.** A broker DLQ publish failure never blocks ingestion or loses the
  poison — the Postgres row is already committed.
- **Kafka transient nack** still commits-to-skip rather than rewinding (avoids a
  poison-pill stall); the parse-failure path now additionally routes to the DLT.
- **No live broker test in CI.** Backends compile under their feature flags
  (`cargo check --features redis,sqs,kafka,nats`); exercising real DLQ delivery
  needs a running broker.
