# dagron-source — workflow ingestion sources and the ingest actor

`dagron-source` defines *where* new workflows come from. It provides the
`WorkflowSource` trait with the built-in File and Channel sources, a `SourceFactory`
extension seam for plugging in queue backends (Redis / SQS / Kafka / NATS), and the
ractor `IngestActor` that turns submissions into runs. It talks to the datastore only
through the `dagron-core` facade, so it needs no backend feature of its own.

## What it does

- **`source`** — the `WorkflowSource` trait and its always-available
  implementations: `FileSource` (one DAG, then drains) and `ChannelSource`
  (submissions over an mpsc channel). `build` / `build_with` are the `SOURCE=…`
  factory that selects a source, dispatching to the `SourceFactory` seam for
  feature-gated queue backends (Redis / SQS / Kafka / NATS).
- **`ingest`** — the ractor `IngestActor` (spawned with `IngestArgs`) that pulls
  submissions, validates each against the core DAG model, and creates runs via
  `db::create_run`, applying `MAX_INFLIGHT_RUNS` admission backpressure and
  dead-lettering payloads that repeatedly fail.

## Feature flags

| Feature | Effect |
|---------|--------|
| `sqlite` | Forward `dagron-core`'s SQLite backend (for standalone / downstream builds). |
| `postgres` | Forward `dagron-core`'s Postgres backend. |

The default engine activates the backend on `dagron-core` directly, so it never
needs these; they exist so a standalone build of this lib can resolve core's
required backend (core is depended on with `default-features = false`).

## Quickstart

Library only — the engine wires it into the reconcile daemon:

```rust
use dagron_source::{source, ingest::{IngestActor, IngestArgs}};
use ractor::Actor;

// Build the configured source (SOURCE=file|redis|sqs|kafka), optionally via a seam.
let wf_source = source::build_with("file", "dag.yaml", None).await?;

// Spawn the ingest actor; it drives submissions into db::create_run.
let (ingest_ref, handle) = IngestActor::spawn(
    Some("ingest".to_string()),
    IngestActor,
    IngestArgs { pool, source: wf_source, max_inflight_runs, /* … */ },
).await?;
```

## Config

| Env | Purpose |
|-----|---------|
| `SOURCE` | Selects the ingestion source: `file` (default) / `redis` / `sqs` / `kafka` / `nats`. |
| `MAX_INFLIGHT_RUNS` | Admission cap the ingest actor enforces on concurrently active runs. |
