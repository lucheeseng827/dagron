//! dagron workflow ingestion — *where* new workflows come from.
//!
//! * [`source`] — the [`WorkflowSource`](source::WorkflowSource) trait, the
//!   always-available `FileSource` / `ChannelSource`, the
//!   [`SourceFactory`](source::SourceFactory) extension seam, and
//!   `source::build` (the `SOURCE=…` selector).
//! * [`stream`] — the built-in [`StreamSource`](stream::StreamSource)
//!   (`SOURCE=stream`): follow an append-only NDJSON file or named pipe, one
//!   workflow per line, at-least-once with a durable offset checkpoint. The
//!   zero-infra streaming on-ramp; managed broker connectors (Kafka / NATS /
//!   SQS / Redis) ship with dagron Enterprise through the same seam.
//! * [`ingest`] — the ractor [`IngestActor`](ingest::IngestActor) that pulls
//!   submissions, validates them against the core DAG model, and creates runs via
//!   the core datastore facade, applying `MAX_INFLIGHT_RUNS` admission backpressure.

pub mod ingest;
pub mod source;
pub mod stream;
