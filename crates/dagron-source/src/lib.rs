//! dagron workflow ingestion — *where* new workflows come from.
//!
//! * [`source`] — the [`WorkflowSource`](source::WorkflowSource) trait, the
//!   always-available `FileSource` / `ChannelSource`, the feature-gated queue
//!   backends (Redis / SQS / Kafka / NATS), and `source::build` (the
//!   `SOURCE=…` factory).
//! * [`ingest`] — the ractor [`IngestActor`](ingest::IngestActor) that pulls
//!   submissions, validates them against the core DAG model, and creates runs via
//!   the core datastore facade, applying `MAX_INFLIGHT_RUNS` admission backpressure.

pub mod ingest;
pub mod source;
