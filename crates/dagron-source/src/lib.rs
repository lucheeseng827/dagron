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
//!   SQS / Redis) are not in this build; they would plug into the same seam.
//! * [`mqtt`] (feature `mqtt`) — the open [`MqttSource`](mqtt::MqttSource)
//!   (`SOURCE=mqtt`): subscribe to a topic on a plant, gateway or robot broker and
//!   turn each message into a run; at-least-once by default, or exactly-once
//!   across restarts when the payload carries a monotonic id
//!   (`MQTT_POSITION_FIELD`) *and* the source was given a datastore
//!   (`with_datastore`) to commit the cursor in — without one there is nothing
//!   durable to compare against and the at-least-once path stands.
//! * [`ingest`] — the ractor [`IngestActor`](ingest::IngestActor) that pulls
//!   submissions, validates them against the core DAG model, and creates runs via
//!   the core datastore facade, applying `MAX_INFLIGHT_RUNS` admission backpressure.

pub mod ingest;
#[cfg(feature = "mqtt")]
pub mod mqtt;
pub mod source;
pub mod stream;
