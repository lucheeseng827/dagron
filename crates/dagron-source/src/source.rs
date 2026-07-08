//! Workflow ingestion sources (v4).
//!
//! Where the `Executor` trait (dagron-executor) abstracts *how a task
//! runs*, [`WorkflowSource`] abstracts *where new workflows come from*. The
//! scheduler is no longer a one-shot "run this YAML and exit" process: a
//! [`WorkflowSource`] is a stream of workflow submissions that the
//! [`IngestActor`](crate::ingest::IngestActor) pulls, parses, and turns into runs
//! via [`db::create_run`](dagron_core::db::create_run) — for as long as the process
//! lives. That makes the scheduler a durable daemon that can sit in front of a
//! high-throughput queue (SQS / Kafka / Redis) and absorb a large influx, with
//! the queue itself buffering bursts and `MAX_INFLIGHT_RUNS` providing admission
//! backpressure.
//!
//! Two sources are always compiled (zero-infra):
//!
//! * [`FileSource`] — emits one bundled DAG file then drains; preserves the
//!   original single-run behaviour and is the default.
//! * [`ChannelSource`] — an in-process `mpsc` queue; the reference "generic
//!   queue" used in tests and for embedding the scheduler in another process.
//!
//! Three real queue backends are feature-gated, each behind its own Cargo
//! feature and implementing the identical trait:
//!
//! | Source | Feature | Transport |
//! |---|---|---|
//! | `RedisSource` | `redis` | reliable list (`BLMOVE` + `LREM` ack) |
//! | `SqsSource`   | `sqs`   | long-poll `ReceiveMessage` + `DeleteMessage` ack |
//! | `KafkaSource` | `kafka` | `StreamConsumer` + manual offset commit on ack |

use anyhow::{bail, Result};
use async_trait::async_trait;

/// One workflow submission pulled from a source. `payload` is the raw DAG spec
/// (YAML — which is a superset of JSON, so JSON payloads parse too). `handle` is
/// an opaque per-source token used to ack/nack the underlying message once the
/// run has been durably created (at-least-once delivery).
pub struct WorkflowMessage {
    pub payload: String,
    pub handle: AckHandle,
}

/// Opaque acknowledgement token. Queue sources stash whatever they need to ack
/// or redeliver the message — an SQS receipt handle, the Redis processing-list
/// payload, etc. In-process / one-shot sources use [`AckHandle::None`].
pub enum AckHandle {
    None,
    /// Carried only by feature-gated queue sources (receipt handle / payload).
    #[allow(dead_code)]
    String(String),
}

/// A stream of workflow submissions.
///
/// `recv` blocks until the next submission is available, or returns `Ok(None)`
/// when the source is *permanently* exhausted (e.g. a one-shot file). Streaming
/// queue backends never return `None` under normal operation — they block until
/// a message arrives. `ack`/`nack` default to no-ops for sources without
/// delivery semantics.
#[async_trait]
pub trait WorkflowSource: Send + 'static {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>>;

    /// Acknowledge that the run was durably created; remove the message so it is
    /// not redelivered.
    async fn ack(&mut self, _handle: &AckHandle) -> Result<()> {
        Ok(())
    }

    /// The message could not be turned into a run; ask the source to redeliver
    /// (or eventually dead-letter) it.
    async fn nack(&mut self, _handle: &AckHandle) -> Result<()> {
        Ok(())
    }

    /// Route a poison submission to this source's **broker-native** dead-letter
    /// destination (SQS DLQ, Kafka DLT topic, Redis DLQ list, NATS DLQ subject),
    /// so downstream consumers/alerting on the broker see it too. The durable
    /// `dead_letters` Postgres row is recorded by the ingest actor regardless;
    /// this is the broker-side mirror. Default: no-op (file/channel have no
    /// broker, and queue backends with no DLQ configured stay Postgres-only).
    async fn dead_letter(&mut self, _payload: &str, _error: &str) -> Result<()> {
        Ok(())
    }
}

// ── FileSource ────────────────────────────────────────────────────────────────

/// Emits a single DAG file once, then drains. Preserves the original
/// "run one workflow and exit" behaviour — the default source.
pub struct FileSource {
    path: String,
    emitted: bool,
}

impl FileSource {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into(), emitted: false }
    }
}

#[async_trait]
impl WorkflowSource for FileSource {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let payload = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|e| anyhow::anyhow!("cannot read DAG file '{}': {e}", self.path))?;
        Ok(Some(WorkflowMessage { payload, handle: AckHandle::None }))
    }
}

// ── ChannelSource ─────────────────────────────────────────────────────────────

/// In-process `mpsc` queue — the zero-infra reference "queue" backend. Useful
/// for tests and for embedding the scheduler in a larger process that produces
/// workflow specs directly. `recv` returns `None` once the sender is dropped.
#[allow(dead_code)] // library/embedding + test surface; not selectable via SOURCE env
pub struct ChannelSource {
    rx: tokio::sync::mpsc::Receiver<String>,
}

#[allow(dead_code)]
impl ChannelSource {
    pub fn new(rx: tokio::sync::mpsc::Receiver<String>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl WorkflowSource for ChannelSource {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>> {
        Ok(self
            .rx
            .recv()
            .await
            .map(|payload| WorkflowMessage { payload, handle: AckHandle::None }))
    }
}

// ── Source selection ──────────────────────────────────────────────────────────

/// Extension hook for ingestion sources beyond the built-in file/channel — e.g.
/// queue backends (Redis/SQS/Kafka/NATS). A build wires its factory into
/// [`build_with`]; the default engine passes `None` and supports only the
/// built-in sources.
#[async_trait]
pub trait SourceFactory: Send + Sync {
    /// Build the source for `kind`, or `Ok(None)` if this factory does not handle
    /// it (so the caller falls through to the built-ins, then errors).
    async fn build(&self, kind: &str, file_path: &str)
        -> Result<Option<Box<dyn WorkflowSource>>>;
}

/// Build the configured ingestion source, consulting `extra` (if any) before the
/// built-ins. `kind` comes from `$SOURCE` (default `file`); `file_path` is the DAG
/// path used by [`FileSource`]. Queue backends read their own settings from env.
pub async fn build_with(
    kind: &str,
    file_path: &str,
    extra: Option<&dyn SourceFactory>,
) -> Result<Box<dyn WorkflowSource>> {
    if let Some(factory) = extra {
        if let Some(src) = factory.build(kind, file_path).await? {
            return Ok(src);
        }
    }
    Ok(match kind {
        "file" => Box::new(FileSource::new(file_path)),
        other => bail!(
            "unknown SOURCE '{other}'. Built-in: 'file'. Queue backends \
             (redis/sqs/kafka/nats) require a registered SourceFactory."
        ),
    })
}

/// Built-in-only source selection. Use [`build_with`] to register
/// additional backends.
pub async fn build(kind: &str, file_path: &str) -> Result<Box<dyn WorkflowSource>> {
    build_with(kind, file_path, None).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn channel_source_yields_then_drains() {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tx.send("name: a\ntasks: []".to_string()).await.unwrap();
        tx.send("name: b\ntasks: []".to_string()).await.unwrap();
        drop(tx);

        let mut src = ChannelSource::new(rx);
        assert_eq!(src.recv().await.unwrap().unwrap().payload, "name: a\ntasks: []");
        assert_eq!(src.recv().await.unwrap().unwrap().payload, "name: b\ntasks: []");
        assert!(src.recv().await.unwrap().is_none(), "drains to None once sender drops");
    }

    #[tokio::test]
    async fn file_source_emits_once_then_none() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("module54-src-{}.yaml", uuid::Uuid::new_v4()));
        tokio::fs::write(&path, "name: one\ntasks: []").await.unwrap();

        let mut src = FileSource::new(path.to_string_lossy().to_string());
        assert!(src.recv().await.unwrap().is_some());
        assert!(src.recv().await.unwrap().is_none(), "one-shot file drains after first emit");

        tokio::fs::remove_file(&path).await.ok();
    }
}
