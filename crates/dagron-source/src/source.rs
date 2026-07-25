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
//! Three sources are always compiled (zero-infra):
//!
//! * [`FileSource`] — emits one bundled DAG file then drains; preserves the
//!   original single-run behaviour and is the default.
//! * [`StreamSource`](crate::stream::StreamSource) — follows an append-only
//!   NDJSON file or named pipe (`SOURCE=stream`), one workflow submission per
//!   line, at-least-once with a durable offset checkpoint — the built-in
//!   streaming on-ramp.
//! * [`ChannelSource`] — an in-process `mpsc` queue; the reference "generic
//!   queue" used in tests and for embedding the scheduler in another process.
//!
//! Managed broker connectors (Kafka, NATS, SQS, Redis) and the CloudEvents
//! webhook gateway are part of **dagron Enterprise**; they plug in through the
//! [`SourceFactory`] seam below, implementing this same trait. A custom backend
//! can be wired in the identical way (`dagron_engine::Seams::source_factory`).

use anyhow::{bail, Result};
use async_trait::async_trait;

/// A source's resumable coordinate for the message currently in flight — what
/// the ingest actor commits in the same transaction as the run it creates.
/// `substream` names the partition for multi-consumer sources (`None` = the
/// source has one cursor); the committed row is keyed
/// `<source>` or `<source>/<substream>` accordingly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingPosition {
    pub substream: Option<String>,
    pub position: String,
}

impl PendingPosition {
    /// A whole-source (unpartitioned) coordinate.
    pub fn whole(position: impl Into<String>) -> Self {
        Self { substream: None, position: position.into() }
    }

    /// The `source_offsets` key this coordinate commits under.
    pub fn offset_key(&self, source_name: &str) -> String {
        match &self.substream {
            Some(s) => format!("{source_name}/{s}"),
            None => source_name.to_string(),
        }
    }
}

/// One workflow submission pulled from a source. `payload` is the raw DAG spec
/// (YAML — which is a superset of JSON, so JSON payloads parse too). `handle` is
/// an opaque per-source token used to ack/nack the underlying message once the
/// run has been durably created (at-least-once delivery).
pub struct WorkflowMessage {
    pub payload: String,
    pub handle: AckHandle,
}

/// Opaque acknowledgement token. Sources with delivery semantics stash whatever
/// they need to ack or redeliver the message — the stream source's next byte
/// offset, an SQS receipt handle, the Redis processing-list payload, etc.
/// In-process / one-shot sources use [`AckHandle::None`].
pub enum AckHandle {
    None,
    /// Carried by sources with real delivery semantics (offset / receipt handle).
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

    // ── Exactly-once (transactional offsets) ─────────────────────────────────
    // A source with a resumable coordinate (byte offset, partition offset,
    // replication LSN) opts into exactly-once run creation by implementing the
    // pair below. The ingest actor then commits the coordinate **in the same
    // datastore transaction** as the run it accounts for
    // (`db::create_run_with_offset`), and hands the committed cursor back at
    // startup — a crash between run-create and broker ack can redeliver the
    // message, but the source (repositioned past the committed cursor) never
    // re-creates its run. Sources without a coordinate (file/channel/webhook)
    // keep the defaults and stay at-least-once.

    /// The coordinate to commit atomically with the run created from the
    /// message most recently returned by `recv` — typically "the position just
    /// past it". `None` (default) = no transactional offset for this message.
    /// A partitioned source names the shard via
    /// [`PendingPosition::substream`], which namespaces the committed row.
    fn pending_position(&self) -> Option<PendingPosition> {
        None
    }

    /// Hand the source its durably-committed cursor before the first `recv`
    /// (`None` = nothing committed yet). A resuming source starts strictly
    /// after this position. Default: ignored.
    async fn set_committed_position(&mut self, _position: Option<String>) -> Result<()> {
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
    build_inner(kind, file_path, None, extra).await
}

/// [`build_with`] plus a datastore pool, which unlocks the multi-consumer
/// built-ins (a `STREAM_PATH` directory becomes a
/// [`ShardedStreamSource`](crate::stream::ShardedStreamSource) splitting its
/// shard files across engines via per-partition leases). The engine calls
/// this; `build_with` remains for pool-less embedding.
pub async fn build_pooled(
    kind: &str,
    file_path: &str,
    pool: &dagron_core::db::Pool,
    extra: Option<&dyn SourceFactory>,
) -> Result<Box<dyn WorkflowSource>> {
    build_inner(kind, file_path, Some(pool), extra).await
}

async fn build_inner(
    kind: &str,
    file_path: &str,
    pool: Option<&dagron_core::db::Pool>,
    extra: Option<&dyn SourceFactory>,
) -> Result<Box<dyn WorkflowSource>> {
    if let Some(factory) = extra {
        if let Some(src) = factory.build(kind, file_path).await? {
            return Ok(src);
        }
    }
    Ok(match kind {
        "file" => Box::new(FileSource::new(file_path)),
        "stream" => {
            let cfg = crate::stream::StreamConfig::from_env()?;
            // Mode is fixed at startup, so decide it explicitly: STREAM_MODE=
            // file|sharded|auto (default auto: an existing dir → sharded, an
            // existing file/FIFO → file, and an ABSENT path is an error rather
            // than a silent guess — a shard dir created later would otherwise
            // be tailed as a single file forever).
            let mode = std::env::var("STREAM_MODE")
                .unwrap_or_else(|_| "auto".to_string())
                .trim()
                .to_ascii_lowercase();
            let sharded = match mode.as_str() {
                "file" => false,
                "sharded" => true,
                "auto" | "" => match tokio::fs::metadata(&cfg.path).await {
                    Ok(m) => m.is_dir(),
                    Err(_) => bail!(
                        "STREAM_PATH '{}' does not exist, so the stream mode cannot be \
                         inferred. Create the file/FIFO or shard directory first, or set \
                         STREAM_MODE=file (wait for a single stream file) or \
                         STREAM_MODE=sharded (a directory of shard files).",
                        cfg.path.display()
                    ),
                },
                other => bail!("unknown STREAM_MODE '{other}' (file | sharded | auto)"),
            };
            if sharded {
                let Some(pool) = pool else {
                    bail!(
                        "STREAM_PATH '{}' selects sharded streaming — sharded \
                         consumption needs the engine's datastore for partition leases; \
                         use source::build_pooled",
                        cfg.path.display()
                    );
                };
                Box::new(crate::stream::ShardedStreamSource::new(
                    pool.clone(),
                    kind,
                    cfg.path,
                ))
            } else {
                Box::new(crate::stream::StreamSource::new(cfg))
            }
        }
        // The managed connector suite: name each kind precisely so the error is
        // an accurate signpost, not a dead end — the open build streams via
        // SOURCE=stream, embeds via the SourceFactory seam, and the managed
        // brokers ship with dagron Enterprise.
        "redis" | "sqs" | "kafka" | "nats" | "events" => bail!(
            "ingestion connector '{kind}' is not bundled in this build. Managed broker \
             connectors (Kafka, NATS, SQS, Redis) and the CloudEvents webhook gateway ship \
             with dagron Enterprise — https://github.com/lucheeseng827/dagron#dagron-enterprise. \
             This build streams out of the box with SOURCE=stream (follow an NDJSON event \
             file or named pipe, at-least-once with a durable offset checkpoint — see \
             docs/STREAMING.md), and custom backends plug in via the SourceFactory seam \
             (dagron_engine::Seams)."
        ),
        other => bail!(
            "unknown SOURCE '{other}'. Built-in kinds: 'file' (one-shot), 'stream' \
             (follow an NDJSON file/pipe). Additional connectors register through the \
             SourceFactory seam."
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

    /// The enterprise connector kinds fail with a signpost (what it is, where it
    /// ships, what the open build offers instead) — the OSS→Enterprise funnel.
    #[tokio::test]
    async fn enterprise_connector_kinds_error_with_signpost() {
        for kind in ["redis", "sqs", "kafka", "nats", "events"] {
            let err = build_with(kind, "unused.yaml", None).await.err().unwrap_or_else(|| {
                panic!("SOURCE={kind} must error without a registered SourceFactory")
            });
            let msg = err.to_string();
            assert!(msg.contains("dagron Enterprise"), "{kind}: names the edition ({msg})");
            assert!(msg.contains("SOURCE=stream"), "{kind}: offers the built-in path ({msg})");
            assert!(msg.contains("SourceFactory"), "{kind}: offers the seam ({msg})");
        }
    }

    /// A registered factory is consulted before the built-ins — the seam the
    /// enterprise build (and any custom embedding) hooks.
    #[tokio::test]
    async fn source_factory_overrides_builtin_error() {
        struct Fixed;
        #[async_trait]
        impl SourceFactory for Fixed {
            async fn build(
                &self,
                kind: &str,
                _file_path: &str,
            ) -> Result<Option<Box<dyn WorkflowSource>>> {
                Ok(match kind {
                    "kafka" => {
                        let (_tx, rx) = tokio::sync::mpsc::channel(1);
                        Some(Box::new(ChannelSource::new(rx)) as Box<dyn WorkflowSource>)
                    }
                    _ => None,
                })
            }
        }
        assert!(build_with("kafka", "unused.yaml", Some(&Fixed)).await.is_ok());
        assert!(build_with("nats", "unused.yaml", Some(&Fixed)).await.is_err(), "unhandled kind still funnels");
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
