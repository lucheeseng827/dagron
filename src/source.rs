// SPDX-License-Identifier: Apache-2.0
//! Workflow ingestion sources.
//!
//! Where [`Executor`](crate::executor::Executor) abstracts *how a task runs*,
//! [`WorkflowSource`] abstracts *where new workflows come from*. The OSS
//! distribution ships two zero-infra sources:
//!
//! * [`FileSource`] — emits one bundled DAG file then drains; the default.
//! * [`ChannelSource`] — an in-process `mpsc` queue for embedding the runner in
//!   another process or driving it from tests.
//!
//! Real queue backends (Redis / SQS / Kafka …) implement the same trait and live
//! in separate distributions; nothing here is queue-specific.

use anyhow::Result;
use async_trait::async_trait;

/// One workflow submission pulled from a source. `payload` is the raw DAG spec
/// (YAML — a superset of JSON, so JSON payloads parse too). `handle` is an opaque
/// per-source token used to ack/nack the underlying message once the run has been
/// durably accepted (at-least-once delivery).
pub struct WorkflowMessage {
    pub payload: String,
    pub handle: AckHandle,
}

/// Opaque acknowledgement token. Queue sources stash whatever they need to ack or
/// redeliver a message. In-process / one-shot sources use [`AckHandle::None`].
pub enum AckHandle {
    None,
    /// Carried by queue sources (e.g. a receipt handle / payload).
    String(String),
}

/// A stream of workflow submissions.
///
/// `recv` blocks until the next submission is available, or returns `Ok(None)`
/// when the source is *permanently* exhausted (e.g. a one-shot file). Streaming
/// queue backends never return `None` under normal operation. `ack`/`nack`
/// default to no-ops for sources without delivery semantics.
#[async_trait]
pub trait WorkflowSource: Send + 'static {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>>;

    /// Acknowledge that the run was durably accepted; remove the message so it is
    /// not redelivered.
    async fn ack(&mut self, _handle: &AckHandle) -> Result<()> {
        Ok(())
    }

    /// The message could not be turned into a run; ask the source to redeliver
    /// (or eventually dead-letter) it.
    async fn nack(&mut self, _handle: &AckHandle) -> Result<()> {
        Ok(())
    }
}

// ── FileSource ────────────────────────────────────────────────────────────────

/// Emits a single DAG file once, then drains — the "run one workflow and exit"
/// default source.
pub struct FileSource {
    path: String,
    emitted: bool,
}

impl FileSource {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            emitted: false,
        }
    }
}

#[async_trait]
impl WorkflowSource for FileSource {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>> {
        if self.emitted {
            return Ok(None);
        }
        let payload = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|e| anyhow::anyhow!("cannot read DAG file '{}': {e}", self.path))?;
        // Only mark exhausted once the read succeeds, so a transient I/O error
        // leaves the source retryable rather than permanently drained.
        self.emitted = true;
        Ok(Some(WorkflowMessage {
            payload,
            handle: AckHandle::None,
        }))
    }
}

// ── ChannelSource ─────────────────────────────────────────────────────────────

/// In-process `mpsc` queue — the zero-infra reference "queue" backend. Useful for
/// tests and for embedding the runner in a larger process that produces workflow
/// specs directly. `recv` returns `None` once the sender is dropped.
pub struct ChannelSource {
    rx: tokio::sync::mpsc::Receiver<String>,
}

impl ChannelSource {
    pub fn new(rx: tokio::sync::mpsc::Receiver<String>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl WorkflowSource for ChannelSource {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>> {
        Ok(self.rx.recv().await.map(|payload| WorkflowMessage {
            payload,
            handle: AckHandle::None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn channel_source_yields_then_drains() {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tx.send("name: a\ntasks: []".to_string()).await.unwrap();
        drop(tx);

        let mut src = ChannelSource::new(rx);
        assert_eq!(
            src.recv().await.unwrap().unwrap().payload,
            "name: a\ntasks: []"
        );
        assert!(
            src.recv().await.unwrap().is_none(),
            "drains to None once sender drops"
        );
    }

    #[tokio::test]
    async fn file_source_emits_once_then_none() {
        let dir = std::env::temp_dir();
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = dir.join(format!(
            "dagron-oss-src-{}-{unique}.yaml",
            std::process::id()
        ));
        tokio::fs::write(&path, "name: one\ntasks: []")
            .await
            .unwrap();

        let mut src = FileSource::new(path.to_string_lossy().to_string());
        assert!(src.recv().await.unwrap().is_some());
        assert!(
            src.recv().await.unwrap().is_none(),
            "one-shot file drains after first emit"
        );

        tokio::fs::remove_file(&path).await.ok();
    }
}
