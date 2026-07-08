//! Run-lifecycle extension seams.
//!
//! The engine ships no-op defaults; alternate builds plug in their own
//! behaviour — emitting run events to an external orchestration layer ([`RunSink`])
//! and accounting task usage ([`Meter`]). The source-side seam is
//! [`dagron_source::source::SourceFactory`] (additional ingestion backends).
//!
//! One-way dependency: alternate implementations depend on these traits,
//! never the reverse.

use std::sync::Arc;

use async_trait::async_trait;
use dagron_source::source::SourceFactory;

/// Notified when a run reaches a terminal state. Default: no-op. An alternate
/// implementation may emit the event to an external orchestration layer.
#[async_trait]
pub trait RunSink: Send + Sync {
    async fn on_run_completed(&self, _run_id: &str, _status: &str) {}
}

/// Usage-accounting hook, called as each task finishes. Default: no-op.
/// An alternate implementation may account or enforce quotas here.
#[async_trait]
pub trait Meter: Send + Sync {
    async fn on_task_completed(&self, _success: bool) {}
}

/// No-op [`RunSink`] (the default).
pub struct NoopRunSink;
#[async_trait]
impl RunSink for NoopRunSink {}

/// No-op [`Meter`] (the default).
pub struct NoopMeter;
#[async_trait]
impl Meter for NoopMeter {}

/// Extension seams handed to [`crate::run`]. [`Default`] is the built-in
/// configuration: built-in sources only, no run sink, no usage accounting.
pub struct Seams {
    /// Extra ingestion sources (e.g. queue backends) consulted before the
    /// built-in file/channel sources.
    pub source_factory: Option<Box<dyn SourceFactory>>,
    pub run_sink: Arc<dyn RunSink>,
    pub meter: Arc<dyn Meter>,
}

impl Default for Seams {
    fn default() -> Self {
        Self {
            source_factory: None,
            run_sink: Arc::new(NoopRunSink),
            meter: Arc::new(NoopMeter),
        }
    }
}
