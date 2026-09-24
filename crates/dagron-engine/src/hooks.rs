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
///
/// **Called once per task that reaches `succeeded` or `failed`, whichever path
/// got it there** — a worker result, a memoization cache hit, or a reconcile
/// sweep resolving a parked `wait` / `wait.url` / `wait.dataset` sensor, a
/// sub-workflow trigger, an approval gate, or a deferred `defer:` remote job.
/// The engine routes every one of those through a single internal helper so
/// this hook and the `scheduler_tasks_{succeeded,failed}_total` counters cannot
/// drift apart; a crate test makes bypassing it a compile-and-test failure.
/// A task the engine could not claim (stale fence) changed nothing and is not
/// reported.
///
/// **Cancellation is not reported here.** `cancel_run` and gang-sibling
/// cancellation terminalize rows as `cancelled`, which is neither arm of this
/// bool — reporting one as a failure would spend quota a tenant never used.
/// A quota keyed on this hook therefore bounds work that *ran*, not work that
/// was called off.
#[async_trait]
pub trait Meter: Send + Sync {
    async fn on_task_completed(&self, _success: bool) {}
}

/// What one poll of a deferred task's remote job found.
///
/// Three states, not two. A transport failure is **not** a verdict and must not
/// be encoded here — it is an `Err` from [`ExternalPoller::poll`], which the
/// sweep re-parks with backoff. Collapsing a 429 into `Failed` would fail a
/// healthy job because its vendor rate-limited us; collapsing it into `Running`
/// would swallow the rate limit and keep hammering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Still going. The sweep re-parks it for another `defer.poll_secs`.
    Running,
    /// Finished successfully. `output` becomes the task's output (and so is
    /// what a downstream `{{ tasks.X.output }}` or `when:` reads).
    Succeeded { output: String },
    /// Finished badly. `reason` becomes the task's output, so the engine's
    /// existing fault classification and `retry_budgets:` see the remote
    /// system's own error text and act on it unchanged.
    Failed { reason: String },
}

/// Everything a poller gets about one parked row.
///
/// A struct rather than `(kind, handle)` because a poller that coalesces calls
/// needs a cache key, and one resolving a named connection needs the run's
/// identity. Both are decided here or not at all: a closed implementation
/// cannot widen this after the fact.
#[derive(Debug, Clone)]
pub struct PollCtx<'a> {
    /// The task's `defer.kind` — what the poller matches on.
    pub kind: &'a str,
    /// The remote job's opaque identity, as the submit reported it.
    pub handle: &'a str,
    /// Where to reach it, pinned at submit. `None` when the handle suffices.
    pub endpoint: Option<&'a str>,
    /// The run this task belongs to.
    pub run_id: &'a str,
    /// The parked `task_runs.id` — also the stable half of the remote job's
    /// name, `dagron-<task_id>-<epoch>`.
    pub task_id: &'a str,
    /// Submission generation, the other half of that name.
    pub epoch: i64,
}

/// Resolves a deferred task's remote job. Default: absent, and the built-in
/// kinds answer for themselves.
///
/// `Result<Option<Verdict>>` rather than `Option<Verdict>`, deliberately, and
/// the shape is [`SourceFactory::build`]'s:
/// * `Ok(None)` — not my kind; fall through to the built-in pollers.
/// * `Ok(Some(v))` — I own this kind and this is what I found.
/// * `Err(e)` — I own this kind and could not reach it. The sweep re-parks
///   with backoff; it is not a verdict about the job.
///
/// Without the `Err` arm a transport failure would have to masquerade as one of
/// the other two, and both lies are expensive — see [`Verdict`].
#[async_trait]
pub trait ExternalPoller: Send + Sync {
    /// Is this job done?
    async fn poll(&self, ctx: &PollCtx<'_>) -> anyhow::Result<Option<Verdict>>;

    /// Tear the remote job down — the run was cancelled, or its deadline
    /// elapsed. `Ok(None)` = not my kind. Best-effort by nature: a job we
    /// cannot reach is a job we cannot stop, and the sweep says so out loud
    /// rather than pretending the cluster is idle.
    async fn cancel(&self, _ctx: &PollCtx<'_>) -> anyhow::Result<Option<()>> {
        Ok(None)
    }
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
    /// Resolves `defer:` kinds the built-ins do not own.
    ///
    /// `Arc`, not `Box` like `source_factory`: that one is consumed once at
    /// startup, while this is consulted per parked row on every sweep and the
    /// sweep polls rows concurrently.
    pub external_poller: Option<Arc<dyn ExternalPoller>>,
}

impl Default for Seams {
    fn default() -> Self {
        Self {
            source_factory: None,
            run_sink: Arc::new(NoopRunSink),
            meter: Arc::new(NoopMeter),
            external_poller: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A poller written the way a closed implementation would write one: owns
    /// one kind, falls through on every other, and distinguishes "cannot reach
    /// it" from "it failed".
    struct FakePoller;

    #[async_trait]
    impl ExternalPoller for FakePoller {
        async fn poll(&self, ctx: &PollCtx<'_>) -> anyhow::Result<Option<Verdict>> {
            if ctx.kind != "fake" {
                return Ok(None); // not mine — fall through to the built-ins
            }
            match ctx.handle {
                "done" => Ok(Some(Verdict::Succeeded { output: "COMPLETED".into() })),
                "bad" => Ok(Some(Verdict::Failed { reason: "OOM".into() })),
                "throttled" => Err(anyhow::anyhow!("429 Too Many Requests")),
                _ => Ok(Some(Verdict::Running)),
            }
        }

        async fn cancel(&self, ctx: &PollCtx<'_>) -> anyhow::Result<Option<()>> {
            if ctx.kind != "fake" {
                return Ok(None);
            }
            Ok(Some(()))
        }
    }

    fn ctx<'a>(kind: &'a str, handle: &'a str) -> PollCtx<'a> {
        PollCtx { kind, handle, endpoint: None, run_id: "r-1", task_id: "t-1", epoch: 0 }
    }

    /// The seam's whole contract in one test: three verdicts, a fall-through,
    /// and a transport error that is NOT a verdict.
    #[tokio::test]
    async fn a_registered_poller_distinguishes_all_four_outcomes() {
        let p = FakePoller;

        assert_eq!(
            p.poll(&ctx("fake", "done")).await.unwrap(),
            Some(Verdict::Succeeded { output: "COMPLETED".into() })
        );
        assert_eq!(
            p.poll(&ctx("fake", "bad")).await.unwrap(),
            Some(Verdict::Failed { reason: "OOM".into() })
        );
        assert_eq!(p.poll(&ctx("fake", "anything")).await.unwrap(), Some(Verdict::Running));

        // Not my kind → fall through, NOT a failure. This is what lets a build
        // carry several pollers and the built-in kinds side by side.
        assert_eq!(p.poll(&ctx("spark-k8s", "x")).await.unwrap(), None);

        // Could not reach it → Err. If this had to be encoded as a Verdict, a
        // rate limit would either fail a healthy job or be swallowed entirely;
        // the sweep re-parks on Err instead.
        assert!(p.poll(&ctx("fake", "throttled")).await.is_err());
    }

    /// `Seams` is constructible with a poller, and `Default` leaves it absent —
    /// so an engine built without one parks rather than resolves.
    #[test]
    fn seams_carries_an_optional_poller() {
        assert!(Seams::default().external_poller.is_none());
        let seams = Seams { external_poller: Some(Arc::new(FakePoller)), ..Default::default() };
        assert!(seams.external_poller.is_some());
    }

    /// `cancel` has a default so a poller that cannot tear a job down does not
    /// have to pretend it can.
    #[tokio::test]
    async fn cancel_defaults_to_not_mine() {
        struct PollOnly;
        #[async_trait]
        impl ExternalPoller for PollOnly {
            async fn poll(&self, _: &PollCtx<'_>) -> anyhow::Result<Option<Verdict>> {
                Ok(Some(Verdict::Running))
            }
        }
        assert_eq!(PollOnly.cancel(&ctx("whatever", "h")).await.unwrap(), None);
    }
}
