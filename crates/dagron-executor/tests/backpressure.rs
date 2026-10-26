//! What the worker reports when a backend says "not now".
//!
//! `executor.rs` unit-tests the marker type and `kube_executor.rs` the one
//! backend that returns it. What neither reaches is the hop between them: an
//! `anyhow` error crossing the `Executor` trait object, through the worker actor,
//! and arriving at the reconcile loop as a `TaskResult`. The engine decides
//! whether a task waits or fails from that struct alone, so this is where "the
//! type survived" has to be true.
//!
//! One pool, and so one `#[tokio::test]`, for all three kinds of error: worker
//! actors register under process-wide names (`worker-0`, …), and a second pool
//! in the same process is refused.
//!
//! The last test pins a string contract with another crate, as
//! `pool_hardening.rs` does: a back-pressure result the engine does *not* wait on
//! (a gang member, or the wait turned off) falls through to the ordinary failure
//! path, where the fault classifier reads its text.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use dagron_core::metrics::Metrics;
use dagron_executor::executor::{
    Backpressure, ExecContext, ExecOutput, Executor, LogChunk, TimeoutError,
};
use dagron_executor::worker::{DispatchPayload, TaskResult, WorkerPool};
use tokio::sync::mpsc;

/// What a full namespace quota reads like by the time it is a task's reason.
const REASON: &str = "pod refused by namespace ResourceQuota 'tenant-quota' \
                      (requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4)";

/// A backend that starts nothing. The task's command picks which error it
/// answers with, so the one pool can be asked for each kind in turn. Only
/// `execute` is written; the sweep methods keep their defaults.
struct Refusing;

#[async_trait]
impl Executor for Refusing {
    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
        Err(match ctx.command[0].as_str() {
            "quota" => anyhow::Error::new(Backpressure { reason: REASON.into() }),
            "deadline" => anyhow::Error::new(TimeoutError { secs: 5 }),
            other => anyhow::anyhow!("create pod 'sched-x': {other}"),
        })
    }
}

/// One dispatch, the way the reconcile loop makes it: a result channel and a
/// live-log channel. Returns the result and what reached the live log.
async fn dispatch(pool: &WorkerPool, command: &str) -> (TaskResult, Vec<LogChunk>) {
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();
    let (log_tx, mut log_rx) = mpsc::unbounded_channel();
    pool.dispatch(DispatchPayload {
        task_id: "t1".into(),
        worker_id: "w1".into(),
        ctx: ExecContext::new(vec![command.into()], Some(5), None),
        attempt: 0,
        max_attempts: 3,
        retry_delay_secs: 0,
        retry_max_delay_secs: None,
        retry_on_timeout: true,
        fence: 7,
        spec: None,
        teardown: false,
        result_tx,
        log_tx: Some(log_tx),
    })
    .expect("the worker takes the task");

    let result = tokio::time::timeout(Duration::from_secs(10), result_rx.recv())
        .await
        .expect("the worker reports within 10 s")
        .expect("and reports exactly one result");
    let mut log = Vec::new();
    while let Ok(chunk) = log_rx.try_recv() {
        log.push(chunk);
    }
    (result, log)
}

/// The task-duration histogram, as far as a build without the ops renderer can
/// see it: its `Debug` shows every counter, which is enough to tell "recorded
/// an observation" from "did not".
fn durations(metrics: &Metrics) -> String {
    format!("{:?}", metrics.task_duration)
}

#[tokio::test]
async fn the_worker_reports_backpressure_only_for_the_marker_type() {
    let metrics = Arc::new(Metrics::new());
    let pool = WorkerPool::new(1, Arc::new(Refusing), Arc::clone(&metrics))
        .await
        .expect("a one-worker pool starts");
    let untouched = durations(&metrics);

    // ── The marker: back-pressure, and nothing else ──────────────────────
    let (r, log) = dispatch(&pool, "quota").await;
    assert!(!r.success);
    assert!(r.backpressure, "the marker type must survive the trait object and the actor");
    assert!(!r.timed_out, "a refusal to start is not a deadline kill");
    // The claim it answers for, echoed so the loop can undo exactly that one.
    assert_eq!((r.task_id.as_str(), r.worker_id.as_str(), r.fence, r.attempt), ("t1", "w1", 7, 0));

    // The reason is the task's output and its live log, as for any other
    // executor error: both are what a waiting task shows.
    let text = format!("not started: {REASON}");
    assert_eq!(r.output.as_deref(), Some(text.as_str()));
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].chunk, format!("{text}\n"));
    assert_eq!((log[0].task_id.as_str(), log[0].fence), ("t1", 7));

    // Nothing ran, so there is no task duration: a namespace at its quota would
    // otherwise fill the histogram with refusals a few milliseconds long.
    assert_eq!(durations(&metrics), untouched, "a refusal recorded a task duration");

    // ── The same words in an ordinary error: an ordinary failure ─────────
    // Which is what a backend that formats the marker into a new error gets.
    let (r, _) = dispatch(&pool, text.as_str()).await;
    assert!(!r.success);
    assert!(!r.backpressure);
    assert!(!r.timed_out);
    assert_eq!(r.output, Some(format!("create pod 'sched-x': {text}")));
    assert_ne!(durations(&metrics), untouched, "a failed attempt still records its duration");

    // ── A deadline kill stays a timeout: the markers do not shadow ───────
    let (r, _) = dispatch(&pool, "deadline").await;
    assert!(!r.success);
    assert!(r.timed_out);
    assert!(!r.backpressure);
    assert_eq!(r.output.as_deref(), Some("command timed out after 5s"));
}

/// When the engine does not wait on a back-pressure result — a `gang:` member
/// under `RUNNER_GANGS`, or the wait turned off — the result takes the ordinary
/// failure path, and that path classifies the output text. It must find nothing:
/// a class would move the task onto that class's retry budget, and a refused pod
/// budgets on `max_attempts`. The classifier lives in another crate and reads
/// the message this one writes, so the two are pinned together here.
#[test]
fn the_backpressure_text_is_not_a_classified_fault() {
    let text = Backpressure { reason: REASON.into() }.to_string();
    let class = dagron_core::fault::classify_text(&text).map(|c| c.class);
    assert_eq!(class, None, "{text:?} is classified");
    // What the failure path does with that verdict. A `max_attempts` of 2 is a
    // budget no fault class defaults to, so this holds only on the task's own.
    let retries = |attempt| {
        dagron_core::models::should_retry_failed_with_class(attempt, 2, false, true, class, None)
    };
    assert!(retries(1) && !retries(2), "{text:?} does not budget on max_attempts");
}
