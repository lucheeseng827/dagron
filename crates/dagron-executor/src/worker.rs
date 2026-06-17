use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::sync::mpsc::UnboundedSender;
use tracing::Instrument;

use crate::executor::{ExecContext, Executor};
use dagron_core::metrics::Metrics;

// ── Result type (produced by workers, consumed by the reconcile loop) ─────────

pub struct TaskResult {
    pub task_id: String,
    pub worker_id: String,
    pub success: bool,
    pub output: Option<String>,
    /// Pre-claim attempt value; the attempt that ran = attempt + 1.
    pub attempt: i64,
    pub max_attempts: u32,
    pub retry_delay_secs: u64,
    /// Per-claim fencing token (the post-claim `version`). Mutations only apply
    /// if the row still carries this version, so a stale attempt whose lease was
    /// reclaimed — even by this same process — cannot overwrite a newer attempt.
    pub fence: i64,
}

// ── Dispatch payload ──────────────────────────────────────────────────────────

pub struct DispatchPayload {
    pub task_id: String,
    pub worker_id: String,
    pub ctx: ExecContext,
    pub attempt: i64,
    pub max_attempts: u32,
    pub retry_delay_secs: u64,
    /// Per-claim fencing token (post-claim `version`); echoed back in TaskResult.
    pub fence: i64,
    /// Channel back to the reconcile loop — the worker sends its result here.
    pub result_tx: UnboundedSender<TaskResult>,
}

// ── WorkerActor ───────────────────────────────────────────────────────────────

/// Message type for the ractor worker actor.
///
/// Each actor processes one `Execute` message at a time; the actor's mailbox
/// queues additional dispatches while the current task is running. N actors
/// in the pool = N concurrent execution slots.
pub enum WorkerMsg {
    Execute(DispatchPayload),
}

struct WorkerActor;

struct WorkerState {
    executor: Arc<dyn Executor>,
    metrics: Arc<Metrics>,
}

#[async_trait]
impl Actor for WorkerActor {
    type Msg = WorkerMsg;
    type State = WorkerState;
    type Arguments = (Arc<dyn Executor>, Arc<Metrics>);

    async fn pre_start(
        &self,
        _myself: ActorRef<WorkerMsg>,
        (executor, metrics): (Arc<dyn Executor>, Arc<Metrics>),
    ) -> Result<WorkerState, ActorProcessingErr> {
        Ok(WorkerState { executor, metrics })
    }

    async fn handle(
        &self,
        myself: ActorRef<WorkerMsg>,
        message: WorkerMsg,
        state: &mut WorkerState,
    ) -> Result<(), ActorProcessingErr> {
        let WorkerMsg::Execute(p) = message;

        // Correlate every line emitted while this task runs (including events from
        // inside the executor backend) under one span, carrying the worker slot,
        // task, attempt and fence. With LOG_SPAN_EVENTS=close this span also yields
        // an automatic per-task timing event for SaaS dashboards.
        let slot = myself.get_name().unwrap_or_else(|| "worker".to_string());
        let span = tracing::info_span!(
            "task",
            worker = %slot,
            task_id = %p.task_id,
            attempt = p.attempt + 1,
            max_attempts = p.max_attempts,
            fence = p.fence,
        );

        let executor = Arc::clone(&state.executor);
        let metrics = Arc::clone(&state.metrics);
        async move {
            tracing::debug!(
                cmd = %p.ctx.command.first().map(String::as_str).unwrap_or("<empty>"),
                "task starting",
            );

            // Task wall time (claim→finish) — the workload signal the no-op load
            // test never exercised. Recorded whether the task succeeds or errors.
            let started = Instant::now();
            let result = executor.execute(&p.ctx).await;
            let elapsed = started.elapsed();
            metrics.observe_task_duration(elapsed.as_secs_f64());
            let duration_ms = elapsed.as_millis();

            let (success, output) = match result {
                Ok(o) => {
                    if o.success {
                        tracing::debug!(duration_ms, "task finished ok");
                    } else {
                        tracing::error!(duration_ms, "non-zero exit");
                    }
                    (o.success, Some(o.output))
                }
                Err(e) => {
                    tracing::error!(duration_ms, err = %e, "executor error");
                    (false, Some(e.to_string()))
                }
            };

            // Fire-and-forget — the reconcile loop drains this channel each tick.
            if p.result_tx.send(TaskResult {
                task_id: p.task_id.clone(),
                worker_id: p.worker_id,
                success,
                output,
                attempt: p.attempt,
                max_attempts: p.max_attempts,
                retry_delay_secs: p.retry_delay_secs,
                fence: p.fence,
            }).is_err() {
                tracing::warn!("result channel closed — reconcile loop may have exited");
            }
        }
        .instrument(span)
        .await;

        Ok(())
    }
}

// ── WorkerPool ────────────────────────────────────────────────────────────────

/// Pool of N `WorkerActor` instances.
///
/// Tasks are dispatched round-robin. For distributed workers, replace the
/// local `ActorRef` vec with remote refs (ractor-cluster) — the dispatch
/// interface is identical.
pub struct WorkerPool {
    workers: Vec<ActorRef<WorkerMsg>>,
    // Keep join handles alive so the runtime does not drop the actors.
    _handles: Vec<tokio::task::JoinHandle<()>>,
    next: AtomicUsize,
}

impl WorkerPool {
    pub async fn new(
        size: usize,
        executor: Arc<dyn Executor>,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        if size == 0 {
            anyhow::bail!("worker pool size must be at least 1");
        }
        let mut workers = Vec::with_capacity(size);
        let mut handles = Vec::with_capacity(size);
        for i in 0..size {
            let (actor_ref, handle) = WorkerActor::spawn(
                Some(format!("worker-{i}")),
                WorkerActor,
                (Arc::clone(&executor), Arc::clone(&metrics)),
            )
            .await
            .map_err(|e| anyhow::anyhow!("spawn worker-{i}: {e}"))?;
            workers.push(actor_ref);
            handles.push(handle);
        }
        Ok(Self { workers, _handles: handles, next: AtomicUsize::new(0) })
    }

    /// Round-robin dispatch: send a task to the next worker actor.
    pub fn dispatch(&self, payload: DispatchPayload) -> Result<()> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[idx]
            .cast(WorkerMsg::Execute(payload))
            .map_err(|_| anyhow::anyhow!("worker-{idx} mailbox closed"))?;
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.workers.len()
    }
}
