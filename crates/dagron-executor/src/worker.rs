use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::sync::mpsc::UnboundedSender;
use tracing::Instrument;

use crate::executor::{ExecContext, Executor, LogChunk, LogSink};
use dagron_core::dag::TaskSpec;
use dagron_core::metrics::Metrics;

// ── Lease heartbeat (long-running tasks) ──────────────────────────────────────

/// Renews a running task's lease while its executor is live — the worker
/// heartbeat that lets a task run longer than the claim-time lease window
/// (training jobs, long consumers) without being reclaimed mid-run. Implemented
/// by the engine over the datastore facade; a trait here keeps this crate
/// datastore-free, the same seam shape as [`LogSink`].
#[async_trait]
pub trait LeaseKeeper: Send + Sync + 'static {
    /// Extend the lease for this claim. `Ok(false)` means the claim is gone —
    /// the task was reclaimed (lease lapsed, e.g. during a long engine stall) —
    /// and the worker must abort its local execution: another attempt owns the
    /// task now, and burning compute here only risks duplicate side effects.
    /// A transient `Err` must NOT abort (a datastore blip while the task is
    /// healthy); the fencing token already protects state if the lease lapses.
    async fn renew(&self, task_id: &str, worker_id: &str, fence: i64) -> Result<bool>;
}

/// Default heartbeat interval — a third of the default 30 s claim-time lease:
/// two consecutive misses still leave headroom before the reconcile loop's
/// expired-lease sweep can reclaim the task. The engine passes the real value
/// (lease/3, from `LEASE_SECS`) into [`WorkerPool::with_lease_keeper`]; this
/// constant only backs [`WorkerPool::new`], which runs without a keeper anyway.
const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);

// ── Result type (produced by workers, consumed by the reconcile loop) ─────────

pub struct TaskResult {
    pub task_id: String,
    pub worker_id: String,
    pub success: bool,
    /// Whether this failure was a `timeout_secs` deadline kill (the executor
    /// error downcast to [`crate::executor::TimeoutError`]) rather than a
    /// non-zero exit or a backend error. Only meaningful when `!success`; drives
    /// retry-on-timeout gating (#24).
    pub timed_out: bool,
    pub output: Option<String>,
    /// Pre-claim attempt value; the attempt that ran = attempt + 1.
    pub attempt: i64,
    pub max_attempts: u32,
    pub retry_delay_secs: u64,
    /// Optional clamp on the exponential backoff (spec `retry_max_delay_secs`).
    pub retry_max_delay_secs: Option<u64>,
    /// Task policy echoed from dispatch: whether a deadline-killed attempt is
    /// retried (`retry_on_timeout`, default true). The retry branch reads it
    /// alongside `timed_out` so it never re-parses the spec (#24).
    pub retry_on_timeout: bool,
    /// Per-claim fencing token (the post-claim `version`). Mutations only apply
    /// if the row still carries this version, so a stale attempt whose lease was
    /// reclaimed — even by this same process — cannot overwrite a newer attempt.
    pub fence: i64,
    /// When the executor finished, stamped just before this result is sent.
    /// The reconcile loop observes `finished.elapsed()` at drain time as
    /// `scheduler_result_wait_seconds` — the direct measure of the
    /// wake-on-completion path (docs/LOW_LATENCY.md F1/A-5).
    pub finished: Instant,
    /// The task's parsed spec, echoed from [`DispatchPayload::spec`] so the
    /// reconcile loop's result handling (repeat / memoization / `produces:`)
    /// never re-reads and re-parses the row's input JSON per completion
    /// (docs/LOW_LATENCY.md B-4). `None` for a task with no stored input.
    pub spec: Option<Box<TaskSpec>>,
}

// ── Dispatch payload ──────────────────────────────────────────────────────────

pub struct DispatchPayload {
    pub task_id: String,
    pub worker_id: String,
    pub ctx: ExecContext,
    pub attempt: i64,
    pub max_attempts: u32,
    pub retry_delay_secs: u64,
    /// Optional clamp on the exponential backoff (spec `retry_max_delay_secs`);
    /// echoed back in [`TaskResult`].
    pub retry_max_delay_secs: Option<u64>,
    /// Whether a `timeout_secs` deadline kill should be retried (spec
    /// `retry_on_timeout`, default true); echoed back in [`TaskResult`] so the
    /// reconcile loop's retry branch can skip a doomed retry (#24).
    pub retry_on_timeout: bool,
    /// Per-claim fencing token (post-claim `version`); echoed back in TaskResult.
    pub fence: i64,
    /// The task's parsed spec, carried through execution and echoed back in
    /// [`TaskResult::spec`] — see there. The claim loop already paid the parse;
    /// this keeps the result path from paying a DB read plus a second parse.
    pub spec: Option<Box<TaskSpec>>,
    /// Channel back to the reconcile loop — the worker sends its result here.
    pub result_tx: UnboundedSender<TaskResult>,
    /// Optional live-log channel (#17). When set, the worker wires a per-task
    /// [`LogSink`] into the executor context so incremental output streams back to
    /// the loop for tailing. `None` disables streaming.
    pub log_tx: Option<UnboundedSender<LogChunk>>,
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

/// Spawn arguments for one worker actor — grew past a readable tuple.
struct WorkerInit {
    executor: Arc<dyn Executor>,
    metrics: Arc<Metrics>,
    lease_keeper: Option<Arc<dyn LeaseKeeper>>,
    /// How often to renew a running task's lease (the engine passes lease/3).
    heartbeat: Duration,
    /// This worker's index into the pool's shared busy flags.
    slot: usize,
    busy: Arc<Vec<AtomicBool>>,
}

struct WorkerState {
    executor: Arc<dyn Executor>,
    metrics: Arc<Metrics>,
    lease_keeper: Option<Arc<dyn LeaseKeeper>>,
    heartbeat: Duration,
    slot: usize,
    busy: Arc<Vec<AtomicBool>>,
}

/// Clears a worker's busy flag on drop, so the flag comes down on every exit
/// path from a task — including an unwinding panic inside an executor backend.
/// A flag that somehow never cleared would only degrade that slot to the old
/// round-robin fallback in [`WorkerPool::dispatch`], never deadlock it.
struct SlotFree(Arc<Vec<AtomicBool>>, usize);
impl Drop for SlotFree {
    fn drop(&mut self) {
        self.0[self.1].store(false, Ordering::Release);
    }
}

#[async_trait]
impl Actor for WorkerActor {
    type Msg = WorkerMsg;
    type State = WorkerState;
    type Arguments = WorkerInit;

    async fn pre_start(
        &self,
        _myself: ActorRef<WorkerMsg>,
        init: WorkerInit,
    ) -> Result<WorkerState, ActorProcessingErr> {
        Ok(WorkerState {
            executor: init.executor,
            metrics: init.metrics,
            lease_keeper: init.lease_keeper,
            heartbeat: init.heartbeat,
            slot: init.slot,
            busy: init.busy,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<WorkerMsg>,
        message: WorkerMsg,
        state: &mut WorkerState,
    ) -> Result<(), ActorProcessingErr> {
        let WorkerMsg::Execute(mut p) = message;

        // Correlate every line emitted while this task runs (including events from
        // inside the executor backend) under one span, carrying the worker slot,
        // task, attempt and fence. With LOG_SPAN_EVENTS=close this span also yields
        // an automatic per-task timing event for observability dashboards.
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
        let lease_keeper = state.lease_keeper.clone();
        let heartbeat_every = state.heartbeat;
        // Busy flag (idle-first dispatch, LOW_LATENCY B-5): `dispatch` set it
        // before casting; this guard clears it once the task — and its result
        // send — are done, on every exit path.
        let free_on_exit = SlotFree(Arc::clone(&state.busy), state.slot);
        async move {
            let _free_on_exit = free_on_exit;
            tracing::debug!(
                cmd = %p.ctx.command.first().map(String::as_str).unwrap_or("<empty>"),
                "task starting",
            );

            // Secret masking (#8): mask any sensitive task-env values out of the
            // captured output *before* it is persisted or surfaced. Built from
            // this task's env, so it covers every executor backend centrally.
            let redactor = crate::redact::Redactor::from_task_env(&p.ctx.env);

            // Live-log streaming (#17): wire a per-attempt sink so the executor can
            // stream incremental (redacted) output back to the loop for tailing.
            if let Some(log_tx) = p.log_tx.take() {
                p.ctx.log_sink = Some(LogSink::new(
                    log_tx,
                    p.task_id.clone(),
                    p.fence,
                    redactor.clone(),
                ));
            }

            // Task wall time (claim→finish) — the workload signal the no-op load
            // test never exercised. Recorded whether the task succeeds or errors.
            let started = Instant::now();

            // Heartbeat while the task runs: renew the claim's lease on an
            // interval so a task may outlive the claim-time lease window
            // (long training jobs, stream consumers). Losing the claim aborts
            // the execution — dropping the future kills a local/docker child
            // (`kill_on_drop`) — because another attempt owns the task now.
            // A transient renew error is only logged: the task is healthy, and
            // if the lease lapses anyway the fencing token voids our result.
            let result = match &lease_keeper {
                None => executor.execute(&p.ctx).await,
                Some(keeper) => {
                    let exec_fut = executor.execute(&p.ctx);
                    tokio::pin!(exec_fut);
                    let mut heartbeat = tokio::time::interval(heartbeat_every);
                    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    heartbeat.tick().await; // the immediate first tick — skip it
                    loop {
                        tokio::select! {
                            res = &mut exec_fut => break res,
                            _ = heartbeat.tick() => {
                                match keeper.renew(&p.task_id, &p.worker_id, p.fence).await {
                                    Ok(true) => {}
                                    Ok(false) => {
                                        break Err(anyhow::anyhow!(
                                            "task lease lost — the task was reclaimed for another \
                                             attempt; local execution aborted"
                                        ));
                                    }
                                    Err(e) => {
                                        tracing::warn!(error = %e, "lease heartbeat failed (transient) — task keeps running");
                                    }
                                }
                            }
                        }
                    }
                }
            };
            let elapsed = started.elapsed();
            metrics.observe_task_duration(elapsed.as_secs_f64());
            let duration_ms = elapsed.as_millis();

            let (success, timed_out, output) = match result {
                Ok(o) => {
                    if o.success {
                        tracing::debug!(duration_ms, "task finished ok");
                    } else {
                        tracing::error!(duration_ms, "non-zero exit");
                    }
                    // A clean Ok is never a timeout — every backend aborts a
                    // deadline via an Err carrying TimeoutError (handled below).
                    (o.success, false, Some(redactor.redact(&o.output).into_owned()))
                }
                Err(e) => {
                    // Distinguish a `timeout_secs` deadline kill from any other
                    // failure so the retry branch can honor `retry_on_timeout` (#24).
                    let timed_out = e.is::<crate::executor::TimeoutError>();
                    // The error string (e.g. a failed connection URL) can carry a
                    // secret too — mask it before it is logged or stored.
                    let msg = redactor.redact(&e.to_string()).into_owned();
                    tracing::error!(duration_ms, timed_out, err = %msg, "executor error");
                    (false, timed_out, Some(msg))
                }
            };

            // Fire-and-forget — the reconcile loop drains this channel each tick.
            if p.result_tx.send(TaskResult {
                task_id: p.task_id.clone(),
                worker_id: p.worker_id,
                success,
                timed_out,
                output,
                attempt: p.attempt,
                max_attempts: p.max_attempts,
                retry_delay_secs: p.retry_delay_secs,
                retry_max_delay_secs: p.retry_max_delay_secs,
                retry_on_timeout: p.retry_on_timeout,
                fence: p.fence,
                finished: Instant::now(),
                spec: p.spec,
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
    /// Round-robin rotor — the fallback when no verified-idle worker is found.
    next: AtomicUsize,
    /// Per-slot busy flags: set by [`Self::dispatch`], cleared by the worker
    /// when its task (and result send) finish. Basis of idle-first dispatch.
    busy: Arc<Vec<AtomicBool>>,
}

impl WorkerPool {
    pub async fn new(
        size: usize,
        executor: Arc<dyn Executor>,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        Self::with_lease_keeper(size, executor, metrics, None, LEASE_RENEW_INTERVAL).await
    }

    /// [`WorkerPool::new`] with a lease heartbeat: while a task executes, its
    /// worker renews the claim lease through `keeper` every `heartbeat` so
    /// long-running tasks are never reclaimed mid-run. The engine derives
    /// `heartbeat` from the configured lease window (lease/3) so shortening
    /// `LEASE_SECS` keeps the two-missed-renewals headroom. `None` keeper = no
    /// heartbeat (the pre-heartbeat behaviour: tasks must finish inside the
    /// claim-time lease window; `heartbeat` is unused).
    pub async fn with_lease_keeper(
        size: usize,
        executor: Arc<dyn Executor>,
        metrics: Arc<Metrics>,
        lease_keeper: Option<Arc<dyn LeaseKeeper>>,
        heartbeat: Duration,
    ) -> Result<Self> {
        if size == 0 {
            anyhow::bail!("worker pool size must be at least 1");
        }
        // `tokio::time::interval` panics on a zero period, and it would only
        // fire mid-task, long after the misconfiguration happened — clamp to
        // the compiled default instead of arming a delayed panic.
        let heartbeat = if heartbeat.is_zero() { LEASE_RENEW_INTERVAL } else { heartbeat };
        let busy: Arc<Vec<AtomicBool>> =
            Arc::new((0..size).map(|_| AtomicBool::new(false)).collect());
        let mut workers = Vec::with_capacity(size);
        let mut handles = Vec::with_capacity(size);
        for i in 0..size {
            let (actor_ref, handle) = WorkerActor::spawn(
                Some(format!("worker-{i}")),
                WorkerActor,
                WorkerInit {
                    executor: Arc::clone(&executor),
                    metrics: Arc::clone(&metrics),
                    lease_keeper: lease_keeper.clone(),
                    heartbeat,
                    slot: i,
                    busy: Arc::clone(&busy),
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("spawn worker-{i}: {e}"))?;
            workers.push(actor_ref);
            handles.push(handle);
        }
        Ok(Self { workers, _handles: handles, next: AtomicUsize::new(0), busy })
    }

    /// Idle-first dispatch (LOW_LATENCY B-5): send the task to a worker whose
    /// mailbox is verifiably empty, so a short task can never queue behind a
    /// long-running one — the head-of-line blocking blind round-robin had.
    ///
    /// The scan-then-set is race-free for its one real caller, the reconcile
    /// loop (a single task): only `dispatch` sets flags, and each worker only
    /// clears its own. Under the loop's capacity accounting an idle worker
    /// always exists at dispatch time; the round-robin rotor remains as the
    /// fallback for the small window where a just-finished worker's clear is
    /// not yet visible — degrading to the old behaviour, never dropping work.
    pub fn dispatch(&self, payload: DispatchPayload) -> Result<()> {
        let idx = (0..self.workers.len())
            .find(|&i| !self.busy[i].load(Ordering::Acquire))
            .unwrap_or_else(|| self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len());
        self.busy[idx].store(true, Ordering::Release);
        if self.workers[idx].cast(WorkerMsg::Execute(payload)).is_err() {
            // The actor never received the task, so its SlotFree guard will
            // never run — clear the flag here or the slot is dead to the
            // idle-first scan forever.
            self.busy[idx].store(false, Ordering::Release);
            anyhow::bail!("worker-{idx} mailbox closed");
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.workers.len()
    }
}
