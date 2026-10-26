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

use crate::checkout::{CheckoutTokens, Credential, TokenRequest};
use crate::executor::{
    ClaimLost, ExecContext, Executor, LogChunk, LogSink, TeardownUnseen, MAX_STOP_GRACE_SECS,
};
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

/// The longest a worker waits for an executor to stop and remove an attempt
/// whose claim is gone: the longest grace a task may name, and two minutes for
/// the backend's own calls. Each backend bounds its teardown inside this (the
/// Kubernetes one, at the longest grace, by a minute); it is here for an
/// executor that does not hear the stop at all.
///
/// Past it the worker stops waiting, and says so: nothing saw the work stop.
/// What keeps a later claim from running beside such a leftover is that
/// claim's own dispatch: it stops and removes what a lower fence left, and
/// starts nothing until it has seen it gone.
const TEARDOWN_CAP: Duration = Duration::from_secs(MAX_STOP_GRACE_SECS + 120);

/// [`TEARDOWN_CAP`], or what a test set in its place: no test can wait twelve
/// minutes for an executor that never stops.
fn teardown_cap() -> Duration {
    #[cfg(test)]
    {
        let ms = tests::TEARDOWN_CAP_MS.load(Ordering::Relaxed);
        if ms > 0 {
            return Duration::from_millis(ms);
        }
    }
    TEARDOWN_CAP
}

// ── Result type (produced by workers, consumed by the reconcile loop) ─────────

/// What a result says of work that was to stop: its claim was lost, it was
/// asked to stop, or the dispatch was a teardown alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Stopped {
    /// Nothing: the result is the task's own.
    #[default]
    No,
    /// The work is gone: stopped and removed, and seen so, or never started.
    Gone,
    /// Nobody saw it go: it may still be running
    /// ([`crate::executor::TeardownUnseen`]).
    Unseen,
}

pub struct TaskResult {
    pub task_id: String,
    pub worker_id: String,
    pub success: bool,
    /// Whether this failure was a `timeout_secs` deadline kill (the executor
    /// error downcast to [`crate::executor::TimeoutError`]) rather than a
    /// non-zero exit or a backend error. Only meaningful when `!success`; drives
    /// retry-on-timeout gating (#24).
    pub timed_out: bool,
    /// Whether the executor refused to start the task for lack of capacity (the
    /// error downcast to [`crate::executor::Backpressure`]): nothing ran and no
    /// workload was created. Only meaningful when `!success`. The reconcile
    /// loop decides what follows: it returns such a task to the queue without
    /// consuming its attempt, or, where the task does not wait, fails the
    /// attempt with this refusal as its output.
    pub backpressure: bool,
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
    /// Whether the work stopped because it was told to, and whether that was
    /// seen. A task that was asked to stop is `cancelled` on `Gone` and left
    /// as it is, to be looked for again, on `Unseen`. `No` for a result the
    /// task reached by itself.
    pub stopped: Stopped,
    /// Echoed from [`DispatchPayload::teardown`].
    pub teardown: bool,
    /// Whether the task was one of a run that is not trusted (its context's
    /// [`crate::executor::Confinement`]). What such a task printed is not
    /// read for how often it is tried again.
    pub confined: bool,
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
    /// A teardown alone ([`Executor::teardown`]): nothing is run. For a task
    /// that was asked to stop and whose work nobody is watching. Its result
    /// is [`Stopped::Gone`] or [`Stopped::Unseen`], never the task's own.
    pub teardown: bool,
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
    checkout_tokens: Option<Arc<dyn CheckoutTokens>>,
}

struct WorkerState {
    executor: Arc<dyn Executor>,
    metrics: Arc<Metrics>,
    lease_keeper: Option<Arc<dyn LeaseKeeper>>,
    heartbeat: Duration,
    slot: usize,
    busy: Arc<Vec<AtomicBool>>,
    /// The operator's token service, which mints each attempt's checkout
    /// credential. `None`: every checkout is fetched without one.
    checkout_tokens: Option<Arc<dyn CheckoutTokens>>,
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
            checkout_tokens: init.checkout_tokens,
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
        let checkout_tokens = state.checkout_tokens.clone();
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

            // The workspace's checkout credential, minted for this attempt
            // alone: asked for here, where a slow token service holds up this
            // task and no other, and revoked below once the attempt is over.
            // A failure to mint fails the attempt before anything runs.
            let minted = match &checkout_tokens {
                Some(tokens) if !p.teardown => {
                    mint_checkout_credential(tokens.as_ref(), &p.ctx).await
                }
                _ => Ok(None),
            };
            let minted = match minted {
                Ok(credential) => {
                    p.ctx.checkout_credential = credential;
                    Ok(())
                }
                Err(e) => Err(e),
            };
            // A patched checkout's patch: a link for this attempt, from the
            // same service. Without one the attempt fails here: a patched
            // checkout never runs its commit alone.
            let minted = match minted {
                Ok(()) if !p.teardown => match checkout_patch(checkout_tokens.as_deref(), &p.ctx).await {
                    Ok(link) => {
                        p.ctx.checkout_patch = link;
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                other => other,
            };
            // The forge token the task asks for, likewise: of the same
            // service, for the same repository, and given to the task as
            // `GITHUB_TOKEN`. As a secret's value, so that every backend
            // treats it as one: masked in the log, and on Kubernetes in the
            // task's Secret rather than its pod spec. A failure to mint fails
            // the attempt as the checkout's does.
            let mut forge_token = None;
            let minted = match (minted, &checkout_tokens) {
                (Ok(()), Some(tokens)) if !p.teardown && p.ctx.forge_token.is_some() => {
                    match mint_forge_token(tokens.as_ref(), &p.ctx).await {
                        Ok(token) => {
                            if let Some(token) = &token {
                                p.ctx.env.push(dagron_core::dag::EnvVar {
                                    name: FORGE_TOKEN_ENV.to_string(),
                                    value: token.password.clone(),
                                    value_from: Some(dagron_core::dag::SecretRef {
                                        secret: FORGE_TOKEN_SECRET.to_string(),
                                        optional: false,
                                    }),
                                });
                            }
                            forge_token = token;
                            Ok(())
                        }
                        Err(e) => Err(e.context("the task's forge token")),
                    }
                }
                (minted, _) => minted,
            };
            // The credentials of the workspace's actions that ask for one,
            // likewise: each for its own repository, for this attempt.
            let minted = match (minted, &checkout_tokens) {
                (Ok(()), Some(tokens)) if !p.teardown => {
                    match mint_action_credentials(tokens.as_ref(), &p.ctx).await {
                        Ok(given) => {
                            p.ctx.action_credentials = given;
                            Ok(())
                        }
                        Err((given, e)) => {
                            p.ctx.action_credentials = given;
                            Err(e.context("an action's credential"))
                        }
                    }
                }
                (minted, _) => minted,
            };

            // Secret masking (#8): mask any sensitive task-env values out of the
            // captured output *before* it is persisted or surfaced. Built from
            // this task's env, so it covers every executor backend centrally;
            // and from the checkout's credential, which the task never holds
            // but a fetch's error could repeat.
            let redactor = crate::redact::Redactor::from_task_env(&p.ctx.env).with_values(
                p.ctx
                    .checkout_credential
                    .iter()
                    .chain(p.ctx.action_credentials.iter().map(|(_, c)| c))
                    .flat_map(|c| [c.password.clone(), c.authorization()]),
            );

            // Live-log streaming (#17): wire a per-attempt sink so the executor can
            // stream incremental (redacted) output back to the loop for tailing.
            if let Some(log_tx) = p.log_tx.take() {
                p.ctx.log_sink = Some(LogSink::new(
                    log_tx,
                    p.task_id.clone(),
                    p.fence,
                    redactor.clone().with_lines(),
                ));
            }

            // Task wall time (claim→finish) — the workload signal the no-op load
            // test never exercised. Recorded whether the task succeeds or errors.
            let started = Instant::now();

            // Heartbeat while the task runs: renew the claim's lease on an
            // interval so a task may outlive the claim-time lease window
            // (long training jobs, stream consumers). A transient renew error
            // is only logged: the task is healthy, and if the lease lapses
            // anyway the fencing token voids our result.
            //
            // Losing the claim ends the execution: the run was cancelled or
            // ran past its deadline, a gang sibling's cancel took the task
            // along, or another scheduler holds it now. Ceasing to wait is
            // not that. Dropping the future ends a local child and leaves a
            // container or a pod running with nobody waiting on it, so the
            // executor is told ([`Stop`]) and given the time to stop and
            // remove what this attempt made. This slot is busy until it has:
            // the work is not over because its row says so.
            let result = match (&minted, &lease_keeper) {
                (Err(e), _) => Err(anyhow::anyhow!("the workspace's checkout credential: {e:#}")),
                (Ok(()), None) => run(executor.as_ref(), &p.ctx, p.teardown).await,
                (Ok(()), Some(keeper)) => {
                    let stop = p.ctx.stop.clone();
                    let exec_fut = run(executor.as_ref(), &p.ctx, p.teardown);
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
                                        stop.request();
                                        let stopping = Instant::now();
                                        let cap = teardown_cap();
                                        match tokio::time::timeout(cap, &mut exec_fut).await {
                                            // The executor stopped, and could not see the
                                            // work gone: a stop or a removal that failed or
                                            // was not answered. Its word, passed on as it
                                            // is; the executor has logged the reason.
                                            Ok(Err(e)) if e.is::<TeardownUnseen>() => {
                                                tracing::warn!(
                                                    took_ms = stopping.elapsed().as_millis(),
                                                    "the task's claim was lost and its work was not \
                                                     seen to stop; what it started may still be running"
                                                );
                                                break Err(e);
                                            }
                                            // Stopped and removed. Or over by itself before
                                            // it heard, with what any attempt that ends leaves.
                                            Ok(_) => {
                                                tracing::info!(
                                                    took_ms = stopping.elapsed().as_millis(),
                                                    "the task's claim was lost; its work was stopped"
                                                );
                                                break Err(anyhow::Error::new(ClaimLost));
                                            }
                                            // An executor that does not hear the stop: its
                                            // execution is dropped, as it was before there
                                            // was one to hear. Said as it is: nothing saw
                                            // the work stop, and this attempt's result must
                                            // not say it did.
                                            Err(_) => {
                                                tracing::warn!(
                                                    cap_secs = cap.as_secs(),
                                                    "the task's claim was lost and its executor did not \
                                                     stop in time; no longer waiting on it, and what it \
                                                     started may still be running"
                                                );
                                                break Err(ClaimLost::unseen(format!(
                                                    "its executor did not stop within {} s",
                                                    cap.as_secs()
                                                )));
                                            }
                                        }
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
            if let (Some(tokens), Some(credential)) =
                (&checkout_tokens, p.ctx.checkout_credential.take())
            {
                revoke_checkout_credential(Arc::clone(tokens), credential);
            }
            if let (Some(tokens), Some(token)) = (&checkout_tokens, forge_token.take()) {
                revoke_checkout_credential(Arc::clone(tokens), token);
            }
            if let Some(tokens) = &checkout_tokens {
                for (_, credential) in std::mem::take(&mut p.ctx.action_credentials) {
                    revoke_checkout_credential(Arc::clone(tokens), credential);
                }
            }
            // A dispatch the executor refused to start ran nothing, so it has no
            // task duration to record and is not an error to log as one.
            let backpressure =
                matches!(&result, Err(e) if e.is::<crate::executor::Backpressure>());
            let elapsed = started.elapsed();
            // Nor does a teardown: no task ran in it.
            if !backpressure && !p.teardown {
                metrics.observe_task_duration(elapsed.as_secs_f64());
            }
            let duration_ms = elapsed.as_millis();
            // Work that stopped because it was told to, and whether anyone
            // saw it go: the reconcile loop settles a task that was asked to
            // stop on this, not on what else the result says.
            let stopped = match &result {
                Err(e) if e.is::<TeardownUnseen>() => Stopped::Unseen,
                Err(e) if e.is::<ClaimLost>() => Stopped::Gone,
                _ => Stopped::No,
            };
            // The attempt is over. A log past its cap that no executor
            // sealed says its end was not kept, rather than end on the line
            // that says its end is to come: one cancelled among them, whose
            // row is still the attempt's until it is settled. Where the claim
            // went to another, the row's fence refuses the line, as it does
            // any late chunk. Not where nothing ran, nor a teardown that was
            // not seen through. And a log that reached its cap is counted,
            // with what it left out.
            if let Some(sink) = &p.ctx.log_sink {
                if !backpressure && stopped != Stopped::Unseen {
                    sink.close();
                }
                if let Some((bytes, lines)) = sink.omitted() {
                    metrics.add_log_capped(bytes);
                    tracing::info!(
                        omitted_bytes = bytes,
                        omitted_lines = lines,
                        tail_bytes = sink.tail(),
                        "this attempt's log passed its cap; its middle is left out"
                    );
                }
            }

            let (success, timed_out, output, dropped) = match result {
                Ok(o) => {
                    if o.success {
                        tracing::debug!(duration_ms, "task finished ok");
                    } else {
                        tracing::error!(duration_ms, "non-zero exit");
                    }
                    // A clean Ok is never a timeout — every backend aborts a
                    // deadline via an Err carrying TimeoutError (handled below).
                    let (output, dropped) =
                        crate::executor::masked_end(&redactor, &o.output, o.dropped);
                    (o.success, false, Some(output), dropped)
                }
                Err(e) => {
                    // Distinguish a `timeout_secs` deadline kill from any other
                    // failure so the retry branch can honor `retry_on_timeout` (#24).
                    let timed_out = e.is::<crate::executor::TimeoutError>();
                    // The error string (e.g. a failed connection URL) can carry a
                    // secret too — mask it before it is logged or stored.
                    let msg = redactor.redact(&e.to_string()).into_owned();
                    // A claim that was lost is no failure of the task's, and
                    // its row is another's to write now: said once above, and
                    // kept out of the task's log.
                    let claim_lost = e.is::<ClaimLost>();
                    if backpressure {
                        tracing::debug!(duration_ms, reason = %msg, "executor back-pressure — nothing was started");
                    } else if !claim_lost {
                        tracing::error!(duration_ms, timed_out, err = %msg, "executor error");
                    }
                    // The reason becomes the task's output; put it in the live
                    // log as well, which the log views read first.
                    // As a note: a log that reached its cap still ends on
                    // why the attempt failed.
                    if let (Some(sink), false) = (&p.ctx.log_sink, claim_lost) {
                        sink.note(&format!("{msg}
"));
                    }
                    (false, timed_out, Some(msg), 0)
                }
            };

            // The output is a row of the scheduling database, like the log:
            // with no NUL in it, for which Postgres would refuse the result
            // and the engine stop, and kept up to the same cap, its end where
            // it is over.
            let output = output.map(|o| {
                crate::executor::cap_output_after(
                    crate::executor::without_nul(o),
                    crate::executor::log_task_cap(),
                    dropped,
                )
            });

            // Fire-and-forget — the reconcile loop drains this channel each tick.
            if p.result_tx.send(TaskResult {
                task_id: p.task_id.clone(),
                worker_id: p.worker_id,
                success,
                timed_out,
                backpressure,
                output,
                attempt: p.attempt,
                max_attempts: p.max_attempts,
                retry_delay_secs: p.retry_delay_secs,
                retry_max_delay_secs: p.retry_max_delay_secs,
                retry_on_timeout: p.retry_on_timeout,
                fence: p.fence,
                finished: Instant::now(),
                spec: p.spec,
                stopped,
                teardown: p.teardown,
                confined: p.ctx.confinement.is_some(),
            }).is_err() {
                tracing::warn!("result channel closed — reconcile loop may have exited");
            }
        }
        .instrument(span)
        .await;

        Ok(())
    }
}

/// One dispatch: the task's execution, or a teardown alone.
///
/// A teardown has no output of its own. It is said in the words a stopped
/// execution uses, so that what follows reads one way: [`ClaimLost`] once
/// nothing of the task is left, and [`ClaimLost::unseen`], with the backend's
/// reason, while something is or cannot be seen.
async fn run(
    executor: &dyn Executor,
    ctx: &ExecContext,
    teardown: bool,
) -> Result<crate::executor::ExecOutput> {
    if !teardown {
        return executor.execute(ctx).await;
    }
    match executor.teardown(ctx).await {
        Ok(()) => Err(anyhow::Error::new(ClaimLost)),
        Err(e) => Err(ClaimLost::unseen(format!("{e:#}"))),
    }
}

/// The credential for `ctx`'s checkout, from the token service: none when
/// its workspace has no checkout, when the repository may not be sent one
/// (plain http to another host), or when the task has no row behind it.
async fn mint_checkout_credential(
    tokens: &dyn CheckoutTokens,
    ctx: &ExecContext,
) -> Result<Option<Credential>> {
    let (Some(checkout), Some(id)) = (crate::checkout::of(ctx)?, ctx.identity.as_ref()) else {
        return Ok(None);
    };
    if !crate::checkout::may_carry_credential(&checkout.repository) {
        return Ok(None);
    }
    tokens
        .mint(&TokenRequest {
            repository: checkout.repository.clone(),
            commit: checkout.commit.clone(),
            run_id: id.run_id.clone(),
            task_id: id.task_id.clone(),
            attempt: id.attempt,
            permissions: None,
            action: false,
        })
        .await
}

/// The link to the patch `ctx`'s checkout lays over its commit, where it is
/// patched: from the token service, for this attempt. An error where there
/// is no service to ask, or it gives none.
async fn checkout_patch(
    tokens: Option<&dyn CheckoutTokens>,
    ctx: &ExecContext,
) -> Result<Option<String>> {
    let (Some(checkout), Some(id)) = (crate::checkout::of(ctx)?, ctx.identity.as_ref()) else {
        return Ok(None);
    };
    if !checkout.patched {
        return Ok(None);
    }
    let Some(tokens) = tokens else {
        anyhow::bail!(
            "this checkout is of a patch, and this engine has no token service to ask for it \
             (DAGRON_CHECKOUT_TOKEN_URL)"
        );
    };
    let link = tokens
        .patch(&TokenRequest {
            repository: checkout.repository.clone(),
            commit: checkout.commit.clone(),
            run_id: id.run_id.clone(),
            task_id: id.task_id.clone(),
            attempt: id.attempt,
            permissions: None,
            action: false,
        })
        .await?;
    Ok(Some(link))
}

/// The credentials of `ctx`'s actions that ask for one, from the token
/// service, each repository's once: none for one that may not be sent one,
/// or that the service gives none. On an error, those minted before it, to
/// be revoked, with the error.
async fn mint_action_credentials(
    tokens: &dyn CheckoutTokens,
    ctx: &ExecContext,
) -> std::result::Result<Vec<(String, Credential)>, (Vec<(String, Credential)>, anyhow::Error)> {
    let mut given: Vec<(String, Credential)> = Vec::new();
    let (Some(ws), Some(id)) = (ctx.workspace.as_ref(), ctx.identity.as_ref()) else {
        return Ok(given);
    };
    let mut asked: Vec<&str> = Vec::new();
    for action in ws.actions.iter().filter(|a| a.credential) {
        if asked.contains(&action.repository.as_str())
            || !crate::checkout::may_carry_credential(&action.repository)
        {
            continue;
        }
        asked.push(&action.repository);
        let minted = tokens
            .mint(&TokenRequest {
                repository: action.repository.clone(),
                commit: action.git_ref.clone(),
                run_id: id.run_id.clone(),
                task_id: id.task_id.clone(),
                attempt: id.attempt,
                permissions: None,
                action: true,
            })
            .await;
        match minted {
            Ok(Some(credential)) => given.push((action.repository.clone(), credential)),
            Ok(None) => {}
            Err(e) => return Err((given, e.context(action.repository.clone()))),
        }
    }
    Ok(given)
}

/// The variable a task holds its forge token in, as GitHub's runner names it.
pub const FORGE_TOKEN_ENV: &str = "GITHUB_TOKEN";
/// The name the forge token is a secret by, which no operator's secret has.
const FORGE_TOKEN_SECRET: &str = "dagron-forge-token";

/// The forge token `ctx` asks for, from the token service: for the
/// repository and commit of its checkout, with the permissions it names.
/// None where it has no checkout, where the repository may not be sent a
/// credential, or where the service gives none.
async fn mint_forge_token(
    tokens: &dyn CheckoutTokens,
    ctx: &ExecContext,
) -> Result<Option<Credential>> {
    let (Some(spec), Some(checkout), Some(id)) = (
        ctx.forge_token.as_ref(),
        crate::checkout::of(ctx)?,
        ctx.identity.as_ref(),
    ) else {
        return Ok(None);
    };
    if !crate::checkout::may_carry_credential(&checkout.repository) {
        return Ok(None);
    }
    tokens
        .mint(&TokenRequest {
            repository: checkout.repository.clone(),
            commit: checkout.commit.clone(),
            run_id: id.run_id.clone(),
            task_id: id.task_id.clone(),
            attempt: id.attempt,
            permissions: Some(spec.permissions.clone()),
            action: false,
        })
        .await
}

/// Revoke a used credential in the background: the attempt's result does
/// not wait on it, and a failure leaves a credential that expires on its own.
fn revoke_checkout_credential(tokens: Arc<dyn CheckoutTokens>, credential: Credential) {
    tokio::spawn(async move {
        if let Err(e) = tokens.revoke(&credential).await {
            tracing::warn!(error = %e, "could not revoke the checkout's credential; it expires on its own");
        }
    });
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
        Self::with_checkout_tokens(size, executor, metrics, lease_keeper, heartbeat, None).await
    }

    /// [`WorkerPool::with_lease_keeper`] with the operator's token service:
    /// each attempt of a task whose workspace has a checkout is given a
    /// credential minted for it, revoked when the attempt ends (see
    /// [`crate::checkout`]).
    pub async fn with_checkout_tokens(
        size: usize,
        executor: Arc<dyn Executor>,
        metrics: Arc<Metrics>,
        lease_keeper: Option<Arc<dyn LeaseKeeper>>,
        heartbeat: Duration,
        checkout_tokens: Option<Arc<dyn CheckoutTokens>>,
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
                    checkout_tokens: checkout_tokens.clone(),
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

    /// Stop every worker, and wait for each to be gone. Worker actors are
    /// registered by name, process-wide: a pool that is stopped frees its
    /// names for the next. A worker that is running a task stops once it is
    /// over.
    pub async fn stop(mut self) {
        for worker in &self.workers {
            worker.stop(None);
        }
        for handle in std::mem::take(&mut self._handles) {
            let _ = handle.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{ExecOutput, TaskIdentity};
    use std::sync::Mutex;

    /// Records the credential each attempt was given, and says it back, as a
    /// task that printed it would.
    #[derive(Default)]
    struct Saying {
        given: Mutex<Vec<Option<Credential>>>,
    }

    #[async_trait]
    impl Executor for Saying {
        async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
            self.given
                .lock()
                .unwrap()
                .push(ctx.checkout_credential.clone());
            let mut said = ctx
                .checkout_credential
                .as_ref()
                .map(|c| format!("password {} header {}", c.password, c.authorization()))
                .unwrap_or_default();
            if let Some(token) = ctx.env.iter().find(|e| e.name == FORGE_TOKEN_ENV) {
                said.push_str(&format!(" token {}", token.value));
            }
            for (repository, credential) in &ctx.action_credentials {
                said.push_str(&format!(" {repository} {}", credential.password));
            }
            Ok(ExecOutput {
                success: true,
                output: said,
                dropped: 0,
            })
        }
    }

    #[derive(Default)]
    struct Tokens {
        minted: Mutex<Vec<TokenRequest>>,
        revoked: Mutex<Vec<Credential>>,
        refuse: bool,
    }

    #[async_trait]
    impl CheckoutTokens for Tokens {
        async fn mint(&self, request: &TokenRequest) -> Result<Option<Credential>> {
            self.minted.lock().unwrap().push(request.clone());
            if self.refuse {
                anyhow::bail!("the token service answered 403: not this repository");
            }
            let kind = if request.permissions.is_some() {
                "job"
            } else {
                "attempt"
            };
            Ok(Some(Credential {
                username: "x-access-token".into(),
                password: format!("ghs_{kind}_{}", request.attempt),
            }))
        }

        async fn revoke(&self, credential: &Credential) -> Result<()> {
            self.revoked.lock().unwrap().push(credential.clone());
            Ok(())
        }
    }

    fn checkout_task(repository: &str) -> ExecContext {
        let mut ctx = ExecContext::new(vec!["make".into()], Some(10), None);
        ctx.workspace = Some(dagron_core::workspace::WorkspaceSpec {
            checkout: Some(dagron_core::workspace::CheckoutSpec {
                repository: repository.into(),
                commit: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
                depth: None,
                patched: false,
            }),
            ..Default::default()
        });
        ctx.identity = Some(TaskIdentity {
            task_id: "t-1".into(),
            run_id: "r-1".into(),
            attempt: 3,
            fence: 3,
            installation: None,
        });
        ctx
    }

    /// Worker actors are registered by name, process-wide: one pool at a time.
    static ONE_POOL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// One attempt of `ctx` through a pool of one, with `tokens`; the pool is
    /// stopped after it, so that the next can take its workers' names.
    async fn attempt(executor: Arc<Saying>, tokens: Arc<Tokens>, ctx: ExecContext) -> TaskResult {
        let _one = ONE_POOL.lock().await;
        let mut pool = WorkerPool::with_checkout_tokens(
            1,
            executor,
            Arc::new(Metrics::new()),
            None,
            Duration::from_secs(10),
            Some(tokens),
        )
        .await
        .unwrap();
        let (result_tx, mut results) = tokio::sync::mpsc::unbounded_channel();
        pool.dispatch(DispatchPayload {
            task_id: "t-1".into(),
            worker_id: "w-1".into(),
            ctx,
            attempt: 2,
            max_attempts: 3,
            retry_delay_secs: 0,
            retry_max_delay_secs: None,
            retry_on_timeout: true,
            fence: 1,
            spec: None,
            teardown: false,
            result_tx,
            log_tx: None,
        })
        .unwrap();
        let result = results.recv().await.expect("a result");
        for worker in &pool.workers {
            worker.stop(None);
        }
        for handle in std::mem::take(&mut pool._handles) {
            let _ = handle.await;
        }
        result
    }

    #[tokio::test]
    async fn an_attempt_s_checkout_credential_is_minted_for_it_masked_and_revoked_after_it() {
        let (executor, tokens) = (Arc::new(Saying::default()), Arc::new(Tokens::default()));
        let result = attempt(
            executor.clone(),
            tokens.clone(),
            checkout_task("https://github.com/acme/widgets.git"),
        )
        .await;
        assert!(result.success);

        let minted = tokens.minted.lock().unwrap().clone();
        assert_eq!(
            minted,
            vec![TokenRequest {
                repository: "https://github.com/acme/widgets.git".into(),
                commit: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
                run_id: "r-1".into(),
                task_id: "t-1".into(),
                attempt: 3,
                permissions: None,
                action: false,
            }]
        );
        let credential = Credential {
            username: "x-access-token".into(),
            password: "ghs_attempt_3".into(),
        };
        assert_eq!(
            executor.given.lock().unwrap().clone(),
            vec![Some(credential.clone())]
        );
        // Whatever the output repeats of it, plain or as the header, is masked.
        let output = result.output.unwrap();
        assert_eq!(output, "password *** header ***", "{output}");
        // And it is revoked once the attempt is over.
        for _ in 0..100 {
            if !tokens.revoked.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(tokens.revoked.lock().unwrap().clone(), vec![credential]);
    }

    /// The actions that ask for a credential are each given one for their
    /// repository, asked for as an action's at the ref it is fetched at,
    /// once for a repository two of them are of; masked, and revoked once
    /// the attempt is over. One that asks for none is asked none for.
    #[tokio::test]
    async fn an_attempt_s_actions_are_minted_credentials_masked_and_revoked() {
        let (executor, tokens) = (Arc::new(Saying::default()), Arc::new(Tokens::default()));
        let mut ctx = checkout_task("https://github.com/acme/widgets.git");
        let action = |repository: &str, git_ref: &str, credential: bool| {
            dagron_core::workspace::ActionSpec {
                repository: repository.into(),
                git_ref: git_ref.into(),
                path: format!("p/{git_ref}"),
                credential,
            }
        };
        ctx.workspace.as_mut().unwrap().actions = vec![
            action("https://github.com/acme/tool.git", "v1", true),
            action("https://github.com/acme/tool.git", "v2", true),
            action("https://github.com/octo/hello.git", "v1", false),
        ];
        let result = attempt(executor.clone(), tokens.clone(), ctx).await;
        assert!(result.success);
        let minted = tokens.minted.lock().unwrap().clone();
        assert_eq!(minted.len(), 2, "{minted:?}");
        assert_eq!(
            minted[1],
            TokenRequest {
                repository: "https://github.com/acme/tool.git".into(),
                commit: "v1".into(),
                run_id: "r-1".into(),
                task_id: "t-1".into(),
                attempt: 3,
                permissions: None,
                action: true,
            }
        );
        let output = result.output.unwrap();
        assert_eq!(
            output, "password *** header *** https://github.com/acme/tool.git ***",
            "{output}"
        );
        for _ in 0..100 {
            if tokens.revoked.lock().unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            tokens.revoked.lock().unwrap().len(),
            2,
            "the checkout's and the action's"
        );
    }

    /// A task that asks for a forge token is given one, for its checkout's
    /// repository with the permissions it names, as `GITHUB_TOKEN` and as a
    /// secret's value; it is masked in the log, and revoked with the
    /// checkout's once the attempt is over.
    #[tokio::test]
    async fn an_attempt_s_forge_token_is_given_to_the_task_masked_and_revoked() {
        let (executor, tokens) = (Arc::new(Saying::default()), Arc::new(Tokens::default()));
        let mut ctx = checkout_task("https://github.com/acme/widgets.git");
        let permissions = std::collections::BTreeMap::from([
            ("contents".to_string(), "read".to_string()),
            ("pull_requests".to_string(), "write".to_string()),
        ]);
        ctx.forge_token = Some(dagron_core::forge_token::ForgeTokenSpec {
            permissions: permissions.clone(),
        });
        let result = attempt(executor.clone(), tokens.clone(), ctx).await;
        assert!(result.success);
        let minted = tokens.minted.lock().unwrap().clone();
        assert_eq!(minted.len(), 2, "the checkout's, then the task's");
        assert_eq!(minted[0].permissions, None);
        assert_eq!(minted[1].permissions, Some(permissions));
        assert_eq!(minted[1].repository, "https://github.com/acme/widgets.git");
        assert_eq!(
            result.output.as_deref(),
            Some("password *** header *** token ***")
        );
        for _ in 0..100 {
            if tokens.revoked.lock().unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut revoked: Vec<String> = tokens
            .revoked
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.password.clone())
            .collect();
        revoked.sort();
        assert_eq!(revoked, ["ghs_attempt_3", "ghs_job_3"]);
    }

    #[tokio::test]
    async fn a_credential_that_cannot_be_minted_fails_the_attempt_before_it_runs() {
        let executor = Arc::new(Saying::default());
        let tokens = Arc::new(Tokens {
            refuse: true,
            ..Tokens::default()
        });
        let result = attempt(
            executor.clone(),
            tokens.clone(),
            checkout_task("https://github.com/acme/widgets.git"),
        )
        .await;
        assert!(!result.success);
        assert!(!result.timed_out);
        assert_eq!(
            result.output.as_deref(),
            Some("the workspace's checkout credential: the token service answered 403: not this repository")
        );
        assert!(
            executor.given.lock().unwrap().is_empty(),
            "the task never ran"
        );
        assert!(tokens.revoked.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn no_credential_is_asked_for_where_none_may_be_sent_or_none_is_needed() {
        for ctx in [
            // Plain http to another host: a credential would cross a network
            // in the clear.
            checkout_task("http://git.example.com/acme/widgets.git"),
            // No checkout at all.
            ExecContext::new(vec!["make".into()], Some(10), None),
        ] {
            let (executor, tokens) = (Arc::new(Saying::default()), Arc::new(Tokens::default()));
            let result = attempt(executor.clone(), tokens.clone(), ctx).await;
            assert!(result.success);
            assert!(tokens.minted.lock().unwrap().is_empty());
            assert_eq!(executor.given.lock().unwrap().clone(), vec![None]);
        }
    }

    /// What a test sets [`teardown_cap`] to, in milliseconds; 0 for the real one.
    pub(super) static TEARDOWN_CAP_MS: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    /// A task that runs until it is told to stop, on a backend where stopping
    /// takes `teardown`: a container's grace. Or one that never hears; or one
    /// that hears and cannot see its work gone (`unseen`).
    struct Running {
        hears: bool,
        unseen: bool,
        teardown: Duration,
        stopped: AtomicBool,
        dropped: Arc<AtomicBool>,
    }

    /// Says when the execution it sits in was dropped rather than finished.
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Executor for Running {
        async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
            if !self.hears {
                let _dropped = Dropped(self.dropped.clone());
                std::future::pending::<()>().await;
            }
            ctx.stop.requested().await;
            tokio::time::sleep(self.teardown).await;
            if self.unseen {
                return Err(ClaimLost::unseen("the daemon would not say"));
            }
            self.stopped.store(true, Ordering::SeqCst);
            Err(anyhow::Error::new(ClaimLost))
        }
    }

    /// A lease that is renewed until the `gone_at`th renewal, and gone then.
    struct GoneAt {
        gone_at: usize,
        renewals: AtomicUsize,
    }

    #[async_trait]
    impl LeaseKeeper for GoneAt {
        async fn renew(&self, _task_id: &str, _worker_id: &str, _fence: i64) -> Result<bool> {
            Ok(self.renewals.fetch_add(1, Ordering::SeqCst) + 1 < self.gone_at)
        }
    }

    /// One dispatch through a pool of one whose lease is gone at its second
    /// renewal: the result, and how long it took to come.
    async fn lose_the_claim(executor: Arc<Running>, cap: Duration) -> (TaskResult, Duration) {
        let _one = ONE_POOL.lock().await;
        TEARDOWN_CAP_MS.store(cap.as_millis() as u64, Ordering::Relaxed);
        let keeper = Arc::new(GoneAt { gone_at: 2, renewals: AtomicUsize::new(0) });
        let mut pool = WorkerPool::with_lease_keeper(
            1,
            executor,
            Arc::new(Metrics::new()),
            Some(keeper),
            Duration::from_millis(20),
        )
        .await
        .unwrap();
        let (result_tx, mut results) = tokio::sync::mpsc::unbounded_channel();
        let started = Instant::now();
        pool.dispatch(DispatchPayload {
            task_id: "t-1".into(),
            worker_id: "w-1".into(),
            ctx: ExecContext::new(vec!["sleep".into(), "600".into()], Some(600), None),
            attempt: 0,
            max_attempts: 1,
            retry_delay_secs: 0,
            retry_max_delay_secs: None,
            retry_on_timeout: true,
            fence: 1,
            spec: None,
            teardown: false,
            result_tx,
            log_tx: None,
        })
        .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(30), results.recv())
            .await
            .expect("a result within 30 s")
            .expect("a result");
        let took = started.elapsed();
        TEARDOWN_CAP_MS.store(0, Ordering::Relaxed);
        for worker in &pool.workers {
            worker.stop(None);
        }
        for handle in std::mem::take(&mut pool._handles) {
            let _ = handle.await;
        }
        (result, took)
    }

    /// A cancel takes the claim away, and the worker learns it at its next
    /// renewal. It used to stop waiting there and leave the container running.
    /// Now the executor is told, and the worker reports only once the executor
    /// has stopped the work: until then the slot is not free and nothing has
    /// said the task is over.
    #[tokio::test]
    async fn a_lost_claim_stops_the_work_before_the_worker_says_anything() {
        let executor = Arc::new(Running {
            hears: true,
            unseen: false,
            teardown: Duration::from_millis(300),
            stopped: AtomicBool::new(false),
            dropped: Arc::default(),
        });
        let (result, took) = lose_the_claim(executor.clone(), Duration::from_secs(20)).await;
        assert!(
            executor.stopped.load(Ordering::SeqCst),
            "the result came after the work was stopped"
        );
        assert!(took >= Duration::from_millis(300), "it waited out the teardown: {took:?}");
        assert!(!result.success && !result.timed_out && !result.backpressure);
        assert_eq!(result.output.as_deref(), Some(ClaimLost.to_string().as_str()));
        assert_eq!(result.stopped, Stopped::Gone, "and the loop is told the work was seen gone");
        assert_eq!(result.fence, 1, "under the claim that is gone, so it is applied to nothing");
    }

    /// An executor that never hears the stop is waited on up to the cap and
    /// then dropped, which is all that was ever done before: a slot is not
    /// lost for good to a backend that will not stop.
    #[tokio::test]
    async fn an_executor_that_does_not_stop_is_dropped_once_the_cap_is_out() {
        let dropped: Arc<AtomicBool> = Arc::default();
        let executor = Arc::new(Running {
            hears: false,
            unseen: false,
            teardown: Duration::ZERO,
            stopped: AtomicBool::new(false),
            dropped: dropped.clone(),
        });
        let (result, took) = lose_the_claim(executor.clone(), Duration::from_millis(200)).await;
        assert!(took >= Duration::from_millis(200), "the cap was waited out: {took:?}");
        assert!(dropped.load(Ordering::SeqCst), "and the execution dropped");
        assert!(!executor.stopped.load(Ordering::SeqCst));
        assert!(!result.success);
        // Nothing saw the work stop, and the result does not say it did.
        let said = result.output.unwrap_or_default();
        assert!(said.contains("may still be running"), "{said}");
        assert_ne!(said, ClaimLost.to_string());
    }

    /// An executor that heard the stop and could not see its work gone, a
    /// removal the daemon refused or did not answer, returns within the cap
    /// like one that could. Its result is its own word, not "stopped".
    #[tokio::test]
    async fn a_teardown_the_executor_could_not_see_through_is_not_reported_as_stopped() {
        let executor = Arc::new(Running {
            hears: true,
            unseen: true,
            teardown: Duration::from_millis(100),
            stopped: AtomicBool::new(false),
            dropped: Arc::default(),
        });
        let (result, took) = lose_the_claim(executor.clone(), Duration::from_secs(20)).await;
        assert!(took < Duration::from_secs(20), "it returned; the cap was not waited out: {took:?}");
        assert!(!result.success && !result.timed_out && !result.backpressure);
        assert_eq!(result.stopped, Stopped::Unseen, "and the loop is told nobody saw it go");
        let said = result.output.unwrap_or_default();
        assert!(said.ends_with("may still be running: the daemon would not say"), "{said}");
        assert_ne!(said, ClaimLost.to_string());
    }

    /// An executor that keeps count of what it was asked for, and whose
    /// teardown finds something left or does not.
    struct Tearing {
        left: Option<&'static str>,
        executed: AtomicUsize,
        torn_down: AtomicUsize,
    }

    #[async_trait]
    impl Executor for Tearing {
        async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
            self.executed.fetch_add(1, Ordering::SeqCst);
            // As a backend that hears the stop: it ends when it is asked.
            ctx.stop.requested().await;
            Err(anyhow::Error::new(ClaimLost))
        }

        async fn teardown(&self, _ctx: &ExecContext) -> Result<()> {
            self.torn_down.fetch_add(1, Ordering::SeqCst);
            match self.left {
                None => Ok(()),
                Some(what) => Err(anyhow::anyhow!("{what}")),
            }
        }
    }

    /// One dispatch through a pool of one with no lease keeper: the result,
    /// and the stop signal the dispatch carried.
    async fn dispatch_one(
        executor: Arc<Tearing>,
        teardown: bool,
        stop_after: Option<Duration>,
    ) -> TaskResult {
        let _one = ONE_POOL.lock().await;
        let mut pool = WorkerPool::new(1, executor, Arc::new(Metrics::new())).await.unwrap();
        let (result_tx, mut results) = tokio::sync::mpsc::unbounded_channel();
        let ctx = ExecContext::new(vec!["sleep".into(), "600".into()], Some(600), None);
        let stop = ctx.stop.clone();
        pool.dispatch(DispatchPayload {
            task_id: "t-1".into(),
            worker_id: "w-1".into(),
            ctx,
            attempt: 0,
            max_attempts: 1,
            retry_delay_secs: 0,
            retry_max_delay_secs: None,
            retry_on_timeout: true,
            fence: 4,
            spec: None,
            teardown,
            result_tx,
            log_tx: None,
        })
        .unwrap();
        if let Some(after) = stop_after {
            tokio::time::sleep(after).await;
            stop.request();
        }
        let result = tokio::time::timeout(Duration::from_secs(30), results.recv())
            .await
            .expect("a result within 30 s")
            .expect("a result");
        for worker in &pool.workers {
            worker.stop(None);
        }
        for handle in std::mem::take(&mut pool._handles) {
            let _ = handle.await;
        }
        result
    }

    /// A teardown dispatch runs nothing. It asks the executor to see that
    /// nothing of the task is left, and its result says which it was: gone,
    /// or not seen gone, with the backend's reason.
    #[tokio::test]
    async fn a_teardown_dispatch_runs_nothing_and_says_whether_the_work_is_gone() {
        let gone = Arc::new(Tearing {
            left: None,
            executed: AtomicUsize::new(0),
            torn_down: AtomicUsize::new(0),
        });
        let result = dispatch_one(gone.clone(), true, None).await;
        assert_eq!(gone.executed.load(Ordering::SeqCst), 0, "nothing was run");
        assert_eq!(gone.torn_down.load(Ordering::SeqCst), 1);
        assert_eq!(result.stopped, Stopped::Gone);
        assert!(result.teardown && !result.success && !result.backpressure && !result.timed_out);
        assert_eq!(result.fence, 4, "under the teardown's own claim");

        let left = Arc::new(Tearing {
            left: Some("1 of its containers still stopping (0b5d8f2e3a41)"),
            executed: AtomicUsize::new(0),
            torn_down: AtomicUsize::new(0),
        });
        let result = dispatch_one(left.clone(), true, None).await;
        assert_eq!(left.executed.load(Ordering::SeqCst), 0);
        assert_eq!(result.stopped, Stopped::Unseen);
        let said = result.output.unwrap_or_default();
        assert!(
            said.ends_with("may still be running: 1 of its containers still stopping (0b5d8f2e3a41)"),
            "{said}"
        );
    }

    /// The stop reaches a running execution through the signal its dispatch
    /// carries, with no lease keeper to renew anything: the engine asks at its
    /// sweep, and the worker need not learn it from a renewal. An ordinary
    /// result is marked as the task's own.
    #[tokio::test]
    async fn a_stop_asked_from_outside_ends_an_execution_that_renews_no_lease() {
        let executor = Arc::new(Tearing {
            left: None,
            executed: AtomicUsize::new(0),
            torn_down: AtomicUsize::new(0),
        });
        let result = dispatch_one(executor.clone(), false, Some(Duration::from_millis(100))).await;
        assert_eq!(executor.executed.load(Ordering::SeqCst), 1);
        assert_eq!(executor.torn_down.load(Ordering::SeqCst), 0, "an execution tears itself down");
        assert_eq!(result.stopped, Stopped::Gone);
        assert!(!result.teardown && !result.success);
    }

    /// How a task that wrote past its log's cap ends.
    #[derive(Clone, Copy)]
    enum PastTheCap {
        /// It fails, and no executor seals its log.
        Fails,
        /// Its executor seals its log, and it succeeds.
        Seals,
        /// Its claim is lost.
        LosesItsClaim,
    }

    /// What a task wrote past its log's cap in one piece, a line: `more`
    /// bytes past it.
    fn past_the_cap(more: usize) -> String {
        format!("{}\n", "x".repeat(crate::executor::log_task_cap() + more))
    }

    impl PastTheCap {
        /// How far past the cap: past the end the log keeps too, where its
        /// executor seals it, so that something is left out.
        fn more(self) -> usize {
            match self {
                PastTheCap::Seals => {
                    crate::executor::log_tail_bytes(crate::executor::log_task_cap()) + 100_000
                }
                _ => 100_000,
            }
        }
    }

    #[async_trait]
    impl Executor for PastTheCap {
        async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
            let sink = ctx.log_sink.as_ref().expect("a log");
            let wrote = past_the_cap(self.more());
            sink.append(&wrote);
            match self {
                PastTheCap::Fails => anyhow::bail!("the tool gave up"),
                PastTheCap::LosesItsClaim => Err(anyhow::Error::new(ClaimLost)),
                PastTheCap::Seals => {
                    sink.seal(&wrote);
                    Ok(ExecOutput {
                        success: true,
                        output: String::new(),
                        dropped: 0,
                    })
                }
            }
        }
    }

    /// One attempt of a task that writes past its log's cap, with a log,
    /// through a pool of one: its result, every chunk its log was sent by
    /// then, and the pool's metrics.
    async fn past_its_cap(end: PastTheCap) -> (TaskResult, Vec<LogChunk>, Arc<Metrics>) {
        let _one = ONE_POOL.lock().await;
        let metrics = Arc::new(Metrics::new());
        let mut pool = WorkerPool::new(1, Arc::new(end), Arc::clone(&metrics))
            .await
            .unwrap();
        let (result_tx, mut results) = tokio::sync::mpsc::unbounded_channel();
        let (log_tx, mut logs) = tokio::sync::mpsc::unbounded_channel();
        pool.dispatch(DispatchPayload {
            task_id: "t-1".into(),
            worker_id: "w-1".into(),
            ctx: ExecContext::new(vec!["make".into()], Some(10), None),
            attempt: 0,
            max_attempts: 1,
            retry_delay_secs: 0,
            retry_max_delay_secs: None,
            retry_on_timeout: true,
            fence: 3,
            spec: None,
            teardown: false,
            result_tx,
            log_tx: Some(log_tx),
        })
        .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(60), results.recv())
            .await
            .expect("a result within 60 s")
            .expect("a result");
        // Each chunk was sent before the result.
        let chunks = std::iter::from_fn(|| logs.try_recv().ok()).collect();
        for worker in &pool.workers {
            worker.stop(None);
        }
        for handle in std::mem::take(&mut pool._handles) {
            let _ = handle.await;
        }
        (result, chunks, metrics)
    }

    /// The bytes and lines the line that says what a log left out says.
    fn left_out(line: &str) -> (usize, usize) {
        let (_, counts) = line
            .split_once(" of this attempt's output (")
            .expect("the line that says what was left out");
        let (bytes, rest) = counts.split_once(" bytes and ").unwrap();
        let (lines, _) = rest.split_once(" lines, counted").unwrap();
        (bytes.parse().unwrap(), lines.parse().unwrap())
    }

    /// A log past its cap whose executor failed without sealing it ends on
    /// a line that says its end was not kept, then why the attempt failed,
    /// both before the result; and is counted, with what it left out.
    #[tokio::test]
    async fn a_capped_log_no_executor_sealed_ends_on_its_end_not_kept() {
        let (result, chunks, metrics) = past_its_cap(PastTheCap::Fails).await;
        assert!(!result.success);
        let cap = crate::executor::log_task_cap();
        assert_eq!(
            chunks.len(),
            3,
            "the head and the line it is held at, the end's line, the note"
        );
        assert!(chunks[0].first && chunks[0].chunk.len() > cap);
        assert!(
            chunks[0].chunk.contains("is held back"),
            "{}",
            &chunks[0].chunk[cap..]
        );
        assert!(
            chunks[1]
                .chunk
                .ends_with("is left out here, and its end was not kept\n"),
            "{}",
            chunks[1].chunk
        );
        assert_eq!(left_out(&chunks[1].chunk), (100_001, 1));
        assert_eq!(chunks[2].chunk, "the tool gave up\n");
        assert!(chunks[1..].iter().all(|c| !c.first));
        assert_eq!(metrics.logs_capped.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.log_bytes_omitted.load(Ordering::Relaxed), 100_001);
    }

    /// An attempt stopped past its cap (a cancel, a lost claim) says its end
    /// was not kept, and is counted: its row takes the line while it is still
    /// the attempt's, and refuses it once it is another's. No note follows:
    /// a stop is no failure of the task's.
    #[tokio::test]
    async fn a_stopped_attempt_s_capped_log_says_its_end_was_not_kept() {
        let (result, chunks, metrics) = past_its_cap(PastTheCap::LosesItsClaim).await;
        assert_eq!(result.stopped, Stopped::Gone);
        assert_eq!(chunks.len(), 2, "the head, then the line that says so");
        assert!(chunks[0].chunk.ends_with("is added below when it ends\n"));
        assert!(
            chunks[1].chunk.ends_with(", and its end was not kept\n"),
            "{}",
            chunks[1].chunk
        );
        assert_eq!(metrics.logs_capped.load(Ordering::Relaxed), 1);
    }

    /// A log its executor sealed is given its end once, and counted once,
    /// with what the line says it left out.
    #[tokio::test]
    async fn a_sealed_capped_log_is_counted_once() {
        let (result, chunks, metrics) = past_its_cap(PastTheCap::Seals).await;
        assert!(result.success);
        assert_eq!(chunks.len(), 2, "the head, then the end with its line");
        let (gap, tail) = chunks[1].chunk.split_once('\n').unwrap();
        assert!(gap.ends_with("; what follows is its end"), "{gap}");
        let wrote = past_the_cap(PastTheCap::Seals.more());
        let (bytes, _) = left_out(gap);
        let cap = crate::executor::log_task_cap();
        assert_eq!(cap + bytes + tail.len(), wrote.len());
        assert_eq!(bytes, 100_001);
        assert_eq!(tail.len(), crate::executor::log_tail_bytes(cap));
        assert_eq!(metrics.logs_capped.load(Ordering::Relaxed), 1);
        assert_eq!(
            metrics.log_bytes_omitted.load(Ordering::Relaxed),
            bytes as u64
        );
    }
}
