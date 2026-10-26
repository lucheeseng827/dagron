//! The run loop: a session at a time, its tasks run on this host's executor.
//!
//! A task is run by dagron-executor's own [`WorkerPool`], as the engine runs
//! its own: the same redaction, caps, teardown of work whose claim is gone,
//! and the same result. What the engine's database does for it is dagron-api's
//! here, over the protocol:
//!
//! - **One heartbeat a session.** The pool's per-task lease renewal is asked
//!   of a [`Keeper`] that answers from what the session's last heartbeat said
//!   ([`State::cancel`]), and makes no call: a task told to stop is stopped
//!   within a second of the heartbeat that said so.
//! - **Logs and results** go up from the uplink ([`uplink`]): a task's log in
//!   segments, a second at a time, and its result once its log is up.
//! - **A session that is gone** (its lease lapsed while this host was cut
//!   off) has its tasks stopped: dagron-api has taken them back already. The
//!   runner opens another and goes on.
//! - **Drain**: on a signal, or when dagron-api says so, the runner takes
//!   nothing more, lets what it runs finish, closes its session and exits. A
//!   second signal stops the tasks now.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use dagron_core::runner::{Held, Job, LogSegment, OpenSession, Outcome, RegisterRequest, Report};
use dagron_executor::executor::LogChunk;
use dagron_executor::worker::{DispatchPayload, LeaseKeeper, Stopped, TaskResult, WorkerPool};
use tokio::sync::{mpsc, watch, Notify};

use crate::client::{Backoff, CallError, Client};
use crate::config::Config;
use crate::host::Host;

/// How long a claim long-polls.
const CLAIM_WAIT_SECS: u64 = 30;

/// How often the pool asks the [`Keeper`] of a running task: locally, so as
/// often as a stop should be heard.
const KEEPER_EVERY: Duration = Duration::from_secs(1);

/// How long a log waits to be sent with what follows it.
const LOG_HOLD: Duration = Duration::from_secs(1);

/// The most of a log sent at once: well under dagron-api's body limit.
const LOG_SEGMENT_MAX: usize = 1024 * 1024;

/// The most of a task's output a result carries: its end.
const OUTPUT_MAX: usize = 4 * 1024 * 1024;

/// How long a result is tried before it is given up: by then the session has
/// lapsed, and the task been taken back.
const RESULT_GIVE_UP: Duration = Duration::from_secs(15 * 60);

/// Whether, and how, the runner is asked to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Shutdown {
    /// Run.
    No,
    /// Drain: take nothing more, finish what runs, exit.
    Drain,
    /// Stop what runs, say so, exit.
    Now,
}

/// A watch the run loop reads its [`Shutdown`] from: SIGTERM or Ctrl-C drain,
/// and a second one stops now.
pub fn shutdown_on_signals() -> watch::Receiver<Shutdown> {
    let (tx, rx) = watch::channel(Shutdown::No);
    tokio::spawn(async move {
        for next in [Shutdown::Drain, Shutdown::Now] {
            wait_for_signal().await;
            match next {
                Shutdown::Drain => tracing::info!(
                    "asked to stop: draining (no new tasks; a second signal stops the running ones)"
                ),
                _ => tracing::warn!("asked again: stopping the running tasks"),
            }
            if tx.send(next).is_err() {
                return;
            }
        }
    });
    rx
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// What this process holds: each task it runs, or has yet to say the result
/// of, and those it is to stop.
#[derive(Default)]
pub(crate) struct State {
    held: HashMap<String, Holding>,
    /// Each `(task, fence)` whose work is to stop.
    cancel: HashSet<(String, i64)>,
}

#[derive(Debug, Clone)]
struct Holding {
    fence: i64,
    session_id: String,
}

#[derive(Default)]
pub(crate) struct Shared {
    state: Mutex<State>,
    /// A task's result was said: a full runner may claim again, and a
    /// draining one may be done.
    freed: Notify,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn held(&self) -> usize {
        self.lock().held.len()
    }

    fn held_by(&self, session_id: &str) -> Vec<Held> {
        self.lock()
            .held
            .iter()
            .filter(|(_, h)| h.session_id == session_id)
            .map(|(id, h)| Held {
                task_id: id.clone(),
                fence: h.fence,
            })
            .collect()
    }

    fn session_of(&self, task_id: &str) -> Option<Holding> {
        self.lock().held.get(task_id).cloned()
    }

    /// Stop the work of `tasks` of `session_id`.
    fn cancel(&self, session_id: &str, tasks: &[String]) {
        let mut state = self.lock();
        let stop: Vec<(String, i64)> = tasks
            .iter()
            .filter_map(|id| {
                state
                    .held
                    .get(id)
                    .filter(|h| h.session_id == session_id)
                    .map(|h| (id.clone(), h.fence))
            })
            .collect();
        state.cancel.extend(stop);
    }

    /// Stop the work of every task of `session_id`, or of every task.
    fn cancel_all(&self, session_id: Option<&str>) {
        let mut state = self.lock();
        let stop: Vec<(String, i64)> = state
            .held
            .iter()
            .filter(|(_, h)| session_id.is_none_or(|s| h.session_id == s))
            .map(|(id, h)| (id.clone(), h.fence))
            .collect();
        state.cancel.extend(stop);
    }

    /// The task's result was said, or will never be: it holds nothing now.
    fn forget(&self, task_id: &str, fence: i64) {
        let mut state = self.lock();
        if state.held.get(task_id).is_some_and(|h| h.fence == fence) {
            state.held.remove(task_id);
        }
        state.cancel.remove(&(task_id.to_string(), fence));
        drop(state);
        self.freed.notify_waiters();
    }
}

/// The pool's per-task lease renewal, answered from what the session's
/// heartbeat said: no call is made for it.
struct Keeper(Arc<Shared>);

#[async_trait]
impl LeaseKeeper for Keeper {
    async fn renew(&self, task_id: &str, _worker_id: &str, fence: i64) -> Result<bool> {
        Ok(!self.0.lock().cancel.contains(&(task_id.to_string(), fence)))
    }
}

/// A task that does not run here, and why: said as its result.
struct Refused {
    task_id: String,
    fence: i64,
    why: String,
}

/// How a session ended.
enum SessionEnd {
    /// Drained: the runner is done.
    Drained,
    /// Gone (its lease lapsed): open another.
    Lost,
}

pub struct Runner {
    cfg: Config,
    client: Client,
    host: Arc<Host>,
    shared: Arc<Shared>,
    /// `None` once stopped, as `run` ends.
    pool: Option<WorkerPool>,
    results_tx: mpsc::UnboundedSender<TaskResult>,
    logs_tx: mpsc::UnboundedSender<LogChunk>,
    refused_tx: mpsc::UnboundedSender<Refused>,
    /// Set when dagron-api refuses the credential: nothing more can be said.
    revoked: Arc<AtomicBool>,
}

impl Runner {
    /// A runner of `host`, speaking to dagron-api with `client` (which holds
    /// the runner's credential). Starts its uplink.
    pub async fn new(cfg: Config, client: Client, host: Host) -> Result<Runner> {
        let shared = Arc::new(Shared::default());
        let pool = WorkerPool::with_lease_keeper(
            cfg.capacity as usize,
            Arc::clone(&host.executor),
            Arc::clone(&host.metrics),
            Some(Arc::new(Keeper(Arc::clone(&shared)))),
            KEEPER_EVERY,
        )
        .await
        .context("starting the worker pool")?;
        let (results_tx, results_rx) = mpsc::unbounded_channel();
        let (logs_tx, logs_rx) = mpsc::unbounded_channel();
        let (refused_tx, refused_rx) = mpsc::unbounded_channel();
        let revoked = Arc::new(AtomicBool::new(false));
        tokio::spawn(uplink(
            client.clone(),
            Arc::clone(&shared),
            Arc::clone(&revoked),
            results_rx,
            logs_rx,
            refused_rx,
        ));
        Ok(Runner {
            cfg,
            client,
            host: Arc::new(host),
            shared,
            pool: Some(pool),
            results_tx,
            logs_tx,
            refused_tx,
            revoked,
        })
    }

    /// Run until drained, a session at a time. An error is one asking again
    /// would not change: a refused credential, or no protocol in common.
    pub async fn run(mut self, shutdown: watch::Receiver<Shutdown>) -> Result<()> {
        let outcome = self.sessions(shutdown).await;
        if outcome.is_err() {
            // Nothing more can be said: what still runs is stopped.
            self.shared.cancel_all(None);
        }
        let pool = self.pool.take();
        if let Some(pool) = pool {
            pool.stop().await;
        }
        outcome
    }

    async fn sessions(&self, mut shutdown: watch::Receiver<Shutdown>) -> Result<()> {
        let mut backoff = Backoff::default();
        loop {
            if *shutdown.borrow() != Shutdown::No {
                self.wind_down(&mut shutdown, None).await;
                return Ok(());
            }
            let request = OpenSession {
                labels: self.cfg.labels.clone(),
                capacity: self.cfg.capacity,
                version: dagron_logging::VERSION.to_string(),
                protocols: dagron_core::runner::PROTOCOLS.to_vec(),
                secrets: self.cfg.secrets,
                os: Some(std::env::consts::OS.to_string()),
                arch: Some(std::env::consts::ARCH.to_string()),
            };
            let session = match self.client.open_session(&request).await {
                Ok(s) => s,
                Err(CallError::Unauthorized(d)) => {
                    bail!("dagron-api refused this runner's credential: {d}")
                }
                Err(CallError::Upgrade(d)) => bail!("dagron-api will not take this runner: {d}"),
                Err(e) => {
                    tracing::warn!(error = %e, "could not open a session; trying again");
                    tokio::select! {
                        _ = backoff.wait() => {}
                        _ = shutdown.changed() => {}
                    }
                    continue;
                }
            };
            backoff.reset();
            tracing::info!(
                session_id = %session.session_id, protocol = session.protocol,
                labels = ?session.labels, capacity = session.capacity,
                executor = ?self.host.kind, secrets = self.cfg.secrets.as_str(),
                "session open"
            );
            match self.serve(&session, &mut shutdown).await? {
                SessionEnd::Drained => return Ok(()),
                SessionEnd::Lost => continue,
            }
        }
    }

    /// One session: claim while there is room, heartbeat it, and end it.
    async fn serve(
        &self,
        session: &dagron_core::runner::Session,
        shutdown: &mut watch::Receiver<Shutdown>,
    ) -> Result<SessionEnd> {
        let sid = session.session_id.clone();
        let draining = Arc::new(AtomicBool::new(false));
        let lost = Arc::new(AtomicBool::new(false));
        let changed = Arc::new(Notify::new());
        let (stop_beat, beat_stopped) = watch::channel(false);
        let beat = tokio::spawn(heartbeat_loop(
            self.client.clone(),
            Arc::clone(&self.shared),
            sid.clone(),
            Duration::from_secs(session.heartbeat_secs.max(1) as u64),
            Arc::clone(&draining),
            Arc::clone(&lost),
            Arc::clone(&self.revoked),
            Arc::clone(&changed),
            beat_stopped,
        ));
        let mut backoff = Backoff::default();
        let capacity = self.cfg.capacity as usize;

        loop {
            if self.revoked.load(Ordering::SeqCst) {
                let _ = stop_beat.send(true);
                self.shared.cancel_all(None);
                bail!("dagron-api refused this runner's credential: it was revoked");
            }
            if lost.load(Ordering::SeqCst) {
                tracing::warn!(session_id = %sid, "the session is gone: stopping its tasks, and opening another");
                self.shared.cancel_all(Some(&sid));
                let _ = stop_beat.send(true);
                return Ok(SessionEnd::Lost);
            }
            if draining.load(Ordering::SeqCst) || *shutdown.borrow() != Shutdown::No {
                break;
            }
            if self.shared.held() >= capacity {
                let freed = self.shared.freed.notified();
                tokio::select! {
                    _ = freed => {}
                    _ = changed.notified() => {}
                    _ = shutdown.changed() => {}
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
                continue;
            }
            tokio::select! {
                claimed = self.client.claim(&sid, CLAIM_WAIT_SECS) => match claimed {
                    Ok(claimed) => {
                        backoff.reset();
                        for job in claimed.tasks {
                            self.dispatch(job, &sid);
                        }
                        if claimed.drain {
                            tracing::info!(session_id = %sid, "dagron-api asked this runner to drain");
                            draining.store(true, Ordering::SeqCst);
                        }
                    }
                    Err(CallError::Gone) => lost.store(true, Ordering::SeqCst),
                    Err(CallError::Unauthorized(_)) => self.revoked.store(true, Ordering::SeqCst),
                    Err(e) => {
                        tracing::warn!(error = %e, "claim failed; trying again");
                        tokio::select! {
                            _ = backoff.wait() => {}
                            _ = shutdown.changed() => {}
                            _ = changed.notified() => {}
                        }
                    }
                },
                // A signal or a heartbeat's word ends the wait for work at
                // once; a claim abandoned mid-poll is given back by
                // dagron-api once the session's heartbeats do not name it.
                _ = shutdown.changed() => {}
                _ = changed.notified() => {}
            }
        }

        // Drain: no new tasks; the session is told, and ends once what it
        // runs is over.
        tracing::info!(session_id = %sid, running = self.shared.held_by(&sid).len(), "draining");
        match self.client.close_session(&sid).await {
            Ok(_) | Err(CallError::Gone) => {}
            Err(e) => tracing::warn!(error = %e, "could not tell dagron-api the session drains"),
        }
        self.wind_down(shutdown, Some((&sid, &*lost))).await;
        match self.client.close_session(&sid).await {
            Ok(true) | Err(CallError::Gone) => tracing::info!(session_id = %sid, "session ended"),
            Ok(false) => {
                tracing::warn!(session_id = %sid, "the session still holds tasks; its lease will lapse")
            }
            Err(e) => tracing::warn!(error = %e, "could not end the session; its lease will lapse"),
        }
        let _ = stop_beat.send(true);
        let _ = beat.await;
        Ok(SessionEnd::Drained)
    }

    /// Wait for what runs to be over and said: as long as it takes, or until
    /// the drain timeout or a second signal, when it is stopped. A draining
    /// session's tasks are stopped too once the session is `lost`: dagron-api
    /// has given them to other runners, so they are no longer this one's.
    async fn wind_down(
        &self,
        shutdown: &mut watch::Receiver<Shutdown>,
        session: Option<(&str, &AtomicBool)>,
    ) {
        let started = Instant::now();
        let mut stopping = false;
        let mut given_back = false;
        loop {
            if self.shared.held() == 0 {
                return;
            }
            if let Some((sid, lost)) = session {
                if !given_back && lost.load(Ordering::SeqCst) {
                    tracing::warn!(session_id = %sid, "the draining session is gone: stopping its tasks");
                    self.shared.cancel_all(Some(sid));
                    given_back = true;
                }
            }
            let timed_out = self
                .cfg
                .drain_timeout
                .is_some_and(|t| started.elapsed() >= t);
            if !stopping && (*shutdown.borrow() == Shutdown::Now || timed_out) {
                tracing::warn!(running = self.shared.held(), "stopping the running tasks");
                self.shared.cancel_all(None);
                stopping = true;
            }
            let freed = self.shared.freed.notified();
            tokio::select! {
                _ = freed => {}
                _ = shutdown.changed() => {}
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    /// Run `job` on the pool, or say why it does not run here.
    fn dispatch(&self, job: Job, session_id: &str) {
        let task_id = job.task_id.clone();
        let fence = job.fence;
        let attempt = job.attempt;
        self.shared.lock().held.insert(
            task_id.clone(),
            Holding {
                fence,
                session_id: session_id.to_string(),
            },
        );
        let refuse = |why: String| {
            tracing::warn!(%task_id, reason = %why, "the task does not run here");
            let _ = self.refused_tx.send(Refused {
                task_id: task_id.clone(),
                fence,
                why,
            });
        };
        match self.host.prepare(job) {
            Ok(ctx) => {
                tracing::info!(%task_id, fence, attempt = attempt + 1, "running a task");
                let Some(pool) = self.pool.as_ref() else {
                    refuse("the runner is stopping".into());
                    return;
                };
                let dispatched = pool.dispatch(DispatchPayload {
                    task_id: task_id.clone(),
                    worker_id: session_id.to_string(),
                    ctx,
                    attempt,
                    // The engine decides what follows a result: these are not
                    // read here.
                    max_attempts: 1,
                    retry_delay_secs: 0,
                    retry_max_delay_secs: None,
                    retry_on_timeout: true,
                    fence,
                    spec: None,
                    teardown: false,
                    result_tx: self.results_tx.clone(),
                    log_tx: Some(self.logs_tx.clone()),
                });
                if let Err(e) = dispatched {
                    refuse(format!("the runner could not start it: {e:#}"));
                }
            }
            Err(why) => refuse(why),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn heartbeat_loop(
    client: Client,
    shared: Arc<Shared>,
    sid: String,
    every: Duration,
    draining: Arc<AtomicBool>,
    lost: Arc<AtomicBool>,
    revoked: Arc<AtomicBool>,
    changed: Arc<Notify>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = stop.changed() => return,
        }
        let running = shared.held_by(&sid);
        match client.heartbeat(&sid, running).await {
            Ok(ack) => {
                if !ack.cancel.is_empty() {
                    tracing::info!(session_id = %sid, tasks = ?ack.cancel, "stopping tasks dagron-api says are to stop");
                    shared.cancel(&sid, &ack.cancel);
                }
                if ack.drain && !draining.swap(true, Ordering::SeqCst) {
                    tracing::info!(session_id = %sid, "dagron-api asked this runner to drain");
                    changed.notify_waiters();
                }
            }
            Err(CallError::Gone) => {
                lost.store(true, Ordering::SeqCst);
                changed.notify_waiters();
                return;
            }
            Err(CallError::Unauthorized(_)) => {
                revoked.store(true, Ordering::SeqCst);
                changed.notify_waiters();
                return;
            }
            // The lease is three heartbeats long: one that is lost is not one
            // that lapses it.
            Err(e) => tracing::warn!(error = %e, "heartbeat failed; trying at the next"),
        }
    }
}

/// One task's log on its way up.
struct LogBuf {
    fence: i64,
    /// The next segment's number. The numbers only rise through the attempt,
    /// a seal's among them: dagron-api reads one at or below the last it
    /// wrote as written already.
    seq: u64,
    pending: String,
    /// The next segment replaces the log: the attempt's first, or a seal.
    reset: bool,
    /// How much of `pending` the segment sent last carried, while it waits
    /// for an answer it did not get. It is sent again as it was, since
    /// dagron-api may have written it; or, where a seal supersedes it, its
    /// number is given up.
    unanswered: Option<usize>,
    since: Option<Instant>,
    /// dagron-api keeps no more of it: the claim is gone, or the log is full.
    dead: bool,
}

/// Logs and results, on their way up. A task's result goes once its log is
/// up, since dagron-api keeps no log of a task an engine has ended.
async fn uplink(
    client: Client,
    shared: Arc<Shared>,
    revoked: Arc<AtomicBool>,
    mut results: mpsc::UnboundedReceiver<TaskResult>,
    mut logs: mpsc::UnboundedReceiver<LogChunk>,
    mut refused: mpsc::UnboundedReceiver<Refused>,
) {
    let mut bufs: HashMap<String, LogBuf> = HashMap::new();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            chunk = logs.recv() => match chunk {
                Some(chunk) => keep(&mut bufs, chunk),
                None => return,
            },
            result = results.recv() => {
                let Some(result) = result else { return };
                while let Ok(chunk) = logs.try_recv() {
                    keep(&mut bufs, chunk);
                }
                // What waits of its log goes up, then its result, from a task
                // of their own: a log's end comes at once, and a large one
                // would hold up every other task's log and result here.
                if let Some(holding) = shared.session_of(&result.task_id) {
                    let buf = bufs.remove(&result.task_id);
                    let (task_id, report) = report_of(&holding.session_id, result);
                    let (client, shared, revoked) = (client.clone(), Arc::clone(&shared), Arc::clone(&revoked));
                    tokio::spawn(async move {
                        if let Some(mut buf) = buf {
                            flush_last(&client, &holding.session_id, &task_id, &mut buf).await;
                        }
                        say(client, shared, revoked, task_id, report).await;
                    });
                }
            }
            r = refused.recv() => {
                let Some(r) = r else { return };
                if let Some(holding) = shared.session_of(&r.task_id) {
                    let report = Report {
                        session_id: holding.session_id,
                        fence: r.fence,
                        outcome: Outcome::Failed,
                        exit_code: None,
                        output: r.why,
                    };
                    tokio::spawn(say(client.clone(), Arc::clone(&shared), Arc::clone(&revoked), r.task_id, report));
                }
            }
            _ = tick.tick() => {
                let due: Vec<String> = bufs
                    .iter()
                    .filter(|(_, b)| !b.dead && b.since.is_some_and(|s| s.elapsed() >= LOG_HOLD))
                    .map(|(id, _)| id.clone())
                    .collect();
                for task_id in due {
                    let Some(holding) = shared.session_of(&task_id) else {
                        bufs.remove(&task_id);
                        continue;
                    };
                    // A segment a task each tick: a log's end comes in one
                    // piece, many segments long, and goes up with its result,
                    // from a task of its own, holding up no other here.
                    if let Some(buf) = bufs.get_mut(&task_id) {
                        flush(&client, &holding.session_id, &task_id, buf, 1).await;
                    }
                }
            }
        }
    }
}

fn keep(bufs: &mut HashMap<String, LogBuf>, chunk: LogChunk) {
    let buf = bufs.entry(chunk.task_id).or_insert_with(|| LogBuf {
        fence: chunk.fence,
        seq: 0,
        pending: String::new(),
        reset: true,
        unanswered: None,
        since: None,
        dead: false,
    });
    if buf.fence != chunk.fence {
        *buf = LogBuf {
            fence: chunk.fence,
            seq: 0,
            pending: String::new(),
            reset: true,
            unanswered: None,
            since: None,
            dead: false,
        };
    }
    if buf.dead {
        return;
    }
    if chunk.first {
        // The attempt's first, or the whole log over again (a seal): what
        // waited is in it, or superseded by it. It replaces the log, and so
        // whatever an unanswered segment wrote; that segment's number, which
        // dagron-api may have written, is given up, and the seal goes past it.
        buf.pending.clear();
        buf.reset = true;
        if buf.unanswered.take().is_some() {
            buf.seq += 1;
        }
    }
    buf.pending.push_str(&chunk.chunk);
    buf.since.get_or_insert_with(Instant::now);
}

/// Send what of a task's log waits, in segments numbered on from the last,
/// up to `most` of them; what is left waits for the next. What could not be
/// sent waits too, and goes then as the same segment, number and text; a
/// log dagron-api keeps no more of is dropped.
async fn flush(client: &Client, session_id: &str, task_id: &str, buf: &mut LogBuf, most: usize) {
    let mut sent = 0;
    while !buf.dead && !buf.pending.is_empty() && sent < most {
        let cut = match buf.unanswered {
            // `pending` has only grown at its end since.
            Some(cut) => cut,
            None => {
                let mut cut = buf.pending.len().min(LOG_SEGMENT_MAX);
                while !buf.pending.is_char_boundary(cut) {
                    cut -= 1;
                }
                cut
            }
        };
        let seq = buf.seq;
        let segment = LogSegment {
            session_id: session_id.to_string(),
            fence: buf.fence,
            text: buf.pending[..cut].to_string(),
            reset: buf.reset,
        };
        match client.log(task_id, seq, &segment).await {
            Ok(()) => {
                buf.pending.drain(..cut);
                buf.reset = false;
                buf.unanswered = None;
                buf.seq = seq + 1;
                sent += 1;
            }
            Err(CallError::Conflict(_))
            | Err(CallError::Gone)
            | Err(CallError::Unauthorized(_)) => {
                buf.dead = true;
                buf.pending.clear();
                buf.unanswered = None;
            }
            Err(e) => {
                tracing::debug!(%task_id, error = %e, "a log segment could not be sent; trying again");
                buf.unanswered = Some(cut);
                return;
            }
        }
    }
    if buf.dead || buf.pending.is_empty() {
        buf.since = None;
    }
}

/// How long the last of a task's log waits between tries, where an answer
/// was lost: about four seconds in all.
const LAST_FLUSH_WAITS: [Duration; 4] = [
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_millis(1000),
    Duration::from_millis(2000),
];

/// Send what waits of a task's log once its attempt is over, before its
/// result: tried again where an answer was lost, as [`flush`] tries a
/// segment again, for [`LAST_FLUSH_WAITS`]. A log dagron-api keeps no more of
/// ends it at once. Past that it is given up, and the result goes without it.
async fn flush_last(client: &Client, session_id: &str, task_id: &str, buf: &mut LogBuf) {
    flush(client, session_id, task_id, buf, usize::MAX).await;
    for wait in LAST_FLUSH_WAITS {
        if buf.dead || buf.pending.is_empty() {
            return;
        }
        tokio::time::sleep(wait).await;
        flush(client, session_id, task_id, buf, usize::MAX).await;
    }
    if !buf.dead && !buf.pending.is_empty() {
        tracing::warn!(%task_id, "the end of a task's log could not be sent; its result is said without it");
    }
}

/// A task's result, as the protocol says it.
fn report_of(session_id: &str, r: TaskResult) -> (String, Report) {
    let outcome = match r.stopped {
        Stopped::Gone => Outcome::Cancelled,
        Stopped::Unseen => {
            tracing::warn!(task_id = %r.task_id, "the task was stopped, and its work was not seen gone");
            Outcome::Cancelled
        }
        Stopped::No if r.success => Outcome::Succeeded,
        Stopped::No if r.backpressure => Outcome::Backpressure,
        Stopped::No if r.timed_out => Outcome::TimedOut,
        Stopped::No => Outcome::Failed,
    };
    let output = dagron_executor::executor::cap_output(r.output.unwrap_or_default(), OUTPUT_MAX);
    (
        r.task_id,
        Report {
            session_id: session_id.to_string(),
            fence: r.fence,
            outcome,
            exit_code: None,
            output,
        },
    )
}

/// Say how a task came out, until dagron-api has it, or says it is too late.
async fn say(
    client: Client,
    shared: Arc<Shared>,
    revoked: Arc<AtomicBool>,
    task_id: String,
    report: Report,
) {
    let started = Instant::now();
    let mut backoff = Backoff::default();
    loop {
        match client.result(&task_id, &report).await {
            Ok(()) => {
                tracing::info!(%task_id, outcome = report.outcome.as_str(), "result said");
                break;
            }
            Err(CallError::Conflict(_)) | Err(CallError::Gone) => {
                tracing::info!(%task_id, "the result came too late: the task is not this session's any more");
                break;
            }
            Err(CallError::Unauthorized(_)) => {
                revoked.store(true, Ordering::SeqCst);
                break;
            }
            Err(e) if e.is_transient() && started.elapsed() < RESULT_GIVE_UP => {
                tracing::warn!(%task_id, error = %e, "the result could not be said; trying again");
                backoff.wait().await;
            }
            Err(e) => {
                tracing::error!(%task_id, error = %e, "the result could not be said; giving it up");
                break;
            }
        }
    }
    shared.forget(&task_id, report.fence);
}

/// The runner's credential: the one kept from its registration, or one it is
/// given, or a new one, for the join token it was given for its first start.
pub async fn credential(cfg: &Config, client: &Client) -> Result<String> {
    if let Ok(text) = tokio::fs::read_to_string(&cfg.token_file).await {
        let token = text.trim();
        if !token.is_empty() {
            return Ok(token.to_string());
        }
    }
    let Some(join_file) = &cfg.join_token_file else {
        bail!(
            "no credential at {}: set {} for the runner's first start",
            cfg.token_file.display(),
            crate::config::ENV_JOIN_TOKEN_FILE
        );
    };
    let join_token = tokio::fs::read_to_string(join_file)
        .await
        .with_context(|| format!("reading the join token at {}", join_file.display()))?;
    let registered = register(cfg, client, join_token.trim()).await?;
    store_credential(&cfg.token_file, &registered.token).await?;
    tracing::info!(
        runner = %format!("runner:{}", registered.runner_id), labels = ?registered.labels,
        credential = %cfg.token_file.display(), "registered"
    );
    Ok(registered.token)
}

/// Exchange a join token for a credential.
pub async fn register(
    cfg: &Config,
    client: &Client,
    join_token: &str,
) -> Result<dagron_core::runner::Registered> {
    let request = RegisterRequest {
        join_token: join_token.to_string(),
        name: cfg.name.clone().or_else(host_name),
        labels: cfg.labels.clone(),
        os: Some(std::env::consts::OS.to_string()),
        arch: Some(std::env::consts::ARCH.to_string()),
        capacity: cfg.capacity,
        version: Some(dagron_logging::VERSION.to_string()),
    };
    let mut backoff = Backoff::default();
    loop {
        match client.register(&request).await {
            Ok(r) => return Ok(r),
            Err(e) if e.is_transient() => {
                tracing::warn!(error = %e, "could not register; trying again");
                backoff.wait().await;
            }
            Err(e) => bail!("registering: {e}"),
        }
    }
}

/// Keep the credential where it is read from next time, readable by this
/// user alone.
pub async fn store_credential(path: &std::path::Path, token: &str) -> Result<()> {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(dir)
            .await
            .with_context(|| format!("making {}", dir.display()))?;
    }
    tokio::fs::write(path, format!("{token}\n"))
        .await
        .with_context(|| format!("writing the credential to {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    }
    Ok(())
}

fn host_name() -> Option<String> {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
}

#[cfg(test)]
mod tests;
