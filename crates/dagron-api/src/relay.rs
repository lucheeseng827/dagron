//! The live log relay (`docs/LOG_PIPELINE.md` §5): a task's log
//! followed as it is written, by as many as watch it, for the cost of one
//! reader.
//!
//! - **One reader for every watched run.** A run somebody follows is
//!   *watched*: the relay reads its tasks' logs from where they have got to,
//!   a pass at a time, and sends what each pass found on the run's channel to
//!   every watcher at once ([`Pass`]). A run nobody follows costs nothing: it
//!   is not read. One that nobody has followed for [`Settings::linger`] is let
//!   go.
//! - **Woken, and on a timer.** A pass is made when the database tells of the
//!   run (its `task_events` NOTIFY: a log append in a store that keeps the open
//!   part in the row, or no store, and a task's end), and every
//!   [`Settings::poll`] whatever it tells: a `file://` store's growth, and the
//!   SQLite build's, tell nobody, and a notification can be lost.
//! - **By bytes.** A log is read from a byte position of the writing it is
//!   ([`LogKey`]: the attempt and the generation, `log_gen`): its closed
//!   segments, then its open part (a `file://` store's file, or the task row's
//!   `log`). Only the bytes past where the relay got to are read, a
//!   [`Settings::step`] at most at a time, and what is sent ends at a line's
//!   end: a line not yet finished waits, unless it is longer than
//!   [`Settings::line_max`] or has waited [`Settings::carry_wait`].
//! - **Bounded.** At most [`Settings::max_tails`] runs are watched at once, and
//!   a stream that would be the one more is refused (429). The text the
//!   channels hold is at most [`Settings::text_bytes`] for the whole process: a
//!   pass that cannot have its text sends where the logs got to instead, and
//!   each watcher reads the text itself ([`What::Grew`]).
//!
//! What a watcher does with it, and what it sends its client, is
//! `routes::logstream`'s.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use dagron_artifact::log_segments::{LogKey, SegmentStore};
use dagron_core::db::{self, LogTail, RowRead};
use futures::FutureExt;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::state::AppState;

/// How the relay is set ([`Settings::from_env`]).
#[derive(Debug, Clone)]
pub struct Settings {
    /// The most runs watched at once (`DAGRON_LOG_MAX_TAILS`).
    pub max_tails: usize,
    /// The most streams open at once, of watched runs and of runs that are
    /// over alike (`DAGRON_LOG_MAX_STREAMS`).
    pub max_streams: usize,
    /// How often a watched run is read when nothing tells of it
    /// (`DAGRON_LOG_STREAM_POLL_MS`).
    pub poll: Duration,
    /// How long a run nobody follows stays watched, so that a page loaded
    /// again finds it so (`DAGRON_LOG_STREAM_LINGER_MS`).
    pub linger: Duration,
    /// The most text the runs' channels hold at once, for the whole process
    /// (`DAGRON_LOG_RELAY_BYTES`).
    pub text_bytes: usize,
    /// The least time between two passes: notifications that come together
    /// are one pass.
    pub min_gap: Duration,
    /// The most bytes one read takes of one task's log.
    pub step: usize,
    /// How much of a task's log a stream begins with where no cursor says
    /// where (`docs/LOG_PIPELINE.md` §5.2).
    pub window: u64,
    /// A line longer than this is sent as it comes, in pieces.
    pub line_max: usize,
    /// A line that has waited this long for its end is sent as it is (a
    /// progress bar, drawn with `\r`).
    pub carry_wait: Duration,
    /// How many passes a run's channel holds for a watcher that is behind.
    pub ring: usize,
    /// How many reads of their own the streams make at once: a stream's
    /// first, and a watcher's that fell behind its channel.
    pub reads: usize,
    /// How long a stream waits for its client to take what it sends before
    /// it ends: a client that stops reading holds nothing of the relay's.
    pub stalled: Duration,
    /// The longest a stream lasts: it is asked for again, and its caller
    /// asked who they are again (a token revoked meanwhile among them).
    pub life: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            max_tails: 256,
            max_streams: 2048,
            poll: Duration::from_millis(1000),
            linger: Duration::from_millis(5000),
            text_bytes: 64 << 20,
            min_gap: Duration::from_millis(250),
            step: 1 << 20,
            window: 256 << 10,
            line_max: 64 << 10,
            carry_wait: Duration::from_secs(2),
            ring: 16,
            reads: if cfg!(feature = "postgres") { 4 } else { 1 },
            stalled: Duration::from_secs(60),
            life: Duration::from_secs(600),
        }
    }
}

impl Settings {
    /// The relay's knobs from the environment; one that cannot be what it
    /// means stops the start.
    pub fn from_env() -> Result<Settings, String> {
        Settings::parse(|name| std::env::var(name).ok())
    }

    fn parse(get: impl Fn(&str) -> Option<String>) -> Result<Settings, String> {
        let mut s = Settings::default();
        let num = |name: &str, least: u64| -> Result<Option<u64>, String> {
            let Some(v) = get(name).filter(|v| !v.trim().is_empty()) else {
                return Ok(None);
            };
            match v.trim().parse::<u64>() {
                Ok(n) if n >= least => Ok(Some(n)),
                _ => Err(format!("{name}={v:?} is not a number of at least {least}")),
            }
        };
        if let Some(n) = num("DAGRON_LOG_MAX_TAILS", 1)? {
            s.max_tails = n as usize;
        }
        if let Some(n) = num("DAGRON_LOG_MAX_STREAMS", 1)? {
            s.max_streams = n as usize;
        }
        if let Some(n) = num("DAGRON_LOG_STREAM_POLL_MS", 50)? {
            s.poll = Duration::from_millis(n);
        }
        if let Some(n) = num("DAGRON_LOG_STREAM_LINGER_MS", 0)? {
            s.linger = Duration::from_millis(n);
        }
        if let Some(n) = num("DAGRON_LOG_RELAY_BYTES", 1 << 20)? {
            s.text_bytes = n.min(u32::MAX as u64) as usize;
        }
        Ok(s)
    }
}

/// What a pass found of one watched run: its tasks' pieces, in its order of
/// execution, and the share of the relay's text budget their text holds,
/// given back when the last watcher has let the pass go.
pub(crate) struct Pass {
    pub(crate) pieces: Vec<Piece>,
    _text: Option<OwnedSemaphorePermit>,
}

/// What a pass found of one task.
#[derive(Debug, Clone)]
pub(crate) struct Piece {
    pub(crate) task_id: Arc<str>,
    pub(crate) name: Arc<str>,
    pub(crate) attempt: i64,
    /// The writing of the log the piece is of: its attempt, and its
    /// generation.
    pub(crate) fence: i64,
    pub(crate) gen: i64,
    pub(crate) status: Arc<str>,
    pub(crate) what: What,
}

#[derive(Debug, Clone)]
pub(crate) enum What {
    /// The task's status changed (it is the piece's `status`).
    Status,
    /// The relay follows this writing of the task's log from `end`: what is
    /// before it is no pass's, and a watcher behind it reads it itself. A
    /// watcher of another writing goes on with this one.
    Began { end: u64 },
    /// Text of the log, from byte `start` to byte `end`, cut at a line's end
    /// (or a character's, for a line too long or too long waiting, and at an
    /// attempt's end).
    Text {
        start: u64,
        end: u64,
        text: Arc<str>,
    },
    /// The log got to `end`, its text not carried (the relay's text budget
    /// was spent): a watcher reads it itself.
    Grew { end: u64 },
    /// This writing is over at `end`: its task is no longer running, and the
    /// relay read all of it there was (`complete`), or found it gone first.
    Ended { end: u64, complete: bool },
}

/// The live relay ([`crate::state::AppState::relay`]).
pub struct Relay {
    pub(crate) settings: Settings,
    reg: Mutex<Registry>,
    wake: Notify,
    text: Arc<Semaphore>,
    /// The streams' own reads at once ([`Settings::reads`]).
    pub(crate) reads: Arc<Semaphore>,
    /// The streams open at once ([`Settings::max_streams`]).
    streams: Arc<Semaphore>,
    stop: CancellationToken,
    running: AtomicBool,
}

#[derive(Default)]
struct Registry {
    runs: HashMap<String, Watched>,
    /// Each entry's own number: a run let go and watched again is another.
    epochs: u64,
}

/// A watched run: its channel, which only the registry sends on (so that a
/// relay that stops closes every watcher's), what its watchers want read,
/// and since when none has watched it.
struct Watched {
    tx: broadcast::Sender<Arc<Pass>>,
    want: Want,
    idle_since: Option<Instant>,
    dirty: bool,
    epoch: u64,
}

/// Which of a run's tasks its watchers want read: all of them (as many
/// watchers as want that), or these, by id or by name (lowercase).
#[derive(Default)]
struct Want {
    all: usize,
    keys: HashMap<String, usize>,
    /// `keys` as a pass reads them, made again only when they change.
    set: Arc<HashSet<String>>,
}

impl Want {
    fn add(&mut self, keys: &Option<Vec<String>>) {
        match keys {
            None => self.all += 1,
            Some(keys) => {
                for key in keys {
                    *self.keys.entry(key.to_lowercase()).or_default() += 1;
                }
                self.set = Arc::new(self.keys.keys().cloned().collect());
            }
        }
    }

    fn remove(&mut self, keys: &Option<Vec<String>>) {
        match keys {
            None => self.all = self.all.saturating_sub(1),
            Some(keys) => {
                for key in keys {
                    let key = key.to_lowercase();
                    if let Some(n) = self.keys.get_mut(&key) {
                        *n -= 1;
                        if *n == 0 {
                            self.keys.remove(&key);
                        }
                    }
                }
                self.set = Arc::new(self.keys.keys().cloned().collect());
            }
        }
    }

    fn snapshot(&self) -> Wanted {
        Wanted {
            all: self.all > 0,
            keys: Arc::clone(&self.set),
        }
    }
}

/// [`Want`] as a pass reads it.
struct Wanted {
    all: bool,
    keys: Arc<HashSet<String>>,
}

impl Wanted {
    fn wants(&self, row: &LogTail) -> bool {
        self.all
            || self.keys.contains(&row.task_id.to_lowercase())
            || self.keys.contains(&row.name.to_lowercase())
    }
}

/// A stream's place among a run's watchers: its receiver, and what it wants
/// read, given back when it is dropped.
pub(crate) struct Joined {
    pub(crate) rx: broadcast::Receiver<Arc<Pass>>,
    _guard: WantGuard,
}

struct WantGuard {
    relay: Arc<Relay>,
    run: String,
    epoch: u64,
    keys: Option<Vec<String>>,
}

impl Drop for WantGuard {
    fn drop(&mut self) {
        // Given back to the entry it was added to: one cleared (the relay
        // stopped) or let go and watched again is another's.
        if let Some(w) = self.relay.registry().runs.get_mut(&self.run) {
            if w.epoch == self.epoch {
                w.want.remove(&self.keys);
            }
        }
    }
}

/// Why a stream was not let watch: the relay watches as many runs as it may.
#[derive(Debug)]
pub(crate) struct Full;

/// Whether the writing a task row's log is will grow no more: its task is
/// over, or is not running and has a log of this writing (an attempt that
/// waits to be retried). A task that has not begun (pending, ready, waiting
/// for an approval, with no log) is not over: its attempt is to come.
pub(crate) fn over(row: &LogTail) -> bool {
    matches!(
        row.status.as_str(),
        "succeeded" | "failed" | "skipped" | "cancelled"
    ) || (row.status != "running" && (row.row_bytes.is_some() || row.segments > 0))
}

/// The relay's own state of a task of a watched run.
struct Tail {
    status: String,
    follow: Option<Follow>,
}

/// Where the relay has got to in a writing of a task's log: what it sent
/// (`sent`), and what it read past that and keeps until a line's end
/// (`carry`).
struct Follow {
    fence: i64,
    gen: i64,
    sent: u64,
    carry: Vec<u8>,
    carry_since: Option<Instant>,
    ended: bool,
}

impl Relay {
    pub fn new(settings: Settings) -> Relay {
        Relay {
            text: Arc::new(Semaphore::new(settings.text_bytes)),
            reads: Arc::new(Semaphore::new(settings.reads.max(1))),
            streams: Arc::new(Semaphore::new(settings.max_streams.max(1))),
            settings,
            reg: Mutex::new(Registry::default()),
            wake: Notify::new(),
            stop: CancellationToken::new(),
            running: AtomicBool::new(false),
        }
    }

    fn registry(&self) -> MutexGuard<'_, Registry> {
        self.reg.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How many runs are watched now.
    pub fn watched(&self) -> usize {
        self.registry().runs.len()
    }

    /// Stop: the relay makes no more passes, and every stream ends (its
    /// client goes on from its cursor, with another replica). What a shutdown
    /// does first.
    pub fn stop(&self) {
        self.stop.cancel();
    }

    /// Resolves once the relay is stopped.
    pub(crate) async fn stopped(&self) {
        self.stop.cancelled().await
    }

    /// A stream's place among those open ([`Settings::max_streams`]), held
    /// for as long as it is: `None` where as many are open as may be.
    pub(crate) fn admit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.streams).try_acquire_owned().ok()
    }

    /// Watch `run` for a stream that wants `keys` of its tasks (`None`: all
    /// of them). A run watched already is joined, whatever the cap; one more
    /// is watched if there is room, or if a run nobody follows any more can
    /// make it.
    pub(crate) fn join(
        self: &Arc<Self>,
        state: &AppState,
        run: &str,
        keys: Option<Vec<String>>,
    ) -> Result<Joined, Full> {
        self.ensure_running(state);
        let mut reg = self.registry();
        if !reg.runs.contains_key(run) {
            if reg.runs.len() >= self.settings.max_tails {
                let idle = reg
                    .runs
                    .iter()
                    .filter(|(_, w)| w.tx.receiver_count() == 0)
                    .min_by_key(|(_, w)| w.idle_since)
                    .map(|(id, _)| id.clone());
                match idle {
                    Some(id) => {
                        reg.runs.remove(&id);
                    }
                    None => return Err(Full),
                }
            }
            reg.epochs += 1;
            let epoch = reg.epochs;
            reg.runs.insert(
                run.to_string(),
                Watched {
                    tx: broadcast::Sender::new(self.settings.ring.max(1)),
                    want: Want::default(),
                    idle_since: None,
                    dirty: true,
                    epoch,
                },
            );
        }
        let w = reg.runs.get_mut(run).expect("inserted above");
        let rx = w.tx.subscribe();
        w.want.add(&keys);
        w.idle_since = None;
        w.dirty = true;
        let epoch = w.epoch;
        drop(reg);
        self.wake.notify_one();
        Ok(Joined {
            rx,
            _guard: WantGuard {
                relay: Arc::clone(self),
                run: run.to_string(),
                epoch,
                keys,
            },
        })
    }

    /// Start the relay's task where it is not running: at the first stream,
    /// and again after it stopped on a panic (its watchers heard their
    /// channels close, and join again).
    fn ensure_running(self: &Arc<Self>, state: &AppState) {
        if self.stop.is_cancelled() || self.running.swap(true, Ordering::AcqRel) {
            return;
        }
        let relay = Arc::clone(self);
        let state = state.clone();
        tokio::spawn(async move {
            let ran = AssertUnwindSafe(Arc::clone(&relay).run(state))
                .catch_unwind()
                .await;
            if ran.is_err() {
                tracing::error!("the log relay stopped on a panic: its streams end, and the next to open starts it again");
            }
            // Not running first: a stream that joins now starts another, and
            // the registry cleared after closes its channel if it was this
            // one's.
            relay.running.store(false, Ordering::Release);
            relay.registry().runs.clear();
        });
    }

    async fn run(self: Arc<Self>, state: AppState) {
        let mut events = state.tx.subscribe();
        let mut poll = tokio::time::interval(self.settings.poll);
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut tails: HashMap<String, HashMap<String, Tail>> = HashMap::new();
        let mut last: Option<Instant> = None;
        let mut more = false;
        loop {
            tokio::select! {
                biased;
                _ = self.stop.cancelled() => return,
                _ = self.wake.notified() => {}
                ev = events.recv() => match ev {
                    Ok(ev) => {
                        if !self.mark(&ev.run_id) {
                            continue;
                        }
                    }
                    Err(RecvError::Lagged(_)) => self.mark_all(),
                    Err(RecvError::Closed) => events = state.tx.subscribe(),
                },
                _ = poll.tick() => self.mark_all(),
                _ = std::future::ready(()), if more => {}
            }
            if let Some(last) = last {
                tokio::time::sleep_until((last + self.settings.min_gap).into()).await;
            }
            loop {
                match events.try_recv() {
                    Ok(ev) => {
                        self.mark(&ev.run_id);
                    }
                    Err(broadcast::error::TryRecvError::Lagged(_)) => self.mark_all(),
                    Err(_) => break,
                }
            }
            let work = self.take(&mut tails);
            if work.is_empty() {
                more = false;
                continue;
            }
            more = self.pass(&state, work, &mut tails).await;
            last = Some(Instant::now());
        }
    }

    fn mark(&self, run: &str) -> bool {
        match self.registry().runs.get_mut(run) {
            Some(w) => {
                w.dirty = true;
                true
            }
            None => false,
        }
    }

    fn mark_all(&self) {
        for w in self.registry().runs.values_mut() {
            w.dirty = true;
        }
    }

    /// The runs a pass is to read, and what their watchers want; the runs
    /// nobody has followed for the linger let go.
    fn take(&self, tails: &mut HashMap<String, HashMap<String, Tail>>) -> Vec<Work> {
        let now = Instant::now();
        let mut reg = self.registry();
        let linger = self.settings.linger;
        reg.runs.retain(|_, w| {
            if w.tx.receiver_count() > 0 {
                w.idle_since = None;
                return true;
            }
            let since = *w.idle_since.get_or_insert(now);
            now.duration_since(since) < linger
        });
        tails.retain(|run, _| reg.runs.contains_key(run));
        reg.runs
            .iter_mut()
            .filter(|(_, w)| w.dirty)
            .map(|(run, w)| {
                w.dirty = false;
                Work {
                    run: run.clone(),
                    tx: w.tx.clone(),
                    wanted: w.want.snapshot(),
                }
            })
            .collect()
    }

    /// One pass over `work`: the runs' tasks in one statement, then what each
    /// wanted task's log has past where the relay got to. Whether a run has
    /// more to read than one step took.
    async fn pass(
        &self,
        state: &AppState,
        work: Vec<Work>,
        tails: &mut HashMap<String, HashMap<String, Tail>>,
    ) -> bool {
        let runs: Vec<String> = work.iter().map(|w| w.run.clone()).collect();
        let rows = match db::log_tails(&state.read_pool, &runs).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(error = %e, "the log relay could not read the runs it watches");
                return false;
            }
        };
        let mut by_run: HashMap<&str, Vec<&LogTail>> = HashMap::new();
        for row in &rows {
            by_run.entry(row.run_id.as_str()).or_default().push(row);
        }
        let mut more = false;
        for w in work {
            let run_rows = by_run.remove(w.run.as_str()).unwrap_or_default();
            let run_tails = tails.entry(w.run.clone()).or_default();
            let present: HashSet<&str> = run_rows.iter().map(|r| r.task_id.as_str()).collect();
            run_tails.retain(|task, _| present.contains(task.as_str()));
            let mut pieces = Vec::new();
            let mut again = false;
            for row in run_rows {
                let tail = run_tails
                    .entry(row.task_id.clone())
                    .or_insert_with(|| Tail {
                        status: String::new(),
                        follow: None,
                    });
                if tail.status != row.status {
                    tail.status = row.status.clone();
                    pieces.push(piece(row, What::Status));
                }
                if !w.wanted.wants(row) {
                    tail.follow = None;
                    continue;
                }
                again |= self.follow(state, row, tail, &mut pieces).await;
            }
            if again {
                more = true;
                self.mark(&w.run);
            }
            if pieces.is_empty() {
                continue;
            }
            let bytes: usize = pieces
                .iter()
                .map(|p| match &p.what {
                    What::Text { text, .. } => text.len(),
                    _ => 0,
                })
                .sum();
            let permit = match bytes {
                0 => None,
                n => match Arc::clone(&self.text)
                    .try_acquire_many_owned(n.min(u32::MAX as usize) as u32)
                {
                    Ok(permit) => Some(permit),
                    Err(_) => {
                        // The text budget is spent: where the logs got to goes
                        // instead, and each watcher reads the text itself.
                        for p in &mut pieces {
                            if let What::Text { end, .. } = p.what {
                                p.what = What::Grew { end };
                            }
                        }
                        None
                    }
                },
            };
            let _ = w.tx.send(Arc::new(Pass {
                pieces,
                _text: permit,
            }));
        }
        more
    }

    /// What a pass reads of one wanted task: where its log is, for one the
    /// relay starts on or that is another writing now, or what it has past
    /// where the relay got to. Whether it has more than one step took.
    async fn follow(
        &self,
        state: &AppState,
        row: &LogTail,
        tail: &mut Tail,
        pieces: &mut Vec<Piece>,
    ) -> bool {
        let store = state.log_store.as_deref();
        let over = over(row);
        let same = tail
            .follow
            .as_ref()
            .is_some_and(|f| (f.fence, f.gen) == (row.fence, row.gen));
        if !same {
            // Started on, or another writing: from where its log is now. What
            // was before is each watcher's to read.
            let layout = match Layout::of(store, row).await {
                Ok(Some(layout)) => layout,
                Ok(None) => return true,
                Err(e) => {
                    tracing::warn!(task_id = %row.task_id, error = %format!("{e:#}"), "the log relay could not read a task's log");
                    return false;
                }
            };
            // Another attempt, while the relay was in the middle of one before
            // that had a log: that one is over, and not all of it was read
            // here. (A task that had not begun had no attempt to end.)
            let logged = |f: &&Follow| f.sent > 0 || !f.carry.is_empty();
            if let Some(old) = tail
                .follow
                .as_ref()
                .filter(|f| !f.ended && f.fence != row.fence)
                .filter(logged)
            {
                pieces.push(Piece {
                    fence: old.fence,
                    gen: old.gen,
                    ..piece(
                        row,
                        What::Ended {
                            end: old.sent,
                            complete: false,
                        },
                    )
                });
            }
            let end = layout.total();
            // From the last line's start, a line not yet ended kept as one
            // that was read: what is sent begins at a line, and so does a
            // watcher's reading of what is before it.
            let (sent, carry) = match over {
                true => (end, Vec::new()),
                false => {
                    let from = end.saturating_sub(self.settings.line_max as u64);
                    match layout
                        .read(&state.read_pool, store, row, from, (end - from) as usize)
                        .await
                    {
                        Ok(Got::Bytes(bytes)) => {
                            let at = match bytes.iter().rposition(|b| *b == b'\n') {
                                Some(at) => at + 1,
                                None if from == 0 => 0,
                                // A line longer than the most kept waiting:
                                // from where it is now, at a character.
                                None => cut(&bytes, true),
                            };
                            (from + at as u64, bytes[at..].to_vec())
                        }
                        Ok(_) => return true,
                        Err(e) => {
                            tracing::warn!(task_id = %row.task_id, error = %format!("{e:#}"), "the log relay could not read a task's log");
                            return false;
                        }
                    }
                }
            };
            tail.follow = Some(Follow {
                fence: row.fence,
                gen: row.gen,
                sent,
                carry_since: (!carry.is_empty()).then(Instant::now),
                carry,
                ended: over,
            });
            pieces.push(piece(row, What::Began { end: sent }));
            if over {
                pieces.push(piece(
                    row,
                    What::Ended {
                        end,
                        complete: true,
                    },
                ));
            }
            return false;
        }
        let f = tail.follow.as_mut().expect("the same writing");
        if f.ended {
            return false;
        }
        let layout = match Layout::of(store, row).await {
            Ok(Some(layout)) => layout,
            // Moved on between the statement and the store: the next pass.
            Ok(None) => return true,
            Err(e) => {
                tracing::warn!(task_id = %row.task_id, error = %format!("{e:#}"), "the log relay could not read a task's log");
                return false;
            }
        };
        let total = layout.total();
        let from = f.sent + f.carry.len() as u64;
        let mut gone = false;
        let mut more = false;
        if total > from {
            match layout
                .read(&state.read_pool, store, row, from, self.settings.step)
                .await
            {
                Ok(Got::Bytes(bytes)) => {
                    if f.carry.is_empty() && !bytes.is_empty() {
                        f.carry_since = Some(Instant::now());
                    }
                    f.carry.extend_from_slice(&bytes);
                    more = f.sent + (f.carry.len() as u64) < total;
                }
                Ok(Got::Gone) => gone = true,
                Ok(Got::Moved) => return true,
                Err(e) => {
                    tracing::warn!(task_id = %row.task_id, error = %format!("{e:#}"), "the log relay could not read a task's log");
                    return false;
                }
            }
        }
        let finished = over && (gone || !more);
        // A line not yet ended is sent as it is when it is long, or has
        // waited: not because much else came with it.
        let force = finished
            || f.carry.len() - cut(&f.carry, false) >= self.settings.line_max
            || f.carry_since
                .is_some_and(|since| since.elapsed() >= self.settings.carry_wait);
        let n = cut(&f.carry, force);
        if n > 0 {
            let start = f.sent;
            let end = start + n as u64;
            let text: Arc<str> = String::from_utf8_lossy(&f.carry[..n]).into();
            f.carry.drain(..n);
            f.carry_since = (!f.carry.is_empty()).then(Instant::now);
            f.sent = end;
            pieces.push(piece(row, What::Text { start, end, text }));
        }
        if finished {
            f.ended = true;
            pieces.push(piece(
                row,
                What::Ended {
                    end: f.sent,
                    complete: !gone,
                },
            ));
        }
        more
    }
}

/// A run a pass is to read.
struct Work {
    run: String,
    tx: broadcast::Sender<Arc<Pass>>,
    wanted: Wanted,
}

fn piece(row: &LogTail, what: What) -> Piece {
    Piece {
        task_id: row.task_id.as_str().into(),
        name: row.name.as_str().into(),
        attempt: row.attempt,
        fence: row.fence,
        gen: row.gen,
        status: row.status.as_str().into(),
        what,
    }
}

/// How many of `bytes` go now: up to the last line's end; or, `force`d (a
/// line too long, or too long waiting, or the attempt over), all of them but
/// a character the next bytes finish.
pub(crate) fn cut(bytes: &[u8], force: bool) -> usize {
    if force {
        return match std::str::from_utf8(bytes) {
            Ok(_) => bytes.len(),
            Err(e) if e.error_len().is_none() => e.valid_up_to(),
            // Bytes that are no text (none are written): taken as they are.
            Err(_) => bytes.len(),
        };
    }
    bytes
        .iter()
        .rposition(|b| *b == b'\n')
        .map_or(0, |at| at + 1)
}

/// Where a writing of a task's log is, as one statement and the store found
/// it: its closed segments' bytes, its open file's (a `file://` store's), and
/// its row part's; or the row's `output`, which a view shows of a task that
/// kept no log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Layout {
    pub(crate) closed: u64,
    pub(crate) segments: i64,
    pub(crate) open: u64,
    pub(crate) row: u64,
    pub(crate) output: bool,
}

/// What a read of a log found.
#[derive(Debug)]
pub(crate) enum Got {
    /// Bytes from where it was asked, as many as were there up to its most:
    /// they may end inside a character.
    Bytes(Vec<u8>),
    /// The writing is gone: the row's log is another's now.
    Gone,
    /// The log moved on between the statement and the store (a segment
    /// closed): read again from a new statement.
    Moved,
}

impl Layout {
    pub(crate) fn total(&self) -> u64 {
        self.closed + self.open + self.row
    }

    /// Where `row`'s log is: `None` where it moved on between the statement
    /// and the store (an open part counted and gone).
    pub(crate) async fn of(
        store: Option<&SegmentStore>,
        row: &LogTail,
    ) -> anyhow::Result<Option<Layout>> {
        let over = matches!(
            row.status.as_str(),
            "succeeded" | "failed" | "skipped" | "cancelled"
        );
        let row_bytes = row.row_bytes.unwrap_or(0).max(0) as u64;
        // A log kept nowhere but its row, or not at all: its row's `log`, or,
        // for a task that is over and kept none, its `output`, as the views
        // show it (never a running attempt's, whose `output` is the last
        // attempt's).
        let in_row = || {
            if row_bytes == 0 && over && row.output_bytes > 0 {
                Layout {
                    closed: 0,
                    segments: 0,
                    open: 0,
                    row: row.output_bytes as u64,
                    output: true,
                }
            } else {
                Layout {
                    closed: 0,
                    segments: 0,
                    open: 0,
                    row: row_bytes,
                    output: false,
                }
            }
        };
        let Some(store) = store else {
            return Ok(Some(in_row()));
        };
        let closed = row.closed.max(0) as u64;
        if store.open_in_row() {
            if row.segments == 0 {
                return Ok(Some(in_row()));
            }
            return Ok(Some(Layout {
                closed,
                segments: row.segments,
                open: 0,
                row: row_bytes,
                output: false,
            }));
        }
        let key = LogKey::new(row.task_id.as_str(), row.fence, row.gen);
        match store.open_size(&key, row.segments).await? {
            Some(open) => Ok(Some(Layout {
                closed,
                segments: row.segments,
                open,
                row: row_bytes,
                output: false,
            })),
            None if row.segments == 0 => Ok(Some(in_row())),
            None => Ok(None),
        }
    }

    /// At most `max` bytes of `row`'s log from byte `from`.
    pub(crate) async fn read(
        &self,
        pool: &crate::db::Pool,
        store: Option<&SegmentStore>,
        row: &LogTail,
        from: u64,
        max: usize,
    ) -> anyhow::Result<Got> {
        if self.output {
            return Ok(
                match db::log_output_bytes(pool, &row.task_id, from as i64, max as i64).await? {
                    Some(bytes) => Got::Bytes(bytes),
                    None => Got::Gone,
                },
            );
        }
        let key = LogKey::new(row.task_id.as_str(), row.fence, row.gen);
        let mut out: Vec<u8> = Vec::new();
        let mut at = from;
        if at < self.closed {
            let Some(store) = store else {
                return Ok(Got::Moved);
            };
            let spans = db::log_segment_spans(pool, &row.task_id, row.fence).await?;
            // Not the segments the statement counted: closed since, or begun
            // again.
            if spans
                .iter()
                .map(|(_, bytes)| (*bytes).max(0) as u64)
                .sum::<u64>()
                != self.closed
            {
                return Ok(Got::Moved);
            }
            let mut start = 0u64;
            for (seq, bytes) in spans {
                let len = bytes.max(0) as u64;
                if at >= start && at < start + len && out.len() < max {
                    let want = (max - out.len()).min((start + len - at) as usize);
                    match store
                        .read_segment_from(&key.segment(seq), at - start, want)
                        .await
                    {
                        Ok(got) if got.len() == want => {
                            at += want as u64;
                            out.extend_from_slice(&got);
                        }
                        // Gone under its name, or shorter than its row says:
                        // the writing was replaced.
                        _ => {
                            return Ok(if out.is_empty() {
                                Got::Moved
                            } else {
                                Got::Bytes(out)
                            })
                        }
                    }
                }
                start += len;
            }
        }
        if out.len() >= max {
            return Ok(Got::Bytes(out));
        }
        if self.open > 0 && at < self.closed + self.open {
            let Some(store) = store else {
                return Ok(Got::Moved);
            };
            // No further than the size the statement's stat found: what is
            // written after it is the next read's, after which the row's part
            // (a file:// store's notes) is placed.
            let want = (max - out.len()).min((self.closed + self.open - at) as usize);
            match store
                .read_open_from(&key, self.segments, at - self.closed, want)
                .await?
            {
                Some(got) => {
                    at += got.len() as u64;
                    out.extend_from_slice(&got);
                }
                None => {
                    return Ok(if out.is_empty() {
                        Got::Moved
                    } else {
                        Got::Bytes(out)
                    })
                }
            }
        }
        if out.len() >= max || at < self.closed + self.open || self.row == 0 && at >= self.total() {
            return Ok(Got::Bytes(out));
        }
        let read = RowRead {
            task_id: row.task_id.clone(),
            fence: row.fence,
            gen: row.gen,
            pos: at as i64,
            base: self.open as i64,
            max: (max - out.len()) as i64,
        };
        match db::log_row_bytes(pool, std::slice::from_ref(&read))
            .await?
            .pop()
        {
            Some(got) if got.closed as u64 == self.closed => {
                out.extend_from_slice(&got.bytes);
                Ok(Got::Bytes(out))
            }
            // A segment closed since: what was read stands, the rest the next
            // statement's.
            Some(_) => Ok(if out.is_empty() {
                Got::Moved
            } else {
                Got::Bytes(out)
            }),
            None => Ok(if out.is_empty() {
                Got::Gone
            } else {
                Got::Bytes(out)
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_goes_now_ends_at_a_line_or_a_character() {
        assert_eq!(cut(b"one\ntwo\nthr", false), 8);
        assert_eq!(cut(b"no end yet", false), 0);
        assert_eq!(cut(b"no end yet", true), 10);
        let e = "é".as_bytes();
        assert_eq!(
            cut(&[b'a', e[0]], true),
            1,
            "a character the next bytes finish waits"
        );
        assert_eq!(cut(b"", true), 0);
    }

    #[test]
    fn the_relay_is_set_from_its_knobs_and_refuses_a_bad_one() {
        let set = |pairs: &'static [(&'static str, &'static str)]| {
            Settings::parse(move |k| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            })
        };
        let s = set(&[
            ("DAGRON_LOG_MAX_TAILS", "3"),
            ("DAGRON_LOG_STREAM_POLL_MS", "200"),
        ])
        .unwrap();
        assert_eq!((s.max_tails, s.poll), (3, Duration::from_millis(200)));
        assert_eq!(s.linger, Settings::default().linger);
        assert!(set(&[("DAGRON_LOG_MAX_TAILS", "0")]).is_err());
        assert!(set(&[("DAGRON_LOG_STREAM_POLL_MS", "often")]).is_err());
        assert!(set(&[("DAGRON_LOG_RELAY_BYTES", "10")]).is_err());
        assert_eq!(
            set(&[("DAGRON_LOG_MAX_STREAMS", "7")]).unwrap().max_streams,
            7
        );
    }
}
