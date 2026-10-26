use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{timeout, Duration};

// ── Shared types ─────────────────────────────────────────────────────────────

/// An image a run's task will want ([`Executor::prefetch`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunImage {
    /// `None`: the executor's default image.
    pub image: Option<String>,
    pub policy: Option<dagron_core::dag::ImagePullPolicy>,
}

/// Output returned by every executor backend.
#[derive(Debug)]
pub struct ExecOutput {
    pub success: bool,
    pub output: String,
    /// How many bytes of the output's beginning the executor did not keep:
    /// `output` is its end ([`cap_output_after`]).
    pub dropped: usize,
}

/// Marker error for a task killed by its `timeout_secs` deadline — as opposed to
/// a non-zero exit or a spawn/backend error. Every executor backend returns this
/// (wrapped in `anyhow`) when it aborts a task at the deadline, so the worker can
/// `downcast` and tell the reconcile loop the failure was a timeout. That lets a
/// task with `retry_on_timeout: false` (fast-win #24) skip the retry a deadline
/// kill would otherwise burn (such kills usually recur). Carries the deadline for
/// the message.
#[derive(Debug)]
pub struct TimeoutError {
    pub secs: u64,
}

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "command timed out after {}s", self.secs)
    }
}

impl std::error::Error for TimeoutError {}

/// Marker error for an execution that was told its claim on the task is gone
/// ([`Stop`]) and stopped: a cancel, a run past its deadline, a lease another
/// scheduler took over. A backend returns it once it has stopped and removed
/// what this attempt made, and has seen it gone; one that could not see that
/// returns [`ClaimLost::unseen`]. It is no verdict on the task, and its result
/// is never applied: the claim it would be applied under is the one that is
/// gone.
#[derive(Debug)]
pub struct ClaimLost;

impl ClaimLost {
    /// The error of an attempt whose claim was lost and whose teardown was
    /// not seen through: a [`ClaimLost`] that reads as [`TeardownUnseen`].
    /// `is` and `downcast_ref` find both.
    pub fn unseen(why: impl Into<String>) -> anyhow::Error {
        anyhow::Error::new(ClaimLost).context(TeardownUnseen(why.into()))
    }
}

/// Beside [`ClaimLost`] on an attempt whose teardown nobody saw finish: the
/// stop or the removal failed, or was not answered in time. What the attempt
/// started may still be running, and its result must not say it was stopped.
///
/// What keeps the task's next attempt from running beside such a leftover is
/// that attempt's own dispatch, which starts nothing until it has seen it
/// gone.
#[derive(Debug)]
pub struct TeardownUnseen(pub String);

impl std::fmt::Display for TeardownUnseen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the task's claim was lost, and what this attempt started may still be running: {}",
            self.0
        )
    }
}

impl std::fmt::Display for ClaimLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the task's claim was lost; what this attempt started was stopped and removed")
    }
}

impl std::error::Error for ClaimLost {}

/// Tells a running execution that its claim on the task is gone, so that it
/// stops the work instead of only ceasing to wait for it.
///
/// Dropping an execution's future ends a local child (`kill_on_drop`) and
/// nothing else: a container or a pod is another machine's process, and went
/// on running after a cancel, with nobody waiting for it. So the worker asks
/// here first, and each backend stops and removes what **this attempt** made,
/// by the names it made them under. Never by the task's labels, which a later
/// attempt's workload carries too.
///
/// Cheap to clone; every clone is the one signal. Never asked: never resolves.
#[derive(Clone, Debug, Default)]
pub struct Stop(Arc<StopState>);

#[derive(Debug, Default)]
struct StopState {
    asked: AtomicBool,
    waiters: tokio::sync::Notify,
}

impl Stop {
    /// Ask the execution to stop. Asking twice is asking once.
    pub fn request(&self) {
        self.0.asked.store(true, Ordering::SeqCst);
        self.0.waiters.notify_waiters();
    }

    pub fn is_requested(&self) -> bool {
        self.0.asked.load(Ordering::SeqCst)
    }

    /// Resolves once a stop has been asked for, however long ago.
    pub async fn requested(&self) {
        loop {
            // Made before the flag is read: a `Notified` hears every
            // `notify_waiters` from its creation on, so a request that lands
            // between the read and the await is not missed.
            let notified = self.0.waiters.notified();
            if self.is_requested() {
                return;
            }
            notified.await;
        }
    }
}

/// Marker error for a dispatch the backend could not start for lack of capacity
/// — as opposed to one that started and failed. The worker `downcast`s it the
/// way it does [`TimeoutError`] and reports it as `TaskResult::backpressure`, so
/// the reconcile loop can put the task back in the queue with its attempt given
/// back instead of counting a failure.
///
/// **A backend may return this only when it created no workload for this
/// dispatch.** The give-back rests on it: the task goes back to the queue as
/// if nothing had started, so a workload left behind by a "refused" dispatch
/// runs with nothing waiting on it until the task's next dispatch reaps it
/// (its fence is the lower: [`is_predecessor`]).
///
/// The type has to reach the worker intact — re-wrapped with `anyhow!("{e}")`
/// it is an ordinary failure. Carries the reason for the message.
#[derive(Debug)]
pub struct Backpressure {
    pub reason: String,
}

impl std::fmt::Display for Backpressure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "not started: {}", self.reason)
    }
}

impl std::error::Error for Backpressure {}

/// One incremental chunk of a running task's output, streamed for live tailing
/// (fast-win #17). The executor emits these through a [`LogSink`] as output
/// arrives; the reconcile loop appends them to the task's stored output so the
/// UI/API can tail it before the task exits. `first` marks the first chunk of an
/// attempt so the loop resets any prior-attempt output before appending.
pub struct LogChunk {
    pub task_id: String,
    pub fence: i64,
    pub chunk: String,
    pub first: bool,
}

/// The setting that caps what is kept of one attempt of a task: its log, and
/// its output. In bytes.
pub const ENV_LOG_TASK_CAP: &str = "DAGRON_LOG_TASK_CAP_BYTES";

/// What is kept where [`ENV_LOG_TASK_CAP`] does not say: 64 MiB.
pub const DEFAULT_LOG_TASK_CAP: usize = 64 * 1024 * 1024;

/// The setting that says how much of the end of a log past its cap is kept,
/// on top of the cap. In bytes, as the task wrote them; `0`, none.
pub const ENV_LOG_TAIL: &str = "DAGRON_LOG_TAIL_BYTES";

/// How long the end of a task's output is waited for once its container or
/// pod has stopped, where the output is followed as the task writes it.
/// Docker ends a container's attachment within moments of its end, and the
/// kubelet a follow of a pod's log within a second or two: this bounds a
/// daemon or an API server that does not.
pub(crate) const FOLLOW_DRAIN: Duration = Duration::from_secs(30);

/// The most that is kept of one attempt's log, and of its output:
/// [`ENV_LOG_TASK_CAP`], read once.
///
/// Both are rows of the scheduling database, and nothing bounded either: a
/// task that wrote without end filled the disk the scheduler runs on. Past the
/// cap a log keeps its beginning, and its end once it ends
/// ([`log_tail_bytes`]), with a line that says what was left out between; an
/// output keeps its end, which is what a result is, and says what it lost.
pub fn log_task_cap() -> usize {
    static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAP.get_or_init(|| parse_log_cap(std::env::var(ENV_LOG_TASK_CAP).ok().as_deref()))
}

/// [`ENV_LOG_TASK_CAP`] as a number of bytes. One that is not a positive
/// integer is said once and is not a cap of nothing: `0` would keep no log at
/// all, of any task.
fn parse_log_cap(raw: Option<&str>) -> usize {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return DEFAULT_LOG_TASK_CAP;
    };
    match raw.parse::<usize>() {
        Ok(cap) if cap > 0 => cap,
        _ => {
            tracing::warn!(
                value = %raw,
                "{ENV_LOG_TASK_CAP} is not a positive number of bytes; a task's log is kept up to \
                 the default, {}",
                human_bytes(DEFAULT_LOG_TASK_CAP)
            );
            DEFAULT_LOG_TASK_CAP
        }
    }
}

/// How much of the end of a log past its cap of `cap` is kept: what
/// [`ENV_LOG_TAIL`] says, read once, and no more than the cap; or, where it
/// says nothing, a quarter of the cap. It is on top of the cap, so a log of
/// a task that writes without end is at most its cap and this, and the lines
/// dagron adds; `0` keeps its beginning alone, as a log was kept before.
pub fn log_tail_bytes(cap: usize) -> usize {
    tail_of(log_tail_setting(), cap)
}

/// [`log_tail_bytes`] where [`ENV_LOG_TAIL`] says `setting`.
fn tail_of(setting: Option<usize>, cap: usize) -> usize {
    match setting {
        None => cap / 4,
        Some(tail) => {
            if tail > cap {
                static SAID: std::sync::Once = std::sync::Once::new();
                SAID.call_once(|| {
                    tracing::warn!(
                        value = tail,
                        cap,
                        "{ENV_LOG_TAIL} is more than {ENV_LOG_TASK_CAP}: a log keeps at most its \
                         cap of its end"
                    );
                });
            }
            tail.min(cap)
        }
    }
}

/// [`ENV_LOG_TAIL`], read once.
fn log_tail_setting() -> Option<usize> {
    static TAIL: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    *TAIL.get_or_init(|| parse_log_tail(std::env::var(ENV_LOG_TAIL).ok().as_deref()))
}

/// [`ENV_LOG_TAIL`] as a number of bytes, `0` among them; `None` where it
/// says nothing. One that is not a whole number is said once, and is the
/// default.
fn parse_log_tail(raw: Option<&str>) -> Option<usize> {
    let raw = raw.map(str::trim).filter(|raw| !raw.is_empty())?;
    match raw.parse::<usize>() {
        Ok(tail) => Some(tail),
        Err(_) => {
            tracing::warn!(
                value = %raw,
                "{ENV_LOG_TAIL} is not a whole number of bytes; a log past its cap keeps the \
                 default of its end, a quarter of its cap"
            );
            None
        }
    }
}

/// `n` bytes as a person reads them: `64 MiB`, `1.5 KiB`, `12 bytes`.
pub fn human_bytes(n: usize) -> String {
    const UNITS: [(usize, &str); 3] = [(1 << 30, "GiB"), (1 << 20, "MiB"), (1 << 10, "KiB")];
    for (size, unit) in UNITS {
        if n >= size {
            let whole = n / size;
            let tenths = (n % size) * 10 / size;
            return match tenths {
                0 => format!("{whole} {unit}"),
                _ => format!("{whole}.{tenths} {unit}"),
            };
        }
    }
    format!("{n} bytes")
}

/// The line a log says it reached its cap on, while the task runs: what the
/// task writes after it is held back, and its last `tail` bytes added when
/// it ends. With no tail, the line a capped log ends on.
fn log_capped_notice(cap: usize, tail: usize) -> String {
    match tail {
        0 => format!(
            "[dagron] this attempt's log reached {} ({ENV_LOG_TASK_CAP}) and stops here\n",
            human_bytes(cap)
        ),
        tail => format!(
            "[dagron] this attempt's log reached {} ({ENV_LOG_TASK_CAP}): what the task writes \
             from here is held back, and the last {} of it ({ENV_LOG_TAIL}) is added below when \
             it ends\n",
            human_bytes(cap),
            human_bytes(tail)
        ),
    }
}

/// The line that says a log stops before its cap: what the task wrote,
/// counted before it is masked, is past it by more than what an executor
/// keeps past it for the log's seal ([`LogSink::raw_cap`]).
fn log_stopped_notice(cap: usize, tail: usize) -> String {
    match tail {
        0 => format!(
            "[dagron] this attempt's output, counted before masking, passed {} \
             ({ENV_LOG_TASK_CAP}), and its log stops here\n",
            human_bytes(cap)
        ),
        tail => format!(
            "[dagron] this attempt's output, counted before masking, passed {} \
             ({ENV_LOG_TASK_CAP}): what the task writes from here is held back, and the last {} \
             of it ({ENV_LOG_TAIL}) is added below when it ends\n",
            human_bytes(cap),
            human_bytes(tail)
        ),
    }
}

/// What follows the line that says what of a log past its cap was left out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gap {
    /// The log's end, after what was left out.
    End,
    /// The rest of it: nothing was left out.
    Rest,
    /// Nothing: what was left out runs to the end, the end of a secret
    /// among it.
    ToEnd,
    /// Nothing: the task wrote nothing past where the log stopped.
    NoMore,
    /// Nothing: the attempt's end was not kept for the log, since no
    /// executor sealed it, or what it kept of it did not add up.
    Unkept,
}

/// The line, in the log of an attempt past its cap, that says how much of
/// what the task wrote was left out of it there: `omitted` bytes and `lines`
/// lines, counted as the task wrote them. Of one form whatever follows, for
/// what reads it:
/// `^\[dagron\] (none|.+) of this attempt's output \(([0-9]+) bytes and ([0-9]+) lines, counted before masking\) is left out here`.
fn gap_line(omitted: usize, lines: usize, gap: Gap) -> String {
    let how = match gap {
        Gap::End => "; what follows is its end",
        Gap::Rest => "; what follows is the rest of it",
        Gap::ToEnd => ", to its end",
        Gap::NoMore => ", and it wrote no more",
        Gap::Unkept => ", and its end was not kept",
    };
    let said = match omitted {
        0 => "none".to_string(),
        n => human_bytes(n),
    };
    format!(
        "[dagron] {said} of this attempt's output ({omitted} bytes and {lines} lines, counted \
         before masking) is left out here{how}\n"
    )
}

/// How long `text` is as a database keeps it ([`without_nul`]): a NUL is
/// three bytes there.
fn stored_len(text: &str) -> usize {
    text.len() + 2 * text.bytes().filter(|&b| b == 0).count()
}

/// How many lines end in `text`.
fn newlines(text: &str) -> usize {
    text.bytes().filter(|&b| b == b'\n').count()
}

/// Where a piece of a task's output that would take its log past its cap is
/// cut: the most of it, clear of a secret ([`Redactor::clear_cut`]) and on
/// a character's edge, that is no more than `room` bytes once masked as the
/// log keeps it. In the task's own bytes, so that what the log took of what
/// the task wrote is known to the byte.
///
/// [`Redactor::clear_cut`]: crate::redact::Redactor::clear_cut
fn head_cut(redactor: &crate::redact::Redactor, piece: &str, room: usize) -> usize {
    if let std::borrow::Cow::Borrowed(_) = redactor.redact(piece) {
        // No secret in it: as long masked as it is, a NUL three bytes.
        let mut len = 0;
        for (at, c) in piece.char_indices() {
            len += if c == '\0' { 3 } else { c.len_utf8() };
            if len > room {
                return at;
            }
        }
        return piece.len();
    }
    let fits = |cut: usize| stored_len(&redactor.redact(&piece[..cut])) <= room;
    // `lo` fits, `hi` does not; halved between, each cut clear of a secret.
    let (mut lo, mut hi) = (0, piece.len());
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if fits(redactor.clear_cut(piece, mid)) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    redactor.clear_cut(piece, lo)
}

/// Where `text`, the rest of what a task wrote from some way into it, can
/// begin to be masked at `at` or past it: past what of it could be the end
/// of a secret begun before it ([`Redactor::reach`]), and past a secret
/// that would be cut in two there.
///
/// [`Redactor::reach`]: crate::redact::Redactor::reach
fn clear_start(redactor: &crate::redact::Redactor, text: &str, at: usize) -> usize {
    redactor.clear_cut(text, redactor.reach(text).max(at))
}

/// The greatest index at or below `at` that does not split a character of `s`.
fn char_floor(s: &str, at: usize) -> usize {
    let mut at = at.min(s.len());
    while !s.is_char_boundary(at) {
        at -= 1;
    }
    at
}

/// `text` as a database keeps it: with no NUL. Postgres takes none in a text
/// value and refuses the whole statement that holds one, so one NUL in what a
/// task wrote cost it the append its log went in, and failed the statement
/// that recorded its result, on which the engine exited. Each is kept as
/// U+FFFD, which is what a byte that is no text already is by here.
pub fn without_nul(text: String) -> String {
    if text.contains('\0') {
        text.replace('\0', "\u{FFFD}")
    } else {
        text
    }
}

/// `output`, or its end where it is over `cap` bytes, under a line saying
/// how much of its beginning is not kept. The end is what a result is: a
/// later task's `when:`, a run's `result_from`, a deferred job's handle. One
/// that depended on more reads the line first, and fails in sight.
pub fn cap_output(output: String, cap: usize) -> String {
    cap_output_after(output, cap, 0)
}

/// [`cap_output`] of an output whose first `dropped` bytes its executor did
/// not keep, `output` being the rest: the line says all that was lost.
pub fn cap_output_after(output: String, cap: usize, dropped: usize) -> String {
    if output.len() <= cap && dropped == 0 {
        return output;
    }
    let mut from = output.len().saturating_sub(cap);
    while !output.is_char_boundary(from) {
        from += 1;
    }
    format!(
        "[dagron] this attempt's output is over {} ({ENV_LOG_TASK_CAP}): its first {} are not \
         kept\n{}",
        human_bytes(cap),
        human_bytes(dropped + from),
        &output[from..]
    )
}

/// How much of a task's output past where its log stopped is kept for the
/// log's seal, at least: a secret its stop cut in two is then whole in it,
/// and the seal masks it whole. No less than the longest secret masked.
const SEAL_PAST_CAP: usize = 64 * 1024;

/// How much more of the end of a task's output than an output keeps is kept
/// of it, at least: where what is kept begins may cut a secret in two, which
/// masking would miss, and it is masked from past that ([`masked_end`]). No
/// less than the longest secret masked, so that one whose end is in what an
/// output keeps begins in what was kept.
const OUTPUT_PAST_CAP: usize = 64 * 1024;

/// What was kept of a task's output, masked, and how much of its beginning
/// is then not kept: an end whose beginning was dropped may begin inside a
/// secret cut in two there, which masking would miss, and masks may shorten
/// it so much that cutting it to the cap does not cut that part off. It is
/// masked from past what of it could be the end of a secret begun before
/// it ([`Redactor::reach`](crate::redact::Redactor::reach)), and past a
/// secret that would cut in two.
pub fn masked_end(
    redactor: &crate::redact::Redactor,
    output: &str,
    dropped: usize,
) -> (String, usize) {
    let skip = match dropped {
        0 => 0,
        _ => clear_start(redactor, output, 0),
    };
    (
        redactor.redact(&output[skip..]).into_owned(),
        dropped + skip,
    )
}

/// What an executor keeps of a task's output as it reads it, each piece
/// sent to the task's log as it is kept: its beginning, as far as the log
/// keeps one, for the log's seal; and its end, as far as an output keeps
/// one, for its output, and for the end of a log past its cap, which is no
/// more than its cap ([`LogSink::tail`]). Not all of it, which a task that
/// writes without end makes as long as it likes, in the engine's memory.
pub(crate) struct Gathered<'a> {
    sink: Option<&'a LogSink>,
    cap: usize,
    /// How much is kept past the log's cap for its seal, and past an
    /// output's for it ([`SEAL_PAST_CAP`], [`OUTPUT_PAST_CAP`]).
    seal_past: usize,
    output_past: usize,
    head: String,
    /// How much of the head came after the log stopped.
    past_cap: usize,
    tail: String,
    /// How much of the output's beginning is no longer in `tail`.
    dropped: usize,
}

impl<'a> Gathered<'a> {
    pub(crate) fn new(sink: Option<&'a LogSink>) -> Self {
        let longest = sink.map_or(0, |sink| sink.redactor.longest());
        Self {
            sink,
            cap: sink.map_or_else(log_task_cap, |sink| sink.cap),
            seal_past: SEAL_PAST_CAP.max(longest),
            output_past: OUTPUT_PAST_CAP.max(longest),
            head: String::new(),
            past_cap: 0,
            tail: String::new(),
            dropped: 0,
        }
    }

    /// A piece of the task's output, of whole lines where it has them.
    pub(crate) fn push(&mut self, piece: &str) {
        if let Some(sink) = self.sink {
            let capped = sink.is_capped();
            sink.append(piece);
            if !capped {
                self.head.push_str(piece);
            } else if self.past_cap < self.seal_past {
                let end = char_floor(piece, self.seal_past - self.past_cap);
                self.head.push_str(&piece[..end]);
                self.past_cap = if end < piece.len() {
                    self.seal_past
                } else {
                    self.past_cap + end
                };
            }
        }
        self.tail.push_str(piece);
        let most = self.cap + self.output_past;
        if self.tail.len() > 2 * most {
            let mut from = self.tail.len() - most;
            while !self.tail.is_char_boundary(from) {
                from += 1;
            }
            self.tail.drain(..from);
            self.dropped += from;
        }
    }

    /// The log sealed with what was kept of its beginning, and of its end:
    /// `tail` holds at least the cap's worth of the output's last bytes, so
    /// all of the end a log past its cap keeps.
    pub(crate) fn seal(&self) {
        if let Some(sink) = self.sink {
            sink.seal_with(&self.head, &self.tail, self.dropped);
        }
    }

    /// What was kept of the output's end, and how much of its beginning was
    /// not.
    pub(crate) fn output(self) -> (String, usize) {
        (self.tail, self.dropped)
    }
}

/// How much of an attempt's log the sink has sent, whether what it sent last
/// stopped in the middle of a line, and whether it has stopped for good; and
/// what it sent was, as a hash and a length (its notes are not in it), which
/// a seal that would write it all again is known by. And how much the task
/// wrote, past the cap too, which what a log past its cap left out is
/// counted from.
#[derive(Default)]
struct Kept {
    bytes: usize,
    mid_line: bool,
    capped: bool,
    /// How much of the task's output, as it wrote it, the log took, to the
    /// byte; and that much, where it stopped there because what it wrote was
    /// past what it takes ([`LogSink::raw_cap`]), not at its cap.
    raw: usize,
    stopped_at: Option<usize>,
    sent: Option<std::hash::DefaultHasher>,
    sent_bytes: usize,
    /// Every byte the task wrote, and every line it ended, past the cap too.
    total_raw: usize,
    total_lines: usize,
    /// The lines ended in what the log took (`raw`).
    head_lines: usize,
    /// Sealed or closed: the attempt is over, and the log takes notes alone.
    ended: bool,
    /// What a log past its cap left out, in bytes and lines as the task
    /// wrote them, once its attempt is over.
    omitted: Option<(usize, usize)>,
}

/// What of `text` an attempt's log takes, `kept` being what it took so far
/// (which this counts on): all of it; or its beginning and the line that
/// says the log reached its cap there; or nothing, once it has stopped.
fn within_cap(cap: usize, tail: usize, kept: &mut Kept, text: String) -> Option<String> {
    if kept.capped {
        return None;
    }
    let room = cap - kept.bytes.min(cap);
    if text.len() <= room {
        kept.bytes += text.len();
        kept.mid_line = !text.ends_with('\n');
        return Some(text);
    }
    kept.capped = true;
    let cut = char_floor(&text, room);
    kept.bytes += cut;
    let head = &text[..cut];
    // The line that says so is a line of its own, wherever the log stops.
    let mid_line = if head.is_empty() {
        kept.mid_line
    } else {
        !head.ends_with('\n')
    };
    kept.mid_line = false;
    let newline = if mid_line { "\n" } else { "" };
    Some(format!("{head}{newline}{}", log_capped_notice(cap, tail)))
}

/// How far into the end of a log past its cap a line's start is looked for:
/// an end that begins in a line longer than this begins inside it.
const LINE_NEAR: usize = 64 * 1024;

/// A per-task handle an executor uses to stream incremental output. Bound to one
/// `(task_id, fence)` by the worker; secrets are masked here (so streamed chunks
/// are redacted like the final output, #8) and the first chunk is flagged so the
/// loop can reset a retried task's prior output. Cheap to clone.
///
/// It sends an attempt's log up to a cap ([`log_task_cap`]) as the task
/// writes it: the chunk that reaches it is cut there, a line says so, and
/// what the task writes after is held back here, counted and not queued for
/// the loop. Once the attempt is over its seal adds the end of what the task
/// wrote ([`log_tail_bytes`]), under a line that says how much was left out
/// between. So a log keeps its beginning, and its end once it ends.
#[derive(Clone)]
pub struct LogSink {
    tx: UnboundedSender<LogChunk>,
    task_id: String,
    fence: i64,
    redactor: crate::redact::Redactor,
    started: Arc<AtomicBool>,
    cap: usize,
    /// How much of the end of a log past its cap its seal adds: no more than
    /// the cap.
    tail: usize,
    kept: Arc<std::sync::Mutex<Kept>>,
    /// The key the hash of what was sent is made with: of this sink alone, so
    /// that no text a task writes can be made to hash as another would.
    keys: std::hash::RandomState,
}

impl LogSink {
    /// Build a sink bound to one task attempt.
    pub fn new(
        tx: UnboundedSender<LogChunk>,
        task_id: String,
        fence: i64,
        redactor: crate::redact::Redactor,
    ) -> Self {
        Self {
            tx,
            task_id,
            fence,
            redactor,
            started: Arc::new(AtomicBool::new(false)),
            cap: log_task_cap(),
            tail: log_tail_bytes(log_task_cap()),
            kept: Arc::default(),
            keys: std::hash::RandomState::new(),
        }
    }

    /// The same sink, keeping `cap` bytes of the attempt's log as it is
    /// written, and of its end what [`log_tail_bytes`] says of that cap.
    pub fn with_cap(mut self, cap: usize) -> Self {
        self.cap = cap;
        self.tail = log_tail_bytes(cap);
        self
    }

    /// The same sink, keeping `tail` bytes of the end of a log past its cap,
    /// and no more than its cap; `0`, its beginning alone. After
    /// [`with_cap`](Self::with_cap), which sets it too.
    pub fn with_tail(mut self, tail: usize) -> Self {
        self.tail = tail.min(self.cap);
        self
    }

    /// How much of the end of a log past its cap its seal adds.
    pub(crate) fn tail(&self) -> usize {
        self.tail
    }

    /// What a log past its cap left out between its beginning and its end,
    /// in bytes and lines as the task wrote them: once its attempt is over,
    /// and for a log that reached its cap alone.
    pub fn omitted(&self) -> Option<(usize, usize)> {
        self.kept.lock().unwrap_or_else(|e| e.into_inner()).omitted
    }

    /// How much of the task's output, as it wrote it, its log takes where it
    /// has secrets: its cap, and what an executor keeps past it for the seal
    /// ([`SEAL_PAST_CAP`], or the longest secret). Masks shorten a text, so
    /// a log of a task that prints a long secret again and again would reach
    /// its cap only far past it, and an executor keep all that for the seal,
    /// as the task wrote it. With no secret, a log's text is as long as what
    /// the task wrote, or longer, and its cap is the bound.
    fn raw_cap(&self) -> Option<usize> {
        (!self.redactor.is_empty()).then(|| {
            self.cap
                .saturating_add(SEAL_PAST_CAP.max(self.redactor.longest()))
        })
    }

    /// Send `chunk`, counted in what the log was sent: as its next, or as
    /// the whole log over again.
    fn record(&self, kept: &mut Kept, chunk: String, whole: bool) {
        use std::hash::{BuildHasher, Hasher};
        kept.sent
            .get_or_insert_with(|| self.keys.build_hasher())
            .write(chunk.as_bytes());
        kept.sent_bytes += chunk.len();
        let first = !self.started.swap(true, Ordering::SeqCst) || whole;
        self.send(chunk, first);
    }

    fn send(&self, chunk: String, first: bool) {
        let _ = self.tx.send(LogChunk {
            task_id: self.task_id.clone(),
            fence: self.fence,
            chunk,
            first,
        });
    }

    /// Whether `log` is what was sent already, as its hash and its length
    /// say.
    fn sent_already(&self, kept: &Kept, log: &str) -> bool {
        use std::hash::{BuildHasher, Hasher};
        let mut hash = self.keys.build_hasher();
        hash.write(log.as_bytes());
        kept.sent_bytes == log.len()
            && kept
                .sent
                .as_ref()
                .is_some_and(|sent| sent.finish() == hash.finish())
    }

    /// Whether the log has stopped at its cap: what is appended to it now is
    /// held back.
    pub fn is_capped(&self) -> bool {
        self.kept.lock().unwrap_or_else(|e| e.into_inner()).capped
    }

    /// Stream one output chunk (redacted). Best-effort: a closed receiver (loop
    /// gone) or an empty chunk is silently dropped. Streaming redaction is
    /// chunk-wise, so a secret split across chunks may slip through the live view
    /// — the final stored output is always redacted whole, so it self-corrects.
    /// Past the cap a chunk is held back: counted, and neither masked nor sent.
    ///
    /// What of it is still under the cap is sent: all of it; or its beginning,
    /// cut clear of a secret, and the line that says the log reached its cap
    /// there; or nothing, once it has. And, where the task has secrets, where
    /// what the task wrote would pass what its log takes
    /// ([`raw_cap`](Self::raw_cap)), what of it is under that, cut clear of a
    /// secret, and a line that says the log stops there. Either cut is made
    /// in what the task wrote, so what the log took of it is known to the
    /// byte.
    ///
    /// Masked and sent under the lock. A task's two streams share one sink,
    /// each from a reader of its own, and the line that says the log stops is
    /// the last that either sends. Once the attempt is over
    /// ([`seal`](Self::seal), [`close`](Self::close)) a chunk is dropped.
    pub fn append(&self, chunk: &str) {
        if chunk.is_empty() {
            return;
        }
        let mut kept = self.kept.lock().unwrap_or_else(|e| e.into_inner());
        if kept.ended {
            return;
        }
        let lines = newlines(chunk);
        kept.total_raw += chunk.len();
        kept.total_lines += lines;
        // Past the cap, counted for what is left out, and not masked.
        if kept.capped {
            return;
        }
        let text = without_nul(self.redactor.redact(chunk).into_owned());
        let room = self.cap - kept.bytes.min(self.cap);
        let raw_room = self.raw_cap().map(|most| most.saturating_sub(kept.raw));
        let over_raw = raw_room.is_some_and(|raw_room| chunk.len() > raw_room);
        if text.len() <= room && !over_raw {
            kept.raw += chunk.len();
            kept.head_lines += lines;
            kept.bytes += text.len();
            kept.mid_line = !text.ends_with('\n');
            self.record(&mut kept, text, false);
            return;
        }
        // The log stops in this piece, at the first bound it passes, in what
        // the task wrote and clear of a secret; where both fall at one byte,
        // on what the task wrote.
        let at_cap = if text.len() > room {
            head_cut(&self.redactor, chunk, room)
        } else {
            chunk.len()
        };
        let at_raw = match raw_room {
            Some(raw_room) if over_raw => self.redactor.clear_cut(chunk, raw_room),
            _ => chunk.len(),
        };
        let stopped = over_raw && at_raw <= at_cap;
        let take = if stopped { at_raw } else { at_cap };
        let mut part = without_nul(self.redactor.redact(&chunk[..take]).into_owned());
        // Under the cap as it was cut; and so whatever masks of values that
        // overlap make of it.
        part.truncate(char_floor(&part, room));
        kept.raw += take;
        kept.head_lines += newlines(&chunk[..take]);
        kept.bytes += part.len();
        kept.capped = true;
        if stopped {
            kept.stopped_at = Some(kept.raw);
        }
        // The line that says so is a line of its own, wherever the log stops.
        let mid_line = if part.is_empty() {
            kept.mid_line
        } else {
            !part.ends_with('\n')
        };
        kept.mid_line = false;
        let newline = if mid_line { "\n" } else { "" };
        let line = if stopped {
            log_stopped_notice(self.cap, self.tail)
        } else {
            log_capped_notice(self.cap, self.tail)
        };
        self.record(&mut kept, format!("{part}{newline}{line}"), false);
    }

    /// Add a line of dagron's own to the log: why the attempt failed, say. It
    /// is not the task's output, so the cap that stops the task's does not
    /// stop it; the reason an attempt failed is not what a full log loses.
    /// It starts on a line of its own, wherever the task's output stopped.
    pub fn note(&self, line: &str) {
        if line.is_empty() {
            return;
        }
        let mut kept = self.kept.lock().unwrap_or_else(|e| e.into_inner());
        let newline = if kept.mid_line { "\n" } else { "" };
        kept.mid_line = !line.ends_with('\n');
        let first = !self.started.swap(true, Ordering::SeqCst);
        let line = without_nul(self.redactor.redact(line).into_owned());
        self.send(format!("{newline}{line}"), first);
    }

    /// Replace the streamed log with `full` redacted as a whole. Chunk-wise
    /// redaction can miss a secret split across chunks, and the log (unlike
    /// `output`) is not rewritten at completion, so an executor calls this once
    /// with everything it streamed. A no-op when the task has no secrets. Kept
    /// up to the cap, as what it replaces was.
    ///
    /// And a no-op where it would write what was sent already: masked whole,
    /// the text is the one the chunks made (no secret was split across two).
    /// The log is then left as it is, its notes in it, and a reader that
    /// follows it is not told it was written again.
    ///
    /// A log that stopped, at its cap or before it, is sealed with what the
    /// task wrote as far as the log took it, and past a secret that cut in
    /// two, which is then masked whole (`full` holds that much past it:
    /// [`Gathered`] keeps [`SEAL_PAST_CAP`], or the longest secret); never
    /// with what came after it, and on the line that says where it stopped,
    /// though masked whole it may be shorter. And it is given its end, from
    /// `full`, which holds all the task wrote ([`seal_with`](Self::seal_with)).
    pub fn seal(&self, full: &str) {
        self.seal_with(full, full, 0)
    }

    /// [`seal`](Self::seal), by an executor that keeps the beginning of what
    /// the task wrote apart from its end ([`Gathered`]): `head` from its
    /// start, as far as the log took it and past that as `seal` says, and
    /// `end` the rest of it from `end_at` bytes in, to the last byte the task
    /// wrote.
    ///
    /// A log past its cap is given its end here, once its attempt is over:
    /// the last [`tail`](Self::tail) bytes of what the task wrote, from a
    /// line's start where one is near, never from inside a secret, and masked
    /// whole, after a line that says how much was left out ([`gap_line`]).
    /// They are added to the log; or, where the seal writes the log again,
    /// they are in the text that takes its place, which nothing after can
    /// take them out of. The log is sealed once: after it, it takes notes
    /// alone.
    pub(crate) fn seal_with(&self, head: &str, end: &str, end_at: usize) {
        let mut kept = self.kept.lock().unwrap_or_else(|e| e.into_inner());
        if kept.ended {
            return;
        }
        kept.ended = true;
        let secrets = !self.redactor.is_empty();
        if !kept.capped {
            if !secrets || head.is_empty() {
                return;
            }
            let mut would = Kept {
                raw: head.len(),
                total_raw: kept.total_raw,
                total_lines: kept.total_lines,
                head_lines: kept.head_lines,
                ended: true,
                ..Kept::default()
            };
            // Masked whole it can pass the cap the chunks kept it under
            // (two secrets that overlap): it stops there, on a line that
            // promises no end, for none comes.
            let text = without_nul(self.redactor.redact(head).into_owned());
            let log = within_cap(self.cap, 0, &mut would, text).unwrap_or_default();
            if !self.sent_already(&kept, &log) {
                *kept = would;
                self.record(&mut kept, log, true);
            }
            return;
        }
        // Where the log stopped, and past a secret its stop cut in two,
        // which `head` holds whole.
        let stop = if secrets {
            self.redactor.clear_cut(head, kept.raw).max(kept.raw)
        } else {
            kept.raw
        };
        let head_lines = kept.head_lines + newlines(head.get(kept.raw..stop).unwrap_or_default());
        // The log as far as that, masked whole, where that is not what was
        // sent: a secret was cut in two.
        let sealed = match head.get(..stop).filter(|_| secrets) {
            None => None,
            Some(full) => {
                let mut would = Kept {
                    raw: stop,
                    stopped_at: kept.stopped_at,
                    total_raw: kept.total_raw,
                    total_lines: kept.total_lines,
                    head_lines,
                    ended: true,
                    ..Kept::default()
                };
                let text = without_nul(self.redactor.redact(full).into_owned());
                let mut log = within_cap(self.cap, self.tail, &mut would, text).unwrap_or_default();
                if !would.capped {
                    let newline = if log.is_empty() || log.ends_with('\n') {
                        ""
                    } else {
                        "\n"
                    };
                    let line = match kept.stopped_at {
                        Some(_) => log_stopped_notice(self.cap, self.tail),
                        None => log_capped_notice(self.cap, self.tail),
                    };
                    log = format!("{log}{newline}{line}");
                    would.mid_line = false;
                    would.capped = true;
                }
                (!self.sent_already(&kept, &log)).then_some((would, log))
            }
        };
        // And its end, with what was left out before it.
        let left = (
            kept.total_raw.saturating_sub(stop),
            kept.total_lines.saturating_sub(head_lines),
        );
        let (omitted, ending) = if self.tail == 0 {
            (left, None)
        } else if end_at + end.len() != kept.total_raw {
            tracing::warn!(
                end_at,
                end_bytes = end.len(),
                written = kept.total_raw,
                "the end kept of a task's output is not what its log was sent; the log says its \
                 end was not kept"
            );
            (left, Some(gap_line(left.0, left.1, Gap::Unkept)))
        } else {
            let (bytes, lines, ending) = self.tail_after(&kept, stop, head_lines, end, end_at);
            ((bytes, lines), Some(ending))
        };
        match sealed {
            // One text in the log's place: its beginning, the lines that say
            // where it stopped and what was left out, and its end.
            Some((would, log)) => {
                *kept = would;
                kept.omitted = Some(omitted);
                let whole = format!("{log}{}", ending.unwrap_or_default());
                kept.mid_line = !whole.is_empty() && !whole.ends_with('\n');
                self.record(&mut kept, whole, true);
            }
            None => {
                kept.raw = stop;
                kept.head_lines = head_lines;
                kept.omitted = Some(omitted);
                if let Some(ending) = ending {
                    let newline = if kept.mid_line { "\n" } else { "" };
                    let ending = format!("{newline}{ending}");
                    kept.mid_line = !ending.ends_with('\n');
                    self.record(&mut kept, ending, false);
                }
            }
        }
    }

    /// The end a log past its cap is given at its seal: the line that says
    /// what was left out after `stop`, where the log stopped `head_lines`
    /// lines in, and the last [`tail`](Self::tail) bytes of what the task
    /// wrote, of `end`, the rest of it from `end_at` bytes in. What was left
    /// out, in bytes and lines, and the text.
    fn tail_after(
        &self,
        kept: &Kept,
        stop: usize,
        head_lines: usize,
        end: &str,
        end_at: usize,
    ) -> (usize, usize, String) {
        let total = kept.total_raw;
        if total <= stop {
            return (0, 0, gap_line(0, 0, Gap::NoMore));
        }
        // The last `tail` bytes, as a database keeps them, from a
        // character's start.
        let mut from_end = end.len().saturating_sub(self.tail);
        while !end.is_char_boundary(from_end) {
            from_end += 1;
        }
        let mut len = stored_len(&end[from_end..]);
        while len > self.tail {
            let Some(c) = end[from_end..].chars().next() else {
                break;
            };
            len -= if c == '\0' { 3 } else { c.len_utf8() };
            from_end += c.len_utf8();
        }
        // Never from before where the log stopped.
        let from = (end_at + from_end).max(stop);
        let Some(rest) = end.get(from - end_at..) else {
            let left = (total - stop, kept.total_lines.saturating_sub(head_lines));
            return (left.0, left.1, gap_line(left.0, left.1, Gap::Unkept));
        };
        let mut at = 0;
        if from != stop {
            // From a line's start, where one begins near; and never inside a
            // secret begun before it, nor one that would be cut in two.
            let at_line = from > end_at && end.as_bytes()[from - end_at - 1] == b'\n';
            if !at_line {
                if let Some(n) = rest[..char_floor(rest, LINE_NEAR)].find('\n') {
                    if n + 1 < rest.len() {
                        at = n + 1;
                    }
                }
            }
            at = clear_start(&self.redactor, rest, at);
        }
        let taken = &rest[at..];
        let omitted = from + at - stop;
        let lines = kept
            .total_lines
            .saturating_sub(head_lines + newlines(taken));
        let masked = without_nul(self.redactor.redact(taken).into_owned());
        let gap = match (masked.is_empty(), omitted) {
            (false, 0) => Gap::Rest,
            (false, _) => Gap::End,
            (true, 0) => Gap::NoMore,
            (true, _) => Gap::ToEnd,
        };
        (
            omitted,
            lines,
            format!("{}{masked}", gap_line(omitted, lines, gap)),
        )
    }

    /// The attempt is over and no executor sealed its log: a log past its
    /// cap says that its end was not kept, and how much of what the task
    /// wrote that leaves out, rather than end on a line that says its end is
    /// to come. After it, as after a seal, the log takes notes alone.
    pub fn close(&self) {
        let mut kept = self.kept.lock().unwrap_or_else(|e| e.into_inner());
        if kept.ended {
            return;
        }
        kept.ended = true;
        if !kept.capped {
            return;
        }
        let omitted = kept.total_raw.saturating_sub(kept.raw);
        let lines = kept.total_lines.saturating_sub(kept.head_lines);
        kept.omitted = Some((omitted, lines));
        if self.tail == 0 {
            return;
        }
        let newline = if kept.mid_line { "\n" } else { "" };
        kept.mid_line = false;
        let line = format!("{newline}{}", gap_line(omitted, lines, Gap::Unkept));
        self.record(&mut kept, line, false);
    }
}

/// What a task is held to because of the run it is of: set by the engine
/// for a task of a run that is not trusted (`dagron_core::confine`), a pull
/// request from a fork as a CI service records it.
///
/// Its envelope is its `isolation`, raised to the engine's floor for such
/// runs. This is the rest, which no envelope says: a backend gives such a
/// task nothing of the operator's own that it gives every other (the Docker
/// executor's mounts and working directory), and holds each container it
/// makes for it to these limits. On Kubernetes a pod's limits are its own
/// `resources` and the namespace's. The numbers are the operator's, read by
/// the engine (`dagron_core::confine::Limits`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Confinement {
    /// The most memory a container made for the task may use, in bytes. It
    /// is given no swap beyond it.
    pub memory_bytes: i64,
    /// The most processes and threads in one.
    pub pids: i64,
    /// The most CPU time one may use, in billionths of a CPU.
    pub nano_cpus: i64,
}

/// All inputs an executor needs to run one task.
pub struct ExecContext {
    pub command: Vec<String>,
    pub timeout_secs: Option<u64>,
    /// Docker image hint — used by DockerExecutor, ignored by LocalExecutor.
    pub docker_image: Option<String>,
    /// When that image is fetched. `None` is each backend's default: the
    /// Docker executor pulls an image its host lacks, and on Kubernetes the
    /// pod states no policy, so the kubelet's applies.
    pub image_pull_policy: Option<dagron_core::dag::ImagePullPolicy>,
    /// Environment variables for the task. Applied by every backend (subprocess
    /// env, container env, pod env).
    pub env: Vec<dagron_core::dag::EnvVar>,
    /// Per-task pod resource requests/limits. KubeExecutor only.
    pub resources: Option<dagron_core::dag::ResourceRequirements>,
    /// ServiceAccount (IRSA) for the task pod. KubeExecutor only.
    pub service_account: Option<String>,
    /// The **effective** trust envelope for this task — already raised to the
    /// operator's floor by the engine. KubeExecutor shapes the pod's security
    /// context from it, and DockerExecutor the containers it makes; what an
    /// executor cannot enforce (every field on Local, a runtime class on
    /// Docker, an allowlist its operator has not made) the engine refuses
    /// before dispatch rather than letting the task run believing itself
    /// sandboxed (`IsolationSpec::require_enforceable_by`).
    pub isolation: Option<dagron_core::isolation::IsolationSpec>,
    /// The task's workspace, when it asked for one: a directory made for this
    /// attempt that it starts in. Every backend makes one; see
    /// [`crate::workspace`].
    pub workspace: Option<dagron_core::workspace::WorkspaceSpec>,
    /// The task's services: containers it has beside it while it runs, each
    /// started before it and seen ready ([`dagron_core::service`]). The
    /// Docker and Kubernetes backends run them. The local one refuses a task
    /// that has any.
    pub services: Vec<dagron_core::service::ServiceSpec>,
    /// The task's own container runs privileged ([`dagron_core::privileged`]):
    /// the engine dispatches such a task only where its operator allows one,
    /// and the Docker and Kubernetes backends refuse it where they were not
    /// told they may run one.
    pub privileged: bool,
    /// Optional live-log sink (#17). When set, the executor streams incremental
    /// output here as it arrives; when `None` the output is only returned in full
    /// at exit (the original behaviour). The worker wires this up per attempt.
    pub log_sink: Option<LogSink>,
    /// Which task row this execution belongs to, and which attempt of it.
    ///
    /// `None` for callers with no task row behind them — tests and the internal
    /// no-op fallback. A backend that creates a remote workload (a pod, a
    /// container) **labels it with this** so the workload can be found again by
    /// something other than the process that created it. See [`TaskIdentity`].
    pub identity: Option<TaskIdentity>,
    /// The credential the workspace's checkout is fetched with, minted for
    /// this attempt by the worker and revoked after it. It reaches the fetch
    /// alone, never the task: see [`crate::checkout`].
    pub checkout_credential: Option<crate::checkout::Credential>,
    /// The link to the patch a patched checkout lays over its commit, from
    /// the token service, for this attempt ([`crate::checkout::patch_env`]).
    pub checkout_patch: Option<String>,
    /// The credentials the workspace's actions are fetched with, by their
    /// repository, minted for this attempt alone, as the checkout's is: for
    /// an action that asks for one ([`dagron_core::workspace::ActionSpec`]).
    pub action_credentials: Vec<(String, crate::checkout::Credential)>,
    /// The forge token the task asks for ([`dagron_core::forge_token`]):
    /// minted for each attempt by the worker, from the same token service,
    /// and given to the task as `GITHUB_TOKEN`.
    pub forge_token: Option<dagron_core::forge_token::ForgeTokenSpec>,
    /// Set for a task of a run that is not trusted: what a backend withholds
    /// from it and holds it to. See [`Confinement`]. `None` for every other
    /// task, which is given what the operator gives tasks.
    pub confinement: Option<Confinement>,
    /// Asked by the worker when this execution's claim on the task is gone.
    /// A backend then stops and removes what this attempt made and returns
    /// [`ClaimLost`]. See [`Stop`].
    pub stop: Stop,
}

impl ExecContext {
    /// Build a minimal context (no env / resources / service account) — used by
    /// tests and the internal no-op fallback path.
    pub fn new(command: Vec<String>, timeout_secs: Option<u64>, docker_image: Option<String>) -> Self {
        Self {
            command,
            timeout_secs,
            docker_image,
            image_pull_policy: None,
            env: Vec::new(),
            resources: None,
            service_account: None,
            isolation: None,
            workspace: None,
            services: Vec::new(),
            privileged: false,
            log_sink: None,
            identity: None,
            checkout_credential: None,
            checkout_patch: None,
            action_credentials: Vec::new(),
            forge_token: None,
            confinement: None,
            stop: Stop::default(),
        }
    }

    /// How long a task told to stop is given before it is killed: its own
    /// `CI_STEP_GRACE_SECS`, which is what `dagron-ci-exec` gives a step, or
    /// [`DEFAULT_STOP_GRACE_SECS`]. Never more than [`MAX_STOP_GRACE_SECS`].
    pub fn stop_grace_secs(&self) -> u64 {
        self.env
            .iter()
            .find(|e| e.name == ENV_STEP_GRACE)
            .and_then(|e| e.value.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_STOP_GRACE_SECS)
            .min(MAX_STOP_GRACE_SECS)
    }
}

/// The variable a CI job sets the grace of a stopped step with; read here so
/// that a stopped task's container or pod is given the same.
pub const ENV_STEP_GRACE: &str = "CI_STEP_GRACE_SECS";
/// What a stopped task gets when it names no grace: `dagron-ci-exec`'s own
/// default, and Docker's and Kubernetes' order of magnitude.
pub const DEFAULT_STOP_GRACE_SECS: u64 = 10;
/// The longest grace a task may name, as `dagron-ci-exec` has it.
pub const MAX_STOP_GRACE_SECS: u64 = 600;

// ── Executor trait ────────────────────────────────────────────────────────────

/// Pluggable execution backend. Swap between local subprocesses, Docker
/// containers, Kubernetes pods, or remote workers without touching the
/// reconcile loop.
#[async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput>;

    /// Whether [`Executor::prefetch`] does anything: the engine reads a
    /// run's tasks for it only where it does.
    fn prefetches(&self) -> bool {
        false
    }

    /// Fetch a run's images ahead of its tasks, as the engine
    /// first dispatches one of them: each task's (`None`, the executor's
    /// default) and its services', under its pull policy. Failures are the
    /// task's to meet and say; this says nothing. A run that is not trusted
    /// is not fetched for.
    async fn prefetch(&self, _run_id: &str, _images: Vec<RunImage>) {}

    /// A run has ended: what the executor kept for it alone (the digests its
    /// images were pinned at) may go.
    fn forget_run(&self, _run_id: &str) {}

    /// See that nothing is left of the task's earlier claims: stop and remove
    /// every workload it still has from a claim before `ctx`'s, with the
    /// task's grace, and say whether they are gone. It starts nothing.
    ///
    /// For a task that was asked to stop and has nobody watching its work:
    /// the worker that held it died, or could not see its own teardown
    /// through. The engine claims the task anew, so that all it left is an
    /// earlier claim's, and dispatches this in place of [`Executor::execute`].
    ///
    /// `Ok` once nothing of the task is left. `Err` while something is, or
    /// while the backend will not say: the engine asks again later, and the
    /// task reads as still stopping until then.
    ///
    /// Default: `Ok`, for an executor whose work cannot outlive it (the local
    /// process pool, whose children die with their execution).
    async fn teardown(&self, _ctx: &ExecContext) -> Result<()> {
        Ok(())
    }

    /// Every workload this installation owns that is old enough to judge.
    ///
    /// The **listing** half of the fleet sweep. It is split from the deleting
    /// half because only the engine can answer the question in between — "is
    /// this task still live?" is a database query, and an `Executor` has no
    /// database. Handing the executor a pool instead would put schema knowledge
    /// behind a trait anyone may implement.
    ///
    /// Default: an empty list, so an executor with nothing to sweep (the local
    /// process pool, whose children die with it) inherits a correct no-op
    /// rather than being forced to write one.
    async fn list_orphan_candidates(&self, _scope: &OrphanScope<'_>) -> Result<Vec<ManagedWorkload>> {
        Ok(Vec::new())
    }

    /// Delete one workload [`Executor::list_orphan_candidates`] returned.
    ///
    /// Takes the handle from the listing rather than re-deriving one, so the
    /// thing deleted is the thing judged. A backend that cannot delete says so
    /// with an `Err`; the sweep logs it and moves on, because one undeletable
    /// leftover must not stop the rest.
    async fn delete_workload(&self, _w: &ManagedWorkload) -> Result<()> {
        Ok(())
    }

    /// Run one step of a running task in an image of its own (a GitHub
    /// Docker action), beside the task: with the task's workspace, its
    /// network and what it is held to, until the step ends or its time is up.
    ///
    /// `task_id` is the task's ([`TaskIdentity::task_id`]); a task that is
    /// not running here, or has no workspace of the executor's making, is
    /// refused. The step's output is returned once it has ended, for the
    /// task's step runner to say in the task's own log.
    ///
    /// Default: refused, for an executor that runs no such container.
    async fn run_step_container(
        &self,
        _task_id: &str,
        _step: StepContainer,
    ) -> Result<StepOutcome> {
        Err(anyhow::Error::new(NoStepContainers))
    }

    /// Build the image of one step of a running task from a Dockerfile (a
    /// GitHub Docker action whose `runs.image` is one), for
    /// [`Executor::run_step_container`] to run it in: the image's name, and
    /// what the build said.
    ///
    /// `task_id` is as there. The engine asks this only for a trusted run.
    ///
    /// Default: refused, for an executor that builds no such image.
    async fn build_step_image(&self, _task_id: &str, _build: StepBuild) -> Result<StepBuilt> {
        Err(anyhow::Error::new(NoStepBuilds))
    }
}

/// The repository a step's image built from a Dockerfile is named in; its
/// tag is what it was built from ([`Executor::build_step_image`]).
pub const BUILT_STEP_IMAGE: &str = "dagron-ci-action";

/// An image for a step, to build ([`Executor::build_step_image`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepBuild {
    /// Its name: [`BUILT_STEP_IMAGE`], tagged by the engine with a digest
    /// of what it is built from, so that one held already is that image and
    /// is not built again.
    pub image: String,
    /// What it is built from: a tar of the Dockerfile's directory.
    pub context: Vec<u8>,
    /// The Dockerfile, a path of the context.
    pub dockerfile: String,
    /// How long the build may take.
    pub timeout: Duration,
}

/// An image built for a step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepBuilt {
    pub image: String,
    /// What the build said, to [`MAX_STEP_OUTPUT_BYTES`].
    pub output: String,
}

/// An executor that builds no image for a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoStepBuilds;

impl std::fmt::Display for NoStepBuilds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "this engine's executor builds no image from a Dockerfile for a step: the Docker \
             executor does, where its operator lets it (DAGRON_DOCKER_BUILD_ACTIONS=allow)",
        )
    }
}

impl std::error::Error for NoStepBuilds {}

/// Where a step container's workspace is, in the container: where GitHub's
/// runner mounts it for a Docker action. The task's own `/workspace` is
/// mounted there too.
pub const STEP_WORKSPACE_DIR: &str = "/github/workspace";

/// The most of a step container's output that is kept.
pub const MAX_STEP_OUTPUT_BYTES: usize = 4 << 20;

/// One step of a task's, in an image of its own ([`Executor::run_step_container`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepContainer {
    pub image: String,
    /// In place of the image's entrypoint, where the step names one.
    pub entrypoint: Option<String>,
    /// In place of the image's command; the image's where there are none.
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    /// How long it may run.
    pub timeout: Duration,
}

/// How a step container's run went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepOutcome {
    pub exit_code: i64,
    /// Its output, both streams in their order, to
    /// [`MAX_STEP_OUTPUT_BYTES`].
    pub output: String,
    /// Its time was up, and it was stopped.
    pub timed_out: bool,
}

/// An executor that runs no step container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoStepContainers;

impl std::fmt::Display for NoStepContainers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "this engine's executor runs no step in an image of its own: the Docker executor does",
        )
    }
}

impl std::error::Error for NoStepContainers {}

/// What a fleet sweep is allowed to look at.
///
/// Both fields narrow, and neither has a safe default — which is why this is a
/// parameter rather than executor state read from the environment.
#[derive(Debug, Clone)]
pub struct OrphanScope<'a> {
    /// The [`LABEL_INSTALLATION`] value to select on. Required, and the reason
    /// the whole sweep is opt-in: see that constant.
    pub installation: &'a str,
    /// Workloads younger than this are never candidates.
    ///
    /// Not a tuning knob — a correctness one. The sweep asks the database
    /// which tasks are live, and a workload created *after* that answer was
    /// computed would look orphaned because its row had not been written yet.
    /// Listing before querying narrows the window; refusing to judge anything
    /// young closes it, without needing the two to be atomic across a
    /// datastore and an apiserver that share no transaction.
    pub min_age: std::time::Duration,
}

/// A workload a fleet sweep found, and enough identity to judge it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedWorkload {
    /// Backend-specific handle — a pod name, a container id. Whatever
    /// [`Executor::delete_workload`] needs to address exactly this one.
    pub handle: String,
    /// The task this workload says it belongs to. The sweep's whole question is
    /// whether that task is still live.
    pub task_id: String,
    /// For the operator reading the log line, not for the decision.
    pub run_id: Option<String>,
    /// Likewise — it says which attempt left this behind.
    pub attempt: Option<String>,
    /// The backend's own identity for this exact object, where it has one.
    ///
    /// A handle names a **slot**; this names the **occupant**. The distinction
    /// is only visible across time, which is exactly the gap a sweep opens: it
    /// lists, then asks the datastore, then deletes, and a name is not a
    /// promise that the three saw the same object. Kubernetes stamps every
    /// object with a UID and will refuse a delete whose precondition names a
    /// different one, so carrying it turns "delete the pod I judged" from a
    /// convention about how names are minted into something the apiserver
    /// enforces.
    ///
    /// `None` where the backend has no equivalent: a Docker container id is
    /// already the identity rather than a name for one, so deletion there
    /// stays id-based and nothing is lost.
    pub uid: Option<String>,
}

impl ManagedWorkload {
    /// Build a candidate from a workload's labels, or refuse it.
    ///
    /// The refusal is the point, and it belongs here rather than in each
    /// backend's listing. A label **selector** constrains only the labels it
    /// names, and says nothing at all about the rest, so a workload can match
    /// `managed-by` and `installation` exactly while carrying a `task-id` that
    /// is empty or not a usable label value. Nothing dagron creates looks like that —
    /// [`TaskIdentity::labels`] refuses to label partially — but a legacy
    /// install, another tool, or a hand-edited manifest can.
    ///
    /// Such a workload would sail through the sweep exactly like a real
    /// orphan: its task id matches no live row, because no row ever had that
    /// id, and the sweep acts on absence. It would then be **deleted**, which
    /// inverts the rule the rest of this module is built on — a workload that
    /// cannot prove it is stale is left alone. [`is_stale_attempt`] already
    /// refuses a missing or unparseable *attempt* for that reason; the task
    /// id, which the whole judgement rests on, deserves it more.
    ///
    /// `run_id` and `attempt` are not validated: they are for the operator
    /// reading the log line, and no decision reads them.
    /// `uid` is the backend's, passed in rather than read from a label: it is
    /// the backend's own identity for the object, not something dagron wrote.
    /// Whether its absence is anomalous is the caller's to decide, because the
    /// answer differs — see [`ManagedWorkload::uid`].
    pub fn from_labels<'a>(
        handle: String,
        uid: Option<String>,
        get: impl Fn(&str) -> Option<&'a String>,
    ) -> Option<Self> {
        let task_id = get(LABEL_TASK_ID)?;
        label_value(task_id)?;
        Some(Self {
            handle,
            task_id: task_id.clone(),
            run_id: get(LABEL_RUN_ID).cloned(),
            attempt: get(LABEL_ATTEMPT).cloned(),
            uid,
        })
    }
}

// ── Task identity, and why a remote workload must carry it ───────────────────

/// Who a remote workload belongs to.
///
/// Before this existed, `KubeExecutor` and `DockerExecutor` both named their
/// workload `sched-<random uuid>` and kept that name **only on the stack of the
/// `execute()` call that created it**. Two things follow, and both are bugs:
///
/// 1. `ARCHITECTURE.md` claims "the lease bounds *concurrent* execution to one
///    holder". Under these two backends it did not. A lease expires while the
///    pod is still running, another scheduler claims the task and creates a
///    *second* pod, and nothing connects either one to the task row — so both
///    run the command at once. The lease bounded which scheduler owned the
///    *row*, never which workloads were running.
/// 2. If the scheduler process dies mid-execution, the random name dies with
///    it. The pod is then unreachable: no label, no derivation, nothing to
///    select on. It runs to completion — or forever, if the command does not
///    exit — and no sweep can ever find it.
///
/// Labelling the workload with the task row fixes both: a dispatch can find and
/// delete a predecessor before starting, and a workload outlives the process
/// that made it without becoming anonymous.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskIdentity {
    /// `task_runs.id` — stable across lease recovery, so it identifies the
    /// *task*, not one attempt at it.
    pub task_id: String,
    /// `task_runs.run_id`, for operators grepping by run.
    pub run_id: String,
    /// `task_runs.attempt` — increments on every claim, including a recovery,
    /// except a claim handed back by [`Backpressure`], which created no
    /// workload. So the label never decreases from one dispatch to the next,
    /// and strictly increases across dispatches that created a workload.
    /// That ordering is what makes it the right discriminator here: a workload
    /// carrying a **lower** attempt for the same task is a predecessor, and a
    /// predecessor is what must be reaped before this one starts. A *higher*
    /// one is not — see [`is_stale_attempt`].
    pub attempt: i64,
    /// This claim's fencing token: the task row's version once claimed.
    ///
    /// The attempt orders a task's claims only until something resets it, and
    /// a rerun, a retry from the console and a clear all set it back to 0. A
    /// workload a cancelled attempt left running then carries the same
    /// attempt as the rerun's first dispatch, which is "this dispatch's own"
    /// by the rule above, and the two ran side by side. The version is never
    /// set back: every claim raises it, and so does every reset. So a
    /// workload with a **lower** fence is a predecessor whatever happened to
    /// the task in between. See [`is_predecessor`].
    pub fence: i64,
    /// Which dagron installation this workload belongs to
    /// ([`LABEL_INSTALLATION`]). `None` when the operator has not set
    /// [`DAGRON_INSTALLATION`] — the workload is then labelled with everything
    /// else and the fleet sweep stays off, because a sweep that cannot tell
    /// this install's workloads from another's is a sweep that deletes the
    /// wrong ones.
    pub installation: Option<String>,
}

/// Whether a workload labelled with `labelled` is a predecessor this dispatch
/// may reap, given that this dispatch is attempt `mine`.
///
/// **Only a strictly lower attempt is stale**, and the two rejected cases are
/// each a way to destroy live work:
///
/// - *Equal* is this dispatch's own workload, on a same-attempt re-entry.
/// - *Higher* is a *newer* attempt, and it is very likely running. A scheduler
///   that stalls between claiming a row and creating the workload can resume
///   after its lease expired and another replica claimed the row — so attempt 1
///   can reach this code while attempt 2 is already executing. Reaping
///   "anything not mine" would kill the live successor and then start duplicate
///   work: exactly the fence violation these labels exist to prevent.
/// - *Missing or unparseable* is ignored on the same principle. A workload that
///   cannot prove it is stale is not deleted. An orphan that survives is a job
///   for a fleet-wide sweep, which can weigh liveness against the datastore; a
///   live workload deleted by a stale attempt is work already lost.
pub fn is_stale_attempt(labelled: Option<&str>, mine: i64) -> bool {
    labelled.and_then(|a| a.parse::<i64>().ok()).is_some_and(|a| a < mine)
}

/// Whether a workload labelled with `fence` and `attempt` is a predecessor
/// the dispatch `mine` may reap before it starts.
///
/// **By its fence, where it carries one:** strictly lower is a predecessor,
/// for the reasons [`is_stale_attempt`] gives for the attempt, and with one
/// more case covered: a rerun, a retry from the console or a clear, each of
/// which sets the attempt back and leaves the fence rising
/// ([`TaskIdentity::fence`]). A fence that does not parse proves nothing, and
/// the workload is left alone.
///
/// **By its attempt, where it carries none:** a workload made before
/// workloads carried a fence is judged as it was when it was made.
pub fn is_predecessor(fence: Option<&str>, attempt: Option<&str>, mine: &TaskIdentity) -> bool {
    match fence {
        Some(fence) => fence.parse::<i64>().is_ok_and(|f| f < mine.fence),
        None => is_stale_attempt(attempt, mine.attempt),
    }
}

/// Label key marking a workload as this project's, so a sweep can scope itself
/// and never touch a pod or container someone else created.
pub const LABEL_MANAGED_BY: &str = "dagron.dev/managed-by";
/// Label key carrying [`TaskIdentity::task_id`].
pub const LABEL_TASK_ID: &str = "dagron.dev/task-id";
/// Label key carrying [`TaskIdentity::run_id`].
pub const LABEL_RUN_ID: &str = "dagron.dev/run-id";
/// Label key carrying [`TaskIdentity::attempt`].
pub const LABEL_ATTEMPT: &str = "dagron.dev/attempt";
/// Label key carrying [`TaskIdentity::fence`].
pub const LABEL_FENCE: &str = "dagron.dev/fence";
/// The value [`LABEL_MANAGED_BY`] always carries.
pub const MANAGED_BY: &str = "dagron";
/// Label key naming **which dagron installation** owns a workload.
///
/// [`LABEL_MANAGED_BY`] says "some dagron made this"; it does not say *which*.
/// That distinction is the whole reason a fleet-wide sweep is dangerous without
/// this label: two installations sharing one Kubernetes namespace both stamp
/// `managed-by=dagron`, so a sweep scoped only on that reads the other install's
/// pods, fails to find their task ids in its **own** database, concludes they
/// are orphans, and deletes running work belonging to someone else.
///
/// The per-task reap does not need it — it selects on a task id, which is a
/// UUID and therefore globally distinct. A fleet sweep has no such anchor: it
/// looks for workloads whose task is *absent*, and absence is exactly what a
/// foreign installation's workload looks like.
///
/// So the value is operator-supplied ([`DAGRON_INSTALLATION`]) and the sweep is
/// **off** without it. dagron cannot infer it: a namespace can hold two
/// installs, one install can span namespaces, and a pod carries no pointer back
/// to the database that created it.
pub const LABEL_INSTALLATION: &str = "dagron.dev/installation";
/// Environment variable supplying [`LABEL_INSTALLATION`]'s value.
pub const DAGRON_INSTALLATION: &str = "DAGRON_INSTALLATION";

/// Longest value a Kubernetes label may hold.
const MAX_LABEL_VALUE: usize = 63;

impl TaskIdentity {
    /// The labels a workload carries. Empty when the identity cannot be
    /// expressed as valid labels, which is a refusal to label *partially*:
    /// a pod carrying `managed-by` but no `task-id` would be selected by a
    /// sweep and match no task, which is precisely how a reaper deletes live
    /// work.
    pub fn labels(&self) -> std::collections::BTreeMap<String, String> {
        let mut out = std::collections::BTreeMap::new();
        let (Some(task), Some(run)) = (label_value(&self.task_id), label_value(&self.run_id))
        else {
            return out;
        };
        out.insert(LABEL_MANAGED_BY.to_string(), MANAGED_BY.to_string());
        out.insert(LABEL_TASK_ID.to_string(), task);
        out.insert(LABEL_RUN_ID.to_string(), run);
        out.insert(LABEL_ATTEMPT.to_string(), self.attempt.to_string());
        out.insert(LABEL_FENCE.to_string(), self.fence.to_string());
        // Only when it expresses as a label. A workload silently carrying a
        // TRUNCATED installation could be selected by another install whose
        // name shares that prefix — so an unusable value contributes nothing
        // and leaves the sweep unable to claim this workload, which is the safe
        // direction.
        if let Some(inst) = self.installation.as_deref().and_then(label_value) {
            out.insert(LABEL_INSTALLATION.to_string(), inst);
        }
        out
    }

    /// Selector matching every workload this project owns for this task,
    /// whatever attempt made it. `None` when the id will not express as a label
    /// — and then nothing is selected, rather than a selector matching more
    /// than intended.
    pub fn task_selector(&self) -> Option<String> {
        let task = label_value(&self.task_id)?;
        Some(format!("{LABEL_MANAGED_BY}={MANAGED_BY},{LABEL_TASK_ID}={task}"))
    }
}

/// A value usable as a Kubernetes label (and therefore as a Docker one, whose
/// rules are looser).
///
/// Validates rather than sanitises. Two different task ids must never map to
/// one label — a sanitiser that strips offending characters can collide, and a
/// collision here means reaping another task's pod. dagron's own ids are
/// hyphenated v4 UUIDs and pass unchanged; anything else is refused and the
/// workload simply goes unlabelled, which is the pre-existing behaviour rather
/// than a new hazard.
pub fn label_value(raw: &str) -> Option<String> {
    if raw.is_empty() || raw.len() > MAX_LABEL_VALUE {
        return None;
    }
    let ok_edge = |c: char| c.is_ascii_alphanumeric();
    let ok_inner = |c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.';
    let mut chars = raw.chars();
    let first = chars.next()?;
    if !ok_edge(first) || !raw.chars().all(ok_inner) {
        return None;
    }
    if !raw.chars().next_back().is_some_and(ok_edge) {
        return None;
    }
    Some(raw.to_string())
}

// ── LocalExecutor ─────────────────────────────────────────────────────────────

/// Subprocess executor — the default backend (original behavior).
#[derive(Debug, Clone, Default)]
pub struct LocalExecutor {
    /// Where every task process starts, and each workspace task works
    /// ([`crate::workspace::ENV_LOCAL_WORKDIR`]). `None`: the engine's own
    /// working directory, and a new directory for each workspace task.
    pub workdir: Option<std::path::PathBuf>,
}

#[async_trait]
impl Executor for LocalExecutor {
    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
        // A service is a container, and a task here is a process of the
        // engine's own. The engine refuses such a task before dispatch; this
        // is the same answer for any other caller.
        if let Some(why) = dagron_core::service::not_run_locally(&ctx.services) {
            anyhow::bail!(why);
        }
        // A workspace task starts in a directory made for this attempt, unless
        // the operator chose the directory every task starts in.
        let scratch = match (&ctx.workspace, &self.workdir) {
            (Some(_), None) => Some(
                crate::workspace::Scratch::new()
                    .map_err(|e| anyhow::anyhow!("the task's workspace could not be made: {e}"))?,
            ),
            _ => None,
        };
        let cwd = scratch.as_ref().map(|s| s.path()).or(self.workdir.as_deref());
        // Its checkout, into the directory made for this attempt; never into
        // the operator's, which holds the code already (`dagron-ci run`'s
        // working tree) and which a fetch would overwrite.
        let fetched = match (crate::checkout::of(ctx), &scratch) {
            (Err(e), _) => Err(e),
            (Ok(Some(checkout)), Some(dir)) => {
                crate::checkout::fetch_locally(
                    dir.path(),
                    checkout,
                    ctx.checkout_credential.as_ref(),
                    ctx.checkout_patch.as_deref(),
                    effective_timeout_secs(ctx.timeout_secs),
                )
                .await
            }
            (Ok(Some(_)), None) => {
                tracing::info!(
                    "the workspace is the operator's directory: nothing is checked out into it"
                );
                Ok(())
            }
            (Ok(None), _) => Ok(()),
        };
        // Its actions, into a directory made for this attempt beside the
        // workspace, whichever the workspace is; the task is told where.
        let mut actions = None;
        let fetched = match (fetched, crate::checkout::actions_of(ctx)) {
            (Err(e), _) | (Ok(()), Err(e)) => Err(e),
            (Ok(()), Ok([])) => Ok(()),
            (Ok(()), Ok(list)) => match crate::workspace::Scratch::new() {
                Err(e) => Err(anyhow::anyhow!(
                    "a directory for the task's actions could not be made: {e}"
                )),
                Ok(dir) => {
                    let got = crate::checkout::fetch_actions_locally(
                        dir.path(),
                        ctx,
                        list,
                        effective_timeout_secs(ctx.timeout_secs),
                    )
                    .await;
                    match got {
                        Ok(()) => actions = Some(dir),
                        Err(_) => dir.remove().await,
                    }
                    got
                }
            },
        };
        if let Err(e) = fetched {
            if let Some(scratch) = scratch {
                scratch.remove().await;
            }
            return Err(e);
        }
        let mut env = ctx.env.clone();
        if let Some(dir) = &actions {
            env.push(dagron_core::dag::EnvVar {
                name: dagron_core::workspace::ENV_ACTIONS_DIR.to_string(),
                value: dir.path().to_string_lossy().into_owned(),
                value_from: None,
            });
        }
        // Stream line-by-line when a live-log sink is wired (#17); otherwise use
        // the byte-exact buffered path (unchanged behaviour).
        let run = async {
            match &ctx.log_sink {
                Some(sink) => {
                    run_command_streaming(&ctx.command, ctx.timeout_secs, &env, cwd, sink).await
                }
                None => run_command_in(&ctx.command, ctx.timeout_secs, &env, cwd).await,
            }
        };
        let ran = tokio::select! {
            ran = run => ran,
            // The claim is gone. Dropping the run ends the child
            // (`kill_on_drop`), which is all of this attempt there is; its
            // directory goes below, as on every other path.
            _ = ctx.stop.requested() => Err(anyhow::Error::new(ClaimLost)),
        };
        if let Some(scratch) = scratch {
            scratch.remove().await;
        }
        if let Some(actions) = actions {
            actions.remove().await;
        }
        let (code, output) = ran?;
        Ok(ExecOutput {
            success: code == 0,
            output,
            dropped: 0,
        })
    }
}

// ── Task wall clock ──────────────────────────────────────────────────────────

/// What a task gets when it names no `timeout_secs` of its own.
pub const DEFAULT_TASK_TIMEOUT_SECS: u64 = 25;

/// The wall clock a task actually gets: what it asked for, the default when it
/// asked for nothing, and never more than `DAGRON_MAX_TASK_TIMEOUT_SECS` where
/// that is set.
///
/// **Why a ceiling exists.** `timeout_secs` comes from the workflow, so before
/// this the longest a task could run was whatever its author typed. That is
/// correct when the engine and the workflows share an owner — it is your
/// hardware — and wrong the moment the operator of the engine is not the author
/// of every DAG on it. Caps on *how many* runs or tasks an installation admits
/// say nothing about worst-case compute while a single task can run for a week.
///
/// **Unset means unlimited**, so an existing deployment behaves exactly as it
/// did. Where a ceiling is set, a workflow asking for longer is clamped rather
/// than rejected, because the alternative is a DAG that validated yesterday
/// failing to admit today with nothing in the task itself having changed.
///
/// A present-but-unusable value (unparseable, or `0`, which would time every task
/// out instantly) is treated as unset and **said out loud once** — a ceiling that
/// silently is not a ceiling is the failure this function exists to prevent.
pub fn effective_timeout_secs(requested: Option<u64>) -> u64 {
    // Read once. The two halves below are split out and pure so they can be tested
    // for every input, which a function consulting a process-wide cache cannot be:
    // the first test to run would fix the ceiling for all the others.
    static CEILING: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    let ceiling = *CEILING
        .get_or_init(|| parse_ceiling(std::env::var("DAGRON_MAX_TASK_TIMEOUT_SECS").ok().as_deref()));
    clamp_to(requested, ceiling)
}

/// `DAGRON_MAX_TASK_TIMEOUT_SECS` as a ceiling, or `None` for no ceiling.
fn parse_ceiling(raw: Option<&str>) -> Option<u64> {
    let raw = raw?;
    if raw.trim().is_empty() {
        return None;
    }
    match raw.trim().parse::<u64>() {
        Ok(n) if n > 0 => Some(n),
        _ => {
            // Not silent. `0` would time every task out instantly and a typo would
            // leave the fleet uncapped — either way the operator set this variable
            // believing it did something.
            tracing::warn!(
                value = %raw,
                "DAGRON_MAX_TASK_TIMEOUT_SECS is not a positive integer; \
                 no task-duration ceiling is in force"
            );
            None
        }
    }
}

/// What the task asked for (or the default), never above the ceiling.
fn clamp_to(requested: Option<u64>, ceiling: Option<u64>) -> u64 {
    let wanted = requested.unwrap_or(DEFAULT_TASK_TIMEOUT_SECS);
    match ceiling {
        Some(max) => wanted.min(max),
        None => wanted,
    }
}

// ── Low-level subprocess runner ───────────────────────────────────────────────

/// Build the `Command` for a task, under whatever this pool permits.
///
/// One place, so the buffered and streaming paths cannot drift: a rule that
/// held on one of them and not the other would be a hole shaped like "whether a
/// live-log sink happened to be wired", and the streaming path is the one a
/// real task takes.
///
/// With neither pool knob set this is what it always was — spawn `command[0]`,
/// layer `env` over the inherited environment. See [`crate::pool`] for what the
/// knobs do and why the allowlist matches a resolved path rather than a word.
fn build_command(
    command: &[String],
    env: &[dagron_core::dag::EnvVar],
    cwd: Option<&std::path::Path>,
) -> Result<Command> {
    let policy = crate::pool::policy();
    let program = policy.program(&command[0], |name| {
        crate::pool::resolve_on_path(name, std::env::var("PATH").ok().as_deref())
    })?;

    let mut cmd = Command::new(program);
    cmd.args(&command[1..]).kill_on_drop(true);
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    match policy.child_env(env, |name| std::env::var(name).ok())? {
        // Isolated: the child gets exactly this, and nothing it was not given.
        Some(pairs) => {
            cmd.env_clear();
            for (name, value) in pairs {
                cmd.env(name, value);
            }
        }
        // Unisolated: inherit, and layer the task's env on top.
        None => {
            for e in env {
                cmd.env(&e.name, &e.value);
            }
        }
    }
    Ok(cmd)
}

/// Spawns `command[0]` with `command[1..]` as args.
/// `timeout_secs` caps execution; falls back to [`DEFAULT_TASK_TIMEOUT_SECS`]
/// (inside the 30 s lease) and is clamped by [`effective_timeout_secs`].
/// `env` is layered on top of the inherited environment — or replaces it, where
/// the pool isolates ([`crate::pool`]).
/// `kill_on_drop` ensures the child is reaped if the future is dropped.
pub async fn run_command(
    command: &[String],
    timeout_secs: Option<u64>,
    env: &[dagron_core::dag::EnvVar],
) -> Result<(i32, String)> {
    run_command_in(command, timeout_secs, env, None).await
}

/// [`run_command`] in the directory `cwd`; the engine's own when `None`.
async fn run_command_in(
    command: &[String],
    timeout_secs: Option<u64>,
    env: &[dagron_core::dag::EnvVar],
    cwd: Option<&std::path::Path>,
) -> Result<(i32, String)> {
    if command.is_empty() {
        bail!("empty command");
    }
    let secs = effective_timeout_secs(timeout_secs);
    if secs == 0 {
        bail!("timeout_secs must be >= 1 when provided");
    }
    let mut cmd = build_command(command, env, cwd)?;

    let output = timeout(Duration::from_secs(secs), cmd.output())
        .await
        .map_err(|_| anyhow::Error::new(TimeoutError { secs }))??;

    let exit_code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    // A task's stderr is part of its output, and nothing wrong with the
    // engine: noted in the engine's own log only at `debug`, where a task that
    // succeeded drew a warning before. Sensitive env values are masked out of
    // that line too (#8); the stored output is redacted centrally in the
    // worker.
    if !stderr.is_empty() && tracing::enabled!(tracing::Level::DEBUG) {
        let redactor = crate::redact::Redactor::from_task_env(env);
        tracing::debug!(stderr = %redactor.redact(stderr.trim()), "subprocess stderr");
    }
    Ok((exit_code, with_stderr_on_failure(exit_code, stdout, &stderr)))
}

/// How much of a failing task's stderr rides along in the stored output.
///
/// Unbounded would be wrong — a task that dies in a retry loop can emit
/// megabytes, and this string lands in a database column, once per attempt.
/// The **tail** rather than the head because the fatal error is the last thing
/// a process prints; the head is startup banners.
const STDERR_TAIL_LIMIT: usize = 16 * 1024;

/// Append a failing command's stderr to its stored output.
///
/// **Only on failure**, which is the whole care here. `output` is load-bearing
/// on the success path — `repeat.until` decides loop termination from it, the
/// memoization store caches it, and `produces:` lineage reads it — so a
/// successful task's output stays byte-identical to what it has always been.
/// A failing task's output is only ever an error message, and it was missing
/// the half that says what went wrong: stderr was logged and then discarded,
/// so `RuntimeError`, `CUDA error`, and every NCCL warning — all of which are
/// written to stderr — never reached the stored record or the fault
/// classifier that now reads it.
///
/// This also makes the local backend agree with the other two: DockerExecutor
/// interleaves both streams into `output`, and KubeExecutor stores the pod's
/// combined log. Local was the odd one out.
fn with_stderr_on_failure(exit_code: i32, stdout: String, stderr: &str) -> String {
    if exit_code == 0 || stderr.trim().is_empty() {
        return stdout;
    }
    let trimmed = stderr.trim_end();
    let tail = if trimmed.len() > STDERR_TAIL_LIMIT {
        // Cut on a char boundary, then forward to the next line break so the
        // first retained line is whole rather than starting mid-token.
        //
        // When the tail contains no line break at all — one enormous line, which
        // a JSON-logging framework produces — there is no boundary to forward
        // to and the retained text does start mid-line. That is deliberate:
        // truncating from the front of a single line keeps the end, and the end
        // is where the error is. Dropping it entirely to preserve a "whole
        // line" property would discard the only diagnostic there is.
        let start = trimmed.len() - STDERR_TAIL_LIMIT;
        let start = (start..trimmed.len())
            .find(|i| trimmed.is_char_boundary(*i))
            .unwrap_or(trimmed.len());
        let rest = &trimmed[start..];
        match rest.find('\n') {
            Some(nl) => &rest[nl + 1..],
            None => rest,
        }
    } else {
        trimmed
    };
    if stdout.trim().is_empty() {
        return tail.to_string();
    }
    let mut out = stdout;
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(tail);
    out
}

/// Streaming variant of [`run_command`] (#17): pipes stdout and forwards each
/// line to `sink` as it arrives (for live tailing) while accumulating the full
/// stdout to return at exit. stderr is drained concurrently (so a chatty child
/// can't deadlock on a full pipe) and logged redacted, matching `run_command`.
/// Line-buffered, so it appends a trailing newline per line — a cosmetic
/// difference from the byte-exact buffered path, acceptable for a log tail.
async fn run_command_streaming(
    command: &[String],
    timeout_secs: Option<u64>,
    env: &[dagron_core::dag::EnvVar],
    cwd: Option<&std::path::Path>,
    sink: &LogSink,
) -> Result<(i32, String)> {
    use std::process::Stdio;

    if command.is_empty() {
        bail!("empty command");
    }
    let secs = effective_timeout_secs(timeout_secs);
    if secs == 0 {
        bail!("timeout_secs must be >= 1 when provided");
    }
    let mut cmd = build_command(command, env, cwd)?;
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = cmd.spawn()?;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");

    // Both streams go to the live log in arrival order; `output` stays stdout.
    // `log` and the sink take each line in the one order, under `log`'s lock,
    // whatever reads the streams: the end of `log` is the end the seal gives
    // a log past its cap.
    let log = std::sync::Mutex::new(String::new());
    let emit = |line: &str| {
        let chunk = format!("{line}\n");
        let mut log = log.lock().expect("log lock");
        log.push_str(&chunk);
        sink.append(&chunk);
    };
    let combined = async {
        // Read both pipes concurrently so neither backpressures the child into
        // a deadlock.
        let stdout_fut = async {
            let mut lines = BufReader::new(stdout).lines();
            let mut acc = String::new();
            while let Some(line) = lines.next_line().await? {
                acc.push_str(&line);
                acc.push('\n');
                emit(&line);
            }
            Ok::<String, anyhow::Error>(acc)
        };
        let stderr_fut = async {
            let mut lines = BufReader::new(stderr).lines();
            let mut acc = String::new();
            while let Some(line) = lines.next_line().await? {
                acc.push_str(&line);
                acc.push('\n');
                emit(&line);
            }
            Ok::<String, anyhow::Error>(acc)
        };
        let (out, err) = tokio::try_join!(stdout_fut, stderr_fut)?;
        let status = child.wait().await?;
        Ok::<_, anyhow::Error>((status, out, err))
    };

    let finished = timeout(Duration::from_secs(secs), combined).await;
    sink.seal(&log.lock().expect("log lock"));
    let (status, stdout_s, stderr_s) = finished.map_err(|_| anyhow::Error::new(TimeoutError { secs }))??;

    // At `debug`, as above: the lines themselves are already in the task's log.
    if !stderr_s.is_empty() && tracing::enabled!(tracing::Level::DEBUG) {
        let redactor = crate::redact::Redactor::from_task_env(env);
        tracing::debug!(stderr = %redactor.redact(stderr_s.trim()), "subprocess stderr");
    }
    let code = status.code().unwrap_or(-1);
    Ok((code, with_stderr_on_failure(code, stdout_s, &stderr_s)))
}

#[cfg(test)]
mod timeout_ceiling_tests {
    use super::{clamp_to, parse_ceiling, DEFAULT_TASK_TIMEOUT_SECS};

    /// Unset must mean unlimited. A self-hosted engine runs on its owner's
    /// hardware, and a ceiling appearing there because the cloud wanted one would
    /// break workflows that have been correct for as long as they have existed.
    #[test]
    fn no_ceiling_leaves_the_task_alone() {
        assert_eq!(parse_ceiling(None), None);
        assert_eq!(clamp_to(Some(86_400), None), 86_400);
        assert_eq!(clamp_to(None, None), DEFAULT_TASK_TIMEOUT_SECS);
    }

    /// The point of the whole change: what a workflow asks for is a request, not a
    /// grant, once an operator has set a ceiling.
    #[test]
    fn a_ceiling_binds_the_request_and_the_default() {
        assert_eq!(clamp_to(Some(86_400), Some(600)), 600);
        assert_eq!(clamp_to(Some(5), Some(600)), 5, "under the ceiling is untouched");
        assert_eq!(clamp_to(None, Some(10)), 10, "the default is clamped too");
        assert_eq!(clamp_to(None, Some(600)), DEFAULT_TASK_TIMEOUT_SECS);
    }

    /// A ceiling that silently is not a ceiling is the failure this exists to
    /// prevent, so every unusable value resolves to "no ceiling" — loudly, via the
    /// warning in `parse_ceiling` — rather than to something arbitrary. `0` matters
    /// most: read as a ceiling it would time out every task in the fleet instantly.
    #[test]
    fn an_unusable_ceiling_is_no_ceiling() {
        for raw in ["0", "abc", "-1", "12s", " ", ""] {
            assert_eq!(parse_ceiling(Some(raw)), None, "{raw:?} must not become a ceiling");
        }
        assert_eq!(parse_ceiling(Some("600")), Some(600));
        assert_eq!(parse_ceiling(Some("  600  ")), Some(600), "whitespace is trimmed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn ident() -> TaskIdentity {
        TaskIdentity {
            task_id: "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e".into(),
            run_id: "9f8e7d6c-5b4a-4938-8271-0a1b2c3d4e5f".into(),
            attempt: 3,
            fence: 7,
            installation: None,
        }
    }

    /// dagron's own ids are hyphenated v4 UUIDs, and they must pass unchanged —
    /// the whole reaper rests on the label round-tripping the task id exactly.
    #[test]
    fn a_task_uuid_is_a_valid_label_value() {
        let id = "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e";
        assert_eq!(label_value(id).as_deref(), Some(id));
    }

    /// Validate, never sanitise.
    ///
    /// A sanitiser that strips offending characters can map two different task
    /// ids onto one label, and a collision here means one task's reaper
    /// deleting another task's live pod. Refusing is the safe direction: the
    /// workload goes unlabelled, which is exactly the behaviour that shipped
    /// before this existed.
    #[test]
    fn an_unlabellable_id_is_refused_rather_than_mangled() {
        assert_eq!(label_value(""), None, "empty");
        assert_eq!(label_value(&"a".repeat(64)), None, "over the 63-byte k8s cap");
        assert_eq!(label_value("-leading"), None, "must start alphanumeric");
        assert_eq!(label_value("trailing-"), None, "must end alphanumeric");
        assert_eq!(label_value("has spaces"), None);
        assert_eq!(label_value("has/slash"), None);
        assert_eq!(label_value("has:colon"), None);
        // Exactly at the cap is fine — the boundary is inclusive.
        assert!(label_value(&"a".repeat(63)).is_some());
        // The inner set k8s allows.
        assert!(label_value("a-b_c.d9").is_some());
    }

    #[test]
    fn labels_carry_the_whole_identity() {
        let l = ident().labels();
        assert_eq!(l.get(LABEL_MANAGED_BY).map(String::as_str), Some(MANAGED_BY));
        assert_eq!(l.get(LABEL_TASK_ID).map(String::as_str), Some("0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e"));
        assert_eq!(l.get(LABEL_RUN_ID).map(String::as_str), Some("9f8e7d6c-5b4a-4938-8271-0a1b2c3d4e5f"));
        assert_eq!(l.get(LABEL_ATTEMPT).map(String::as_str), Some("3"));
        assert_eq!(l.get(LABEL_FENCE).map(String::as_str), Some("7"), "the fence is the discriminator");
    }

    /// A rerun sets the attempt back to 0 and leaves the fence rising. The
    /// container a cancelled attempt left running carries attempt 1, as the
    /// rerun's first dispatch does, and by the attempt alone it was "this
    /// dispatch's own": two containers of one job ran at once.
    #[test]
    fn a_rerun_s_first_dispatch_reaps_what_the_cancelled_attempt_left() {
        let rerun = TaskIdentity { attempt: 1, fence: 9, ..ident() };
        assert!(!is_stale_attempt(Some("1"), rerun.attempt), "by the attempt it looks like its own");
        assert!(is_predecessor(Some("4"), Some("1"), &rerun), "by the fence it is older");
        // A cleared task's too, whose attempt before the clear was higher.
        assert!(is_predecessor(Some("4"), Some("3"), &rerun));
    }

    /// The same three answers the attempt gave, by the fence: only a strictly
    /// lower one is a predecessor.
    #[test]
    fn only_a_strictly_lower_fence_is_reapable() {
        let mine = ident();
        assert!(is_predecessor(Some("6"), None, &mine));
        assert!(!is_predecessor(Some("7"), None, &mine), "its own");
        assert!(!is_predecessor(Some("8"), None, &mine), "a newer claim, very likely running");
        assert!(!is_predecessor(Some("10"), None, &ident()), "numeric, not lexical");
        // A newer claim's workload is not reaped for carrying a lower attempt:
        // that is what a rerun's looks like to the attempt it replaced.
        assert!(!is_predecessor(Some("8"), Some("1"), &mine));
    }

    /// A fence that proves nothing leaves the workload alone; one made before
    /// workloads carried a fence is judged by the rule it was made under.
    #[test]
    fn an_unprovable_fence_is_left_alone_and_a_missing_one_falls_back_to_the_attempt() {
        let mine = ident();
        for fence in ["", "seven", "6x", "99999999999999999999"] {
            assert!(!is_predecessor(Some(fence), Some("1"), &mine), "{fence:?}");
        }
        assert!(is_predecessor(None, Some("2"), &mine), "no fence: a lower attempt");
        assert!(!is_predecessor(None, Some("3"), &mine));
        assert!(!is_predecessor(None, None, &mine), "and nothing at all proves nothing");
    }

    /// A teardown nobody saw through is still a lost claim, and does not read
    /// as one whose work was stopped.
    #[test]
    fn a_teardown_not_seen_through_is_a_lost_claim_that_says_so() {
        let unseen = ClaimLost::unseen("the daemon did not answer");
        assert!(unseen.is::<ClaimLost>() && unseen.is::<TeardownUnseen>());
        let said = unseen.to_string();
        assert!(said.ends_with("may still be running: the daemon did not answer"), "{said}");
        assert!(!said.contains("was stopped and removed"), "{said}");
        assert!(!anyhow::Error::new(ClaimLost).is::<TeardownUnseen>());
    }

    /// The stop signal is heard however long before the wait it was asked for,
    /// by every clone, and is never heard unasked.
    #[tokio::test]
    async fn a_stop_is_heard_once_asked_and_never_before() {
        let stop = Stop::default();
        let other = stop.clone();
        assert!(!stop.is_requested());
        let waiting = tokio::time::timeout(Duration::from_millis(50), other.requested()).await;
        assert!(waiting.is_err(), "never asked: never resolves");
        let heard = tokio::spawn(async move { other.requested().await });
        tokio::task::yield_now().await;
        stop.request();
        stop.request();
        tokio::time::timeout(Duration::from_secs(5), heard).await.unwrap().unwrap();
        assert!(stop.is_requested());
        // Asked before the wait began.
        tokio::time::timeout(Duration::from_secs(5), stop.requested()).await.unwrap();
    }

    /// A stopped task is given the grace its job named, bounded, and a
    /// default where it named none.
    #[test]
    fn a_stopped_task_s_grace_is_its_job_s() {
        let with = |value: &str| {
            let mut ctx = ExecContext::new(vec!["true".into()], None, None);
            ctx.env.push(dagron_core::dag::EnvVar {
                name: ENV_STEP_GRACE.into(),
                value: value.into(),
                value_from: None,
            });
            ctx.stop_grace_secs()
        };
        assert_eq!(ExecContext::new(vec!["true".into()], None, None).stop_grace_secs(), 10);
        assert_eq!(with("3"), 3);
        assert_eq!(with("0"), 0);
        assert_eq!(with("9000"), MAX_STOP_GRACE_SECS);
        assert_eq!(with("soon"), DEFAULT_STOP_GRACE_SECS);
    }

    /// The fence, in one predicate. Only a strictly lower attempt is a
    /// predecessor — every other answer here is a way to delete live work.
    #[test]
    fn only_a_strictly_lower_attempt_is_reapable() {
        assert!(is_stale_attempt(Some("1"), 2), "an earlier attempt is a predecessor");
        assert!(is_stale_attempt(Some("1"), 9));
    }

    /// The bug this predicate replaced: the old check skipped only the EQUAL
    /// attempt, so attempt 1 waking up after its lease expired would delete
    /// attempt 2's live pod and then start duplicate work.
    #[test]
    fn a_newer_attempt_is_never_reaped_by_an_older_one() {
        assert!(!is_stale_attempt(Some("2"), 1), "attempt 2 is probably running right now");
        assert!(!is_stale_attempt(Some("2"), 2), "and this one is our own");
    }

    /// A workload that cannot prove it is stale is left alone. An orphan that
    /// survives is a job for a fleet-wide sweep; a live pod deleted on a guess
    /// is work already lost.
    #[test]
    fn an_unprovable_attempt_is_left_alone() {
        assert!(!is_stale_attempt(None, 5), "unlabelled");
        assert!(!is_stale_attempt(Some(""), 5), "empty");
        assert!(!is_stale_attempt(Some("one"), 5), "not a number");
        assert!(!is_stale_attempt(Some("3x"), 5), "not entirely a number");
        assert!(!is_stale_attempt(Some("99999999999999999999"), 5), "overflows i64");
    }

    /// Lexical comparison would call "10" stale against 9. It is not.
    #[test]
    fn the_comparison_is_numeric_not_lexical() {
        assert!(!is_stale_attempt(Some("10"), 9), "10 > 9, whatever string order says");
        assert!(is_stale_attempt(Some("9"), 10));
    }

    /// All-or-nothing, and this is the load-bearing case.
    ///
    /// A workload labelled `managed-by=dagron` with no `task-id` is selected by
    /// the reaper's `managed-by` clause and matches no task — which is precisely
    /// the shape that gets live work deleted. So an identity that cannot be
    /// fully expressed contributes no labels at all.
    #[test]
    fn a_partial_identity_contributes_no_labels_at_all() {
        let bad = TaskIdentity {
            task_id: "has/slash".into(),
            run_id: "ok".into(),
            attempt: 1,
            fence: 1,
            installation: None,
        };
        assert!(bad.labels().is_empty(), "no managed-by without a task-id");
        assert!(bad.task_selector().is_none(), "and nothing to select on");
    }

    /// The selector is scoped twice over: to this project, and to this task.
    /// Either clause alone would over-select.
    #[test]
    fn the_selector_names_both_owner_and_task() {
        let sel = ident().task_selector().expect("a uuid selects");
        assert!(sel.contains(&format!("{LABEL_MANAGED_BY}={MANAGED_BY}")), "{sel}");
        assert!(sel.contains(&format!("{LABEL_TASK_ID}=0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e")), "{sel}");
        assert!(!sel.contains(LABEL_ATTEMPT), "attempt must NOT narrow it — the point is to find OTHER attempts");
    }

    /// A context with no task row behind it carries no identity, and that path
    /// must keep working: it is what tests and the no-op fallback use.
    #[test]
    fn a_context_without_a_task_row_has_no_identity() {
        let ctx = ExecContext::new(vec!["true".into()], None, None);
        assert!(ctx.identity.is_none());
    }

    /// A workspace task on the local executor starts in a directory made for
    /// the attempt, empty, and gone once the attempt is over, on the buffered
    /// path and on the streaming one alike.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_local_workspace_is_made_for_each_attempt_and_removed_after_it() {
        let script = "pwd; ls -A | wc -l; echo left-behind > file";
        let mut ctx = ExecContext::new(vec!["sh".into(), "-c".into(), script.into()], Some(10), None);
        ctx.workspace = Some(dagron_core::workspace::WorkspaceSpec::default());
        let first = LocalExecutor::default().execute(&ctx).await.unwrap();
        assert!(first.success, "{}", first.output);
        let (dir, entries) = first.output.split_once('\n').unwrap();
        assert!(
            std::path::Path::new(dir).file_name().unwrap().to_string_lossy().starts_with("dagron-ws-"),
            "{dir}"
        );
        assert_eq!(entries.trim(), "0", "empty at the start");
        assert!(!std::path::Path::new(dir).exists(), "{dir} is left after the attempt");

        let (tx, _rx) = mpsc::unbounded_channel::<LogChunk>();
        ctx.log_sink = Some(LogSink::new(tx, "task-1".to_string(), 1, crate::redact::Redactor::default()));
        let second = LocalExecutor::default().execute(&ctx).await.unwrap();
        let (again, entries) = second.output.split_once('\n').unwrap();
        assert_ne!(again, dir, "each attempt a directory of its own");
        assert_eq!(entries.trim(), "0");
        assert!(!std::path::Path::new(again).exists());
    }

    /// Where the operator set the directory every task starts in, a task works
    /// there, workspace or not, and the directory is the operator's to keep.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_operator_s_directory_is_every_local_task_s() {
        let dir = std::env::temp_dir().join(format!("dagron-workdir-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir(&dir).unwrap();
        let exec = LocalExecutor { workdir: Some(dir.clone()) };
        let mut ctx = ExecContext::new(vec!["sh".into(), "-c".into(), "pwd; echo kept > kept".into()], Some(10), None);
        let plain = exec.execute(&ctx).await.unwrap();
        ctx.workspace = Some(dagron_core::workspace::WorkspaceSpec::default());
        let workspace = exec.execute(&ctx).await.unwrap();
        let canonical = std::fs::canonicalize(&dir).unwrap();
        for out in [plain, workspace] {
            assert_eq!(std::fs::canonicalize(out.output.trim()).unwrap(), canonical);
        }
        assert_eq!(std::fs::read_to_string(dir.join("kept")).unwrap(), "kept\n");
        std::fs::remove_dir_all(&dir).unwrap();

        // And with none, a task without a workspace runs where it always did.
        let out = LocalExecutor::default()
            .execute(&ExecContext::new(vec!["pwd".into()], Some(10), None))
            .await
            .unwrap();
        assert_eq!(
            std::fs::canonicalize(out.output.trim()).unwrap(),
            std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap()
        );
    }

    /// LocalExecutor streams each stdout line to the sink as a chunk (first-flagged
    /// on the first) while still returning the full accumulated output (#17).
    #[tokio::test]
    async fn local_executor_streams_lines_to_sink() {
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(tx, "task-1".to_string(), 7, crate::redact::Redactor::default());
        let ctx = ExecContext {
            command: vec!["printf".to_string(), "a\\nb\\n".to_string()],
            timeout_secs: Some(10),
            docker_image: None,
            image_pull_policy: None,
            env: vec![],
            resources: None,
            service_account: None,
            isolation: None,
            workspace: None,
            services: Vec::new(),
            privileged: false,
            log_sink: Some(sink),
            identity: None,
            checkout_credential: None,
            checkout_patch: None,
            action_credentials: Vec::new(),
            forge_token: None,
            confinement: None,
            stop: Stop::default(),
        };

        let out = LocalExecutor::default().execute(&ctx).await.unwrap();
        assert!(out.success);
        assert_eq!(out.output, "a\nb\n", "full output still returned for the final store");

        // The two lines streamed as two chunks; only the first carries `first`.
        let mut chunks = Vec::new();
        while let Ok(c) = rx.try_recv() {
            chunks.push(c);
        }
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].chunk, "a\n");
        assert!(chunks[0].first, "first chunk flagged for the reset-on-attempt");
        assert_eq!(chunks[0].task_id, "task-1");
        assert_eq!(chunks[0].fence, 7);
        assert_eq!(chunks[1].chunk, "b\n");
        assert!(!chunks[1].first);
    }

    /// With no sink, the buffered path runs unchanged (byte-exact output).
    #[tokio::test]
    async fn local_executor_without_sink_is_buffered() {
        let ctx = ExecContext::new(vec!["printf".to_string(), "hi".to_string()], Some(10), None);
        let out = LocalExecutor::default().execute(&ctx).await.unwrap();
        assert!(out.success);
        assert_eq!(out.output, "hi", "no trailing newline added on the buffered path");
    }

    /// A task that outruns its `timeout_secs` deadline returns an error that
    /// downcasts to [`TimeoutError`], so the reconcile loop can distinguish a
    /// deadline kill from a non-zero exit and honor `retry_on_timeout` (#24).
    /// Covers both the buffered and the streaming (log-sink) paths.
    #[tokio::test]
    async fn timeout_is_a_typed_timeout_error() {
        // Buffered path.
        let ctx = ExecContext::new(vec!["sleep".to_string(), "5".to_string()], Some(1), None);
        let err = LocalExecutor::default().execute(&ctx).await.expect_err("must time out");
        assert!(err.is::<TimeoutError>(), "buffered timeout must be a TimeoutError, got: {err}");

        // Streaming path (log sink wired).
        let (tx, _rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(tx, "t".to_string(), 1, crate::redact::Redactor::default());
        let ctx = ExecContext {
            command: vec!["sleep".to_string(), "5".to_string()],
            timeout_secs: Some(1),
            docker_image: None,
            image_pull_policy: None,
            env: vec![],
            resources: None,
            service_account: None,
            isolation: None,
            workspace: None,
            services: Vec::new(),
            privileged: false,
            log_sink: Some(sink),
            identity: None,
            checkout_credential: None,
            checkout_patch: None,
            action_credentials: Vec::new(),
            forge_token: None,
            confinement: None,
            stop: Stop::default(),
        };
        let err = LocalExecutor::default().execute(&ctx).await.expect_err("must time out");
        assert!(err.is::<TimeoutError>(), "streaming timeout must be a TimeoutError, got: {err}");
    }

    /// [`Backpressure`] is found by its type, as [`TimeoutError`] is, and by
    /// nothing else: the same words in a re-formatted error are an ordinary
    /// failure. That is the safe direction — an attempt is consumed rather than
    /// given back for a dispatch nobody vouched had created no workload.
    #[test]
    fn backpressure_is_carried_by_its_type_not_its_text() {
        let err = anyhow::Error::new(Backpressure { reason: "the namespace quota is full".into() });
        assert!(err.is::<Backpressure>());
        assert!(!err.is::<TimeoutError>(), "a refusal to start is not a deadline kill");
        assert_eq!(err.to_string(), "not started: the namespace quota is full");

        let reformatted = anyhow::anyhow!("{err}");
        assert_eq!(reformatted.to_string(), err.to_string());
        assert!(!reformatted.is::<Backpressure>(), "the text alone must not carry the meaning");
    }

    /// A plain non-zero exit is NOT a timeout — it stays a normal failure so the
    /// usual attempts-based retry still applies.
    #[tokio::test]
    async fn nonzero_exit_is_not_a_timeout() {
        let ctx = ExecContext::new(vec!["false".to_string()], Some(10), None);
        let out = LocalExecutor::default().execute(&ctx).await.expect("false exits cleanly, non-zero");
        assert!(!out.success, "`false` exits non-zero");
    }

    /// A successful task's stored output must stay byte-identical: `repeat.until`
    /// terminates loops from it, the memo store caches it, and `produces:`
    /// lineage reads it. Folding stderr in on success would change all three.
    #[tokio::test]
    async fn a_successful_command_still_stores_only_its_stdout() {
        let ctx = ExecContext {
            command: vec!["sh".into(), "-c".into(), "echo out; echo noise >&2".into()],
            timeout_secs: Some(10),
            docker_image: None,
            image_pull_policy: None,
            env: vec![],
            resources: None,
            service_account: None,
            isolation: None,
            workspace: None,
            services: Vec::new(),
            privileged: false,
            log_sink: None,
            identity: None,
            checkout_credential: None,
            checkout_patch: None,
            action_credentials: Vec::new(),
            forge_token: None,
            confinement: None,
            stop: Stop::default(),
        };
        let out = LocalExecutor::default().execute(&ctx).await.unwrap();
        assert!(out.success);
        assert_eq!(out.output.trim(), "out", "stderr must not leak into a success");
    }

    /// With a live log, stderr reaches the log even when the task succeeds, while
    /// `output` (what `when:` and the cache read) stays stdout alone.
    #[tokio::test]
    async fn a_successful_streamed_command_logs_stderr_but_keeps_output_stdout() {
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(tx, "t".to_string(), 1, crate::redact::Redactor::default());
        let ctx = ExecContext {
            command: vec!["sh".into(), "-c".into(), "echo out; echo warn >&2".into()],
            timeout_secs: Some(10),
            docker_image: None,
            image_pull_policy: None,
            env: vec![],
            resources: None,
            service_account: None,
            isolation: None,
            workspace: None,
            services: Vec::new(),
            privileged: false,
            log_sink: Some(sink),
            identity: None,
            checkout_credential: None,
            checkout_patch: None,
            action_credentials: Vec::new(),
            forge_token: None,
            confinement: None,
            stop: Stop::default(),
        };
        let out = LocalExecutor::default().execute(&ctx).await.unwrap();
        assert!(out.success);
        assert_eq!(out.output, "out\n", "the value is stdout only");
        let mut log = String::new();
        while let Ok(c) = rx.try_recv() {
            log.push_str(&c.chunk);
        }
        assert!(log.contains("out\n") && log.contains("warn\n"), "both streams logged: {log:?}");
    }

    /// `seal` rewrites the whole log redacted as one piece (so a secret split
    /// across chunks is still masked), and does nothing when there is no secret.
    #[test]
    fn seal_replaces_the_log_redacted_whole() {
        let secret = dagron_core::dag::EnvVar {
            name: "DEPLOY_KEY".to_string(),
            value: "s3cr3t-token-value".to_string(),
            value_from: Some(dagron_core::dag::SecretRef {
                secret: "deploy-key".to_string(),
                optional: false,
            }),
        };
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(
            tx,
            "t".to_string(),
            1,
            crate::redact::Redactor::from_task_env(&[secret]),
        );
        sink.append("key=s3cr3t-");
        sink.append("token-value\n");
        sink.seal("key=s3cr3t-token-value\n");
        let chunks: Vec<LogChunk> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        let last = chunks.last().unwrap();
        assert!(last.first, "a seal replaces rather than appends");
        assert_eq!(last.chunk, "key=***\n");

        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        LogSink::new(tx, "t".to_string(), 1, crate::redact::Redactor::default()).seal("plain\n");
        assert!(rx.try_recv().is_err(), "no secrets, nothing to rewrite");
    }

    /// A seal that would write what was sent already writes nothing: no
    /// secret was split across two chunks, so the log is as it would be
    /// masked whole, and it keeps the notes the engine wrote into it.
    #[test]
    fn a_seal_that_changes_nothing_is_not_made() {
        let secret = dagron_core::dag::EnvVar {
            name: "DEPLOY_KEY".to_string(),
            value: "s3cr3t-token-value".to_string(),
            value_from: Some(dagron_core::dag::SecretRef {
                secret: "deploy-key".to_string(),
                optional: false,
            }),
        };
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(
            tx,
            "t".to_string(),
            1,
            crate::redact::Redactor::from_task_env(std::slice::from_ref(&secret)),
        );
        sink.append("key=s3cr3t-token-value\n");
        sink.append("more\n");
        sink.note("the attempt failed\n");
        sink.seal("key=s3cr3t-token-value\nmore\n");
        let chunks = sent(&mut rx);
        assert_eq!(
            chunks.len(),
            3,
            "{:?}",
            chunks.iter().map(|c| &c.chunk).collect::<Vec<_>>()
        );
        assert_eq!(chunks[0].chunk, "key=***\n");
        assert!(
            chunks[1..].iter().all(|c| !c.first),
            "nothing written again"
        );
        // One that would change it is made: a sink is sealed once, so a
        // sink of its own, sealed with a line it did not send.
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(
            tx,
            "t".to_string(),
            1,
            crate::redact::Redactor::from_task_env(&[secret]),
        );
        sink.append("key=s3cr3t-token-value\n");
        sink.append("more\n");
        sink.seal("key=s3cr3t-token-value\nmore\nand a line it did not send\n");
        let last = sent(&mut rx).pop().unwrap();
        assert!(last.first);
        assert_eq!(last.chunk, "key=***\nmore\nand a line it did not send\n");
    }

    fn sent(rx: &mut mpsc::UnboundedReceiver<LogChunk>) -> Vec<LogChunk> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    /// A sink of `cap` that masks `s3cr3t-token-value`, and keeps no end of
    /// a log past its cap: a log kept as it was before it kept one.
    fn secret_sink(cap: usize) -> (LogSink, mpsc::UnboundedReceiver<LogChunk>) {
        secret_sink_with_tail(cap, 0)
    }

    /// A sink of `cap` that masks `s3cr3t-token-value`, and keeps `tail`
    /// bytes of the end of a log past its cap.
    fn secret_sink_with_tail(
        cap: usize,
        tail: usize,
    ) -> (LogSink, mpsc::UnboundedReceiver<LogChunk>) {
        let secret = dagron_core::dag::EnvVar {
            name: "DEPLOY_TOKEN".to_string(),
            value: "s3cr3t-token-value".to_string(),
            value_from: None,
        };
        let (tx, rx) = mpsc::unbounded_channel::<LogChunk>();
        let redactor = crate::redact::Redactor::from_task_env(&[secret]);
        (
            LogSink::new(tx, "t".to_string(), 1, redactor)
                .with_cap(cap)
                .with_tail(tail),
            rx,
        )
    }

    /// What an executor gathers of a task's output is bounded, however much
    /// it writes: its beginning for the log's seal, which is then no more a
    /// rewrite than the whole would make it; its end for its output, which
    /// is then what the whole would make it, a secret cut at the edge of what
    /// was kept included.
    #[test]
    fn what_is_gathered_of_an_output_is_bounded_and_is_the_same() {
        let cap = 1000;
        let line = |n: usize| format!("line {n:06} s3cr3t-token-value and the rest of it\n");
        let mut whole = String::new();
        let (sink, mut rx) = secret_sink_with_tail(cap, cap / 4);
        let mut gathered = Gathered::new(Some(&sink));
        for n in 0..8_000 {
            let piece = line(n);
            whole.push_str(&piece);
            gathered.push(&piece);
        }
        assert!(
            gathered.head.len() <= 2 * cap + SEAL_PAST_CAP,
            "{}",
            gathered.head.len()
        );
        assert!(
            gathered.tail.len() <= 2 * (cap + OUTPUT_PAST_CAP),
            "{}",
            gathered.tail.len()
        );
        assert!(
            whole.len() > 2 * (cap + OUTPUT_PAST_CAP),
            "the test writes past what is kept"
        );
        gathered.seal();
        let chunks = sent(&mut rx);
        assert!(
            chunks.iter().skip(1).all(|c| !c.first),
            "the seal wrote nothing again"
        );
        let redactor = crate::redact::Redactor::from_task_env(&[dagron_core::dag::EnvVar {
            name: "DEPLOY_TOKEN".to_string(),
            value: "s3cr3t-token-value".to_string(),
            value_from: None,
        }]);
        // The edge of what was kept, inside the secret: cut off once masked.
        let (tail, dropped) = gathered.output();
        assert!(dropped > 0);
        let (tail, dropped) = masked_end(&redactor, &tail, dropped);
        let cut = cap_output_after(tail, cap, dropped);
        let all = cap_output(redactor.redact(&whole).into_owned(), cap);
        // The same end; what the executor did not keep is counted as the
        // task wrote it, what was cut of the rest as it is masked.
        assert_eq!(
            cut.split_once('\n').unwrap().1,
            all.split_once('\n').unwrap().1
        );
        assert!(
            cut.starts_with("[dagron] this attempt's output is over"),
            "{cut}"
        );
        assert!(!cut.contains("s3cr3t"), "{cut}");
    }

    /// A secret longer than what is kept past an output's cap, whose end is
    /// in what the output keeps, is kept from its start, and masked whole.
    #[test]
    fn a_secret_longer_than_the_margin_is_masked_in_the_output() {
        let long = "Q".repeat(OUTPUT_PAST_CAP + 30_000);
        let secret = dagron_core::dag::EnvVar {
            name: "BIG_TOKEN".to_string(),
            value: long.clone(),
            value_from: None,
        };
        let redactor = crate::redact::Redactor::from_task_env(&[secret]);
        let (tx, _rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(tx, "t".to_string(), 1, redactor.clone()).with_cap(1000);
        let mut gathered = Gathered::new(Some(&sink));
        for n in 0..6_000 {
            gathered.push(&format!("filler line {n:06} of the task's own output\n"));
        }
        gathered.push(&format!("{long}\n"));
        gathered.push(&"after it\n".repeat(50));
        let (tail, dropped) = gathered.output();
        let (tail, dropped) = masked_end(&redactor, &tail, dropped);
        let cut = cap_output_after(tail, 1000, dropped);
        assert!(!cut.contains('Q'), "{}", &cut[..200]);
        assert!(cut.contains("***"));
    }

    /// An output's kept end, its beginning dropped, that begins inside a
    /// secret is masked from past it: masks shorten the end, so cutting it to
    /// the cap would not cut that part off.
    #[test]
    fn a_kept_end_that_begins_in_a_secret_is_masked_from_past_it() {
        let long = "Q".repeat(1000);
        let secret = dagron_core::dag::EnvVar {
            name: "BIG_TOKEN".to_string(),
            value: long.clone(),
            value_from: None,
        };
        let redactor = crate::redact::Redactor::from_task_env(&[secret]);
        let cap = 100_000;
        let mut cut_in_one = false;
        for before in 0..4 {
            let (tx, _rx) = mpsc::unbounded_channel::<LogChunk>();
            let sink = LogSink::new(tx, "t".to_string(), 1, redactor.clone()).with_cap(cap);
            let mut gathered = Gathered::new(Some(&sink));
            let line = format!("{}{long}\n", "x".repeat(before));
            for _ in 0..10_000 {
                gathered.push(&line);
            }
            let (tail, dropped) = gathered.output();
            assert!(dropped > 0);
            cut_in_one |= tail.starts_with('Q');
            let (masked, dropped) = masked_end(&redactor, &tail, dropped);
            let cut = cap_output_after(masked, cap, dropped);
            assert!(!cut.contains('Q'), "{}", &cut[..300]);
            assert!(cut.starts_with("[dagron] this attempt's output is over"));
        }
        assert!(cut_in_one, "what was kept begins inside the secret");
        // An end kept whole is masked whole.
        assert_eq!(masked_end(&redactor, "QQ", 0), ("QQ".to_string(), 0));
        assert_eq!(
            masked_end(&redactor, &format!("{long}\n"), 0),
            ("***\n".to_string(), 0)
        );
    }

    /// A log whose secrets mask it far shorter than what the task wrote
    /// stops once what it wrote is past its cap by what is kept past it for
    /// the seal, on a line that says so: what is gathered for the seal is
    /// bounded as the task wrote it, and the seal, which masks it whole,
    /// writes nothing again.
    #[test]
    fn a_log_masked_short_stops_once_what_the_task_wrote_is_past_its_cap() {
        let long = "Q".repeat(1000);
        let secret = dagron_core::dag::EnvVar {
            name: "BIG_TOKEN".to_string(),
            value: long.clone(),
            value_from: None,
        };
        let redactor = crate::redact::Redactor::from_task_env(&[secret]);
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let cap = 100_000;
        let sink = LogSink::new(tx, "t".to_string(), 1, redactor)
            .with_cap(cap)
            .with_tail(0);
        let mut gathered = Gathered::new(Some(&sink));
        let line = format!("{long}\n");
        for _ in 0..10_000 {
            gathered.push(&line);
        }
        assert!(sink.is_capped());
        assert!(
            gathered.head.len() <= cap + 2 * SEAL_PAST_CAP + line.len(),
            "{}",
            gathered.head.len()
        );
        gathered.seal();
        let chunks = sent(&mut rx);
        assert!(
            chunks.iter().skip(1).all(|c| !c.first),
            "the seal wrote nothing again"
        );
        let log: String = chunks.iter().map(|c| c.chunk.as_str()).collect();
        assert!(log.len() < cap, "{}", log.len());
        assert!(!log.contains('Q'));
        assert!(
            log.ends_with(&format!("***\n{}", log_stopped_notice(cap, 0))),
            "{}",
            &log[log.len() - 200..]
        );
        assert!(log_stopped_notice(cap, 0).ends_with(
            ", counted before masking, passed 97.6 KiB (DAGRON_LOG_TASK_CAP_BYTES), and its log \
             stops here\n"
        ));
    }

    /// A piece longer than what is left of what a log takes is taken as far
    /// as that, and the log stops there, or at its cap where it reaches it in
    /// it: a task with a secret it never prints keeps its log's beginning.
    #[test]
    fn a_long_piece_is_taken_as_far_as_the_log_takes_it() {
        let cap = 1000;
        let (sink, mut rx) = secret_sink(cap);
        sink.append(&"x".repeat(cap + SEAL_PAST_CAP + 5000));
        let log: String = sent(&mut rx).iter().map(|c| c.chunk.as_str()).collect();
        assert_eq!(
            log,
            format!("{}\n{}", "x".repeat(cap), log_capped_notice(cap, 0))
        );
        // Where masks keep it under its cap, it stops where what was written
        // passed what it takes, past the secret that is in two there.
        let cap = 100_000;
        let most = cap + SEAL_PAST_CAP;
        let (sink, mut rx) = secret_sink(cap);
        let n = (most - 9) / 18;
        let secrets = "s3cr3t-token-value".repeat(n);
        let pad = "x".repeat(most - 7 - 18 * n);
        sink.append(&format!("{secrets}{pad}s3cr3t-token-value and on"));
        let log: String = sent(&mut rx).iter().map(|c| c.chunk.as_str()).collect();
        assert!(!log.contains("s3cr3t"), "{}", &log[log.len() - 200..]);
        assert!(!log.contains("and on"));
        assert!(
            log.ends_with(&format!("x***\n{}", log_stopped_notice(cap, 0))),
            "{}",
            &log[log.len() - 200..]
        );
    }

    /// A log that stops at its cap, a secret the follow cut into pieces in
    /// what it took and past it (a line longer than what is read of it at a
    /// time), is sealed as far as it stopped, and on its line: masked whole
    /// it is far shorter, and what the task wrote after it, the end of what
    /// was kept cut in a secret, is not in it.
    #[test]
    fn a_log_capped_and_masked_shorter_is_sealed_where_it_stopped() {
        let long = "Z".repeat(100_000);
        let secret = dagron_core::dag::EnvVar {
            name: "BIG_TOKEN".to_string(),
            value: long.clone(),
            value_from: None,
        };
        let redactor = crate::redact::Redactor::from_task_env(&[secret]);
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let cap = 50_000;
        let sink = LogSink::new(tx, "t".to_string(), 1, redactor)
            .with_cap(cap)
            .with_tail(0);
        let mut gathered = Gathered::new(Some(&sink));
        gathered.push("before it\n");
        let text = format!("{long} and {long} and {long}\n");
        for piece in text.as_bytes().chunks(32 * 1024) {
            gathered.push(std::str::from_utf8(piece).unwrap());
        }
        assert!(sink.is_capped());
        let live: String = sent(&mut rx).iter().map(|c| c.chunk.as_str()).collect();
        assert!(live.ends_with(&log_capped_notice(cap, 0)));
        gathered.seal();
        let last = sent(&mut rx).pop().unwrap();
        assert!(last.first);
        assert!(!last.chunk.contains("ZZZZ"), "{}", &last.chunk[..200]);
        assert_eq!(
            last.chunk,
            format!("before it\n***\n{}", log_capped_notice(cap, 0))
        );
    }

    /// A log that stops so in a secret, cut in two by where the task's
    /// output was read, has it in part until it is sealed; the seal masks it
    /// whole, and the log ends as it did.
    #[test]
    fn a_log_stopped_in_a_secret_is_sealed_past_it() {
        let cap = 100_000;
        let (sink, mut rx) = secret_sink(cap);
        let most = cap + SEAL_PAST_CAP;
        let mut gathered = Gathered::new(Some(&sink));
        let line = "s3cr3t-token-value\n";
        let mut raw = 0;
        while raw + line.len() + 100 < most {
            gathered.push(line);
            raw += line.len();
        }
        let end = "key=s3cr3t-";
        gathered.push(&format!("{}{end}", "x".repeat(most - raw - end.len())));
        assert!(!sink.is_capped(), "the log takes it, to the byte");
        gathered.push("token-value\nand on\n");
        assert!(sink.is_capped());
        let live: String = sent(&mut rx).iter().map(|c| c.chunk.as_str()).collect();
        assert!(live.ends_with(&format!("{end}\n{}", log_stopped_notice(cap, 0))));
        gathered.seal();
        let last = sent(&mut rx).pop().unwrap();
        assert!(last.first, "the seal writes it again");
        assert!(!last.chunk.contains("s3cr3t"));
        assert!(!last.chunk.contains("and on"));
        assert!(last
            .chunk
            .ends_with(&format!("xkey=***\n{}", log_stopped_notice(cap, 0))));
    }

    /// Under the cap, all of it is gathered, and the seal is the whole's.
    #[test]
    fn an_output_under_the_cap_is_gathered_whole() {
        let (sink, mut rx) = secret_sink(10_000);
        let mut gathered = Gathered::new(Some(&sink));
        gathered.push("key=s3cr3t-");
        gathered.push("token-value\n");
        gathered.seal();
        let chunks = sent(&mut rx);
        assert_eq!(
            chunks.last().map(|c| c.chunk.as_str()),
            Some("key=***\n"),
            "the split secret is sealed"
        );
        assert!(chunks.last().unwrap().first);
        assert_eq!(
            gathered.output(),
            ("key=s3cr3t-token-value\n".to_string(), 0)
        );
        // With no log, the end alone.
        let mut gathered = Gathered::new(None);
        gathered.push("a\n");
        assert_eq!(gathered.output(), ("a\n".to_string(), 0));
        assert_eq!(cap_output_after("abc".into(), 10, 0), "abc");
        assert!(
            cap_output_after("abc".into(), 10, 5).ends_with("its first 5 bytes are not kept\nabc")
        );
    }

    /// A sink of `cap` with no secret, that keeps no end of a log past its
    /// cap: a log kept as it was before it kept one.
    fn capped_sink(cap: usize) -> (LogSink, mpsc::UnboundedReceiver<LogChunk>) {
        let (tx, rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(tx, "t".to_string(), 1, crate::redact::Redactor::default());
        (sink.with_cap(cap).with_tail(0), rx)
    }

    /// An attempt's log is sent up to its cap and no further. The chunk that
    /// reaches the cap is cut there, the log's last line says that it stops,
    /// and what the task writes after that is not queued for the loop at all:
    /// a task that writes without end costs the engine nothing more.
    #[test]
    fn a_log_is_sent_up_to_its_cap_and_says_where_it_stops() {
        let (sink, mut rx) = capped_sink(25);
        sink.append("0123456789\n");
        sink.append("0123456789\n");
        sink.append("0123456789\n");
        for _ in 0..1000 {
            sink.append("never kept\n");
        }
        let chunks = sent(&mut rx);
        assert_eq!(chunks.len(), 3, "nothing is queued past the cap");
        let log: String = chunks.iter().map(|c| c.chunk.as_str()).collect();
        let notice = log_capped_notice(25, 0);
        assert_eq!(log, format!("0123456789\n0123456789\n012\n{notice}"));
        assert!(
            notice.contains("25 bytes") && notice.contains(ENV_LOG_TASK_CAP),
            "{notice}"
        );
        assert!(notice.ends_with('\n'));
    }

    /// The line a capped log ends on is a line of its own: after a chunk that
    /// filled the log to the byte and stopped in the middle of a line, and
    /// after one that ended a line. A character is not cut in two.
    #[test]
    fn the_line_a_capped_log_ends_on_is_its_own() {
        let notice = log_capped_notice(10, 0);

        let (sink, mut rx) = capped_sink(10);
        sink.append("0123456789"); // to the byte, and no end of line
        sink.append("more\n");
        let log: String = sent(&mut rx).iter().map(|c| c.chunk.as_str()).collect();
        assert_eq!(log, format!("0123456789\n{notice}"));

        let (sink, mut rx) = capped_sink(10);
        sink.append("012345678\n");
        sink.append("more\n");
        let log: String = sent(&mut rx).iter().map(|c| c.chunk.as_str()).collect();
        assert_eq!(log, format!("012345678\n{notice}"));

        // "é" is two bytes, and the cap falls between them.
        let (sink, mut rx) = capped_sink(10);
        sink.append("012345678é and more\n");
        let log: String = sent(&mut rx).iter().map(|c| c.chunk.as_str()).collect();
        assert_eq!(log, format!("012345678\n{notice}"));
    }

    /// What dagron itself adds to a log (why the attempt failed) is not the
    /// task's output, and is added to a log that has reached its cap.
    #[test]
    fn a_note_is_added_to_a_log_that_has_reached_its_cap() {
        let (sink, mut rx) = capped_sink(4);
        sink.append("0123456789\n");
        sink.append("dropped\n");
        sink.note("task timed out after 5s\n");
        let chunks = sent(&mut rx);
        let log: String = chunks.iter().map(|c| c.chunk.as_str()).collect();
        assert_eq!(
            log,
            format!("0123\n{}task timed out after 5s\n", log_capped_notice(4, 0))
        );
        assert!(
            chunks[0].first && !chunks[1].first,
            "the note is added, not put in the log's place"
        );

        // And a note that is all a log holds starts it.
        let (sink, mut rx) = capped_sink(4);
        sink.note("the image could not be pulled\n");
        assert!(sent(&mut rx)[0].first);

        // One that follows output which stopped in the middle of a line
        // starts on a line of its own.
        let (sink, mut rx) = capped_sink(100);
        sink.append("downloading 40%");
        sink.note("task timed out after 5s\n");
        let log: String = sent(&mut rx).iter().map(|c| c.chunk.as_str()).collect();
        assert_eq!(log, "downloading 40%\ntask timed out after 5s\n");
    }

    /// A log sent again as a whole, masked as a whole, ends where the chunks
    /// it takes the place of stopped, on the line that says so, though masked
    /// whole it is shorter: what the task wrote after them is never in it.
    #[test]
    fn a_log_sent_again_as_a_whole_ends_where_it_stopped() {
        let secret = dagron_core::dag::EnvVar {
            name: "DEPLOY_KEY".to_string(),
            value: "s3cr3t-token-value".to_string(),
            value_from: Some(dagron_core::dag::SecretRef {
                secret: "deploy-key".to_string(),
                optional: false,
            }),
        };
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(
            tx,
            "t".to_string(),
            1,
            crate::redact::Redactor::from_task_env(&[secret]),
        )
        .with_cap(12)
        .with_tail(0);
        sink.append("key=s3cr3t-");
        sink.append("token-value\n");
        sink.append("and a great deal more\n");
        sink.seal("key=s3cr3t-token-value\nand a great deal more\n");
        let chunks = sent(&mut rx);
        let last = chunks.last().unwrap();
        assert!(last.first, "it takes the log's place");
        assert_eq!(last.chunk, format!("key=***\n{}", log_capped_notice(12, 0)));
        // The log has stopped again: nothing follows it but a note.
        sink.append("late\n");
        assert!(rx.try_recv().is_err());
    }

    /// A sink of `cap` with no secret that keeps `tail` bytes of the end of
    /// a log past its cap.
    fn tail_sink(cap: usize, tail: usize) -> (LogSink, mpsc::UnboundedReceiver<LogChunk>) {
        let (tx, rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(tx, "t".to_string(), 1, crate::redact::Redactor::default());
        (sink.with_cap(cap).with_tail(tail), rx)
    }

    /// A log as a reader has it: each chunk added to it, and the attempt's
    /// first, or a seal, in place of what came before, as the engine's batch
    /// and dagron-api take them.
    fn joined(chunks: &[LogChunk]) -> String {
        let mut log = String::new();
        for chunk in chunks {
            if chunk.first {
                log.clear();
            }
            log.push_str(&chunk.chunk);
        }
        log
    }

    /// The bytes and lines the line that says what a log left out says, as
    /// what reads that line reads it.
    fn gap_in(log: &str) -> Option<(usize, usize)> {
        let line = log.lines().find(|line| {
            line.starts_with("[dagron] ") && line.contains(" of this attempt's output (")
        })?;
        let (_, counts) = line.split_once(" of this attempt's output (")?;
        let (bytes, rest) = counts.split_once(" bytes and ")?;
        let (lines, _) = rest.split_once(" lines, counted before masking) is left out here")?;
        Some((bytes.parse().ok()?, lines.parse().ok()?))
    }

    /// What follows the line that says what a log left out: its end.
    fn after_gap(log: &str) -> &str {
        let at = log
            .find(" of this attempt's output (")
            .expect("a line that says what was left out");
        let line_end = at + log[at..].find('\n').expect("a whole line");
        &log[line_end + 1..]
    }

    /// How much of what the task wrote the log took, to the byte.
    fn head_raw(sink: &LogSink) -> usize {
        sink.kept.lock().unwrap().raw
    }

    /// A log past its cap is sent up to its cap as the task writes it, and
    /// nothing after; once the attempt is over its seal adds a line that
    /// says how much was left out, and the task's last lines, which begin
    /// at a line's start: so much that the three add up to what it wrote.
    #[test]
    fn a_log_past_its_cap_keeps_its_head_and_its_tail() {
        let (sink, mut rx) = tail_sink(1000, 300);
        let mut whole = String::new();
        for n in 1..=1000 {
            let line = format!("line {n:04} of the task\n");
            whole.push_str(&line);
            sink.append(&line);
        }
        let live = sent(&mut rx);
        assert!(
            joined(&live).ends_with(&log_capped_notice(1000, 300)),
            "nothing is sent past the line it is held at"
        );
        assert!(log_capped_notice(1000, 300).contains(
            "reached 1000 bytes (DAGRON_LOG_TASK_CAP_BYTES): what the task writes from here is \
             held back, and the last 300 bytes of it (DAGRON_LOG_TAIL_BYTES) is added below when \
             it ends"
        ));
        assert_eq!(head_raw(&sink), 1000);
        sink.seal(&whole);
        let sealed = sent(&mut rx);
        assert_eq!(sealed.len(), 1, "one chunk, the line and the tail");
        assert!(!sealed[0].first, "added to the log, not in its place");
        let (omitted, lines) = sink.omitted().unwrap();
        let (gap, tail) = sealed[0].chunk.split_once('\n').unwrap();
        assert_eq!(format!("{gap}\n"), gap_line(omitted, lines, Gap::End));
        assert!(
            tail.starts_with("line 0988 ") && tail.ends_with("line 1000 of the task\n"),
            "{tail}"
        );
        assert!(tail.len() <= 300);
        assert_eq!(1000 + omitted + tail.len(), whole.len());
        assert!(whole.ends_with(tail));
        assert_eq!(lines, newlines(&whole[1000..1000 + omitted]));
        let mut all = live;
        all.extend(sealed);
        assert_eq!(gap_in(&joined(&all)), Some((omitted, lines)));
    }

    /// The end of a log past its cap is the end of what its executor kept of
    /// the output, whose beginning it dropped: counted across what it
    /// dropped, to the byte and the line.
    #[test]
    fn a_tail_is_taken_from_what_gathered_kept_of_its_end() {
        let cap = 1000;
        let (sink, mut rx) = tail_sink(cap, 300);
        let mut gathered = Gathered::new(Some(&sink));
        let mut whole = String::new();
        for n in 0..8_000 {
            let piece = format!("line {n:06} of the task's own output\n");
            whole.push_str(&piece);
            gathered.push(&piece);
        }
        assert!(
            gathered.dropped > 0,
            "what was kept of its end lost its beginning"
        );
        gathered.seal();
        let log = joined(&sent(&mut rx));
        let (omitted, lines) = sink.omitted().unwrap();
        let tail = after_gap(&log);
        assert!(
            tail.starts_with("line ") && tail.ends_with("line 007999 of the task's own output\n")
        );
        let head = head_raw(&sink);
        assert_eq!(head, cap);
        assert_eq!(head + omitted + tail.len(), whole.len());
        assert!(whole.ends_with(tail));
        assert_eq!(lines, newlines(&whole[head..whole.len() - tail.len()]));
        assert_eq!(gap_in(&log), Some((omitted, lines)));
    }

    /// An end that is not what the log was sent is not sent as the log's
    /// end: the log says its end was not kept, and a warning says why.
    #[test]
    fn an_end_that_does_not_add_up_is_not_sent_as_the_log_s() {
        let (sink, mut rx) = tail_sink(100, 50);
        let whole = "0123456789\n".repeat(30);
        sink.append(&whole);
        sink.seal_with(&whole, &whole[100..], 99);
        let log = joined(&sent(&mut rx));
        let (omitted, lines) = sink.omitted().unwrap();
        assert_eq!(
            (omitted, lines),
            (whole.len() - 100, newlines(&whole[100..]))
        );
        assert!(
            log.ends_with(&gap_line(omitted, lines, Gap::Unkept)),
            "{log}"
        );
    }

    /// Where the task wrote no more past the cap than its end keeps, nothing
    /// is left out, and the line says so: the rest follows it, from where
    /// the log stopped, the middle of a line among it. Where it wrote nothing
    /// past where the log stopped, the line says that.
    #[test]
    fn nothing_left_out_is_said_so() {
        // Stopped at a line's end.
        let (sink, mut rx) = tail_sink(100, 50);
        let whole: String = (0..13).map(|n| format!("{n:09}\n")).collect();
        for line in whole.split_inclusive('\n') {
            sink.append(line);
        }
        sink.seal(&whole);
        let notice = log_capped_notice(100, 50);
        let rest = gap_line(0, 0, Gap::Rest);
        assert!(rest.starts_with(
            "[dagron] none of this attempt's output (0 bytes and 0 lines, counted before \
             masking) is left out here; what follows is the rest of it"
        ));
        let log = joined(&sent(&mut rx));
        assert_eq!(
            log,
            format!("{}{notice}{rest}{}", &whole[..100], &whole[100..])
        );
        assert_eq!(log.replace(&notice, "").replace(&rest, ""), whole);
        assert_eq!(sink.omitted(), Some((0, 0)));

        // Stopped in the middle of a line: the rest of it follows the line.
        let (sink, mut rx) = tail_sink(100, 50);
        let whole = "abcdef\n".repeat(20);
        for line in whole.split_inclusive('\n') {
            sink.append(line);
        }
        sink.seal(&whole);
        let log = joined(&sent(&mut rx));
        assert_eq!(
            log,
            format!("{}\n{notice}{rest}{}", &whole[..100], &whole[100..])
        );
        assert!(after_gap(&log).starts_with("cdef\nabcdef\n"));
        assert_eq!(sink.omitted(), Some((0, 0)));

        // Nothing past where it stopped, which was past a secret to the end
        // of what the task wrote.
        let (sink, mut rx) = secret_sink_with_tail(20, 10);
        let whole = "0123456789key=s3cr3t-token-value";
        sink.append("0123456789");
        sink.append("key=s3cr3t-");
        sink.append("token-value");
        sink.seal(whole);
        let chunks = sent(&mut rx);
        let last = chunks.last().unwrap();
        assert!(
            last.first,
            "the secret was cut in two: the log is written again"
        );
        assert_eq!(
            last.chunk,
            format!(
                "0123456789key=***\n{}{}",
                log_capped_notice(20, 10),
                gap_line(0, 0, Gap::NoMore)
            )
        );
        assert!(gap_line(0, 0, Gap::NoMore).ends_with(", and it wrote no more\n"));
        assert_eq!(sink.omitted(), Some((0, 0)));
    }

    /// The end of a log past its cap never begins inside a secret: one its
    /// window begins in is passed, and counted in what was left out. And a
    /// secret of several lines whose last is too short to be masked alone is
    /// passed whole where the window begins a line before its end, though
    /// the window's next line begins in it: what could be the end of a
    /// secret is reckoned from where the window begins, then past the line.
    #[test]
    fn a_tail_that_would_begin_in_a_secret_begins_past_it() {
        let (sink, mut rx) = secret_sink_with_tail(100, 30);
        let whole = format!("{}xxs3cr3t-token-value the end of it", "a\n".repeat(60));
        for line in whole.split_inclusive('\n') {
            sink.append(line);
        }
        sink.seal(&whole);
        let log = joined(&sent(&mut rx));
        for part in ["s3cr3t", "cr3t", "token", "value"] {
            assert!(!log.contains(part), "{part}: {log}");
        }
        assert_eq!(after_gap(&log), " the end of it");
        let (omitted, lines) = sink.omitted().unwrap();
        assert_eq!((omitted, lines), (40, 10), "what was passed is counted");
        assert_eq!(100 + omitted + " the end of it".len(), whole.len());

        let key = "MIIBOgIBAAJBAKj34\nGkxF0d==\nab==";
        let redactor = crate::redact::Redactor::default()
            .with_values([key])
            .with_lines();
        assert_eq!(
            redactor.redact("ab==\n"),
            "ab==\n",
            "too short to be masked alone"
        );
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(tx, "t".to_string(), 1, redactor)
            .with_cap(100)
            .with_tail(17);
        let whole = format!("{}{key}\nafter\n", "a\n".repeat(60));
        assert_eq!(&whole[whole.len() - 17..], "F0d==\nab==\nafter\n");
        for line in whole.split_inclusive('\n') {
            sink.append(line);
        }
        sink.seal(&whole);
        let log = joined(&sent(&mut rx));
        assert!(!log.contains("ab=="), "{log}");
        assert!(!log.contains("F0d"), "{log}");
        assert_eq!(after_gap(&log), "\nafter\n");
    }

    /// A secret the task's output was read across two pieces of, past the
    /// cap and in the end a log keeps, is masked whole there: the end is
    /// masked as one text.
    #[test]
    fn a_secret_split_across_pieces_in_the_tail_is_masked_whole() {
        let (sink, mut rx) = secret_sink_with_tail(100, 200);
        let mut whole = "a\n".repeat(60);
        for line in whole.split_inclusive('\n') {
            sink.append(line);
        }
        for piece in ["key=s3cr3t-", "token-value\n", "done\n"] {
            whole.push_str(piece);
            sink.append(piece);
        }
        sink.seal(&whole);
        let chunks = sent(&mut rx);
        assert!(
            chunks.iter().skip(1).all(|c| !c.first),
            "nothing written again"
        );
        let log = joined(&chunks);
        assert!(!log.contains("s3cr3t"), "{log}");
        assert_eq!(
            after_gap(&log),
            format!("{}key=***\ndone\n", "a\n".repeat(10))
        );
    }

    /// A log past its cap that its seal writes again, a secret cut in two in
    /// its beginning, is written again with its end in it: one text, in the
    /// log's place, which nothing after it takes the end out of. Where no
    /// secret was cut in two, its end is added to it.
    #[test]
    fn a_capped_log_sealed_again_carries_its_tail() {
        let (sink, mut rx) = secret_sink_with_tail(100, 40);
        let mut whole = String::new();
        let mut write = |piece: &str| {
            whole.push_str(piece);
            sink.append(piece);
        };
        write("key=s3cr3t-");
        write("token-value\n");
        for _ in 0..60 {
            write("a\n");
        }
        let live = sent(&mut rx);
        assert!(
            joined(&live).contains("s3cr3t"),
            "the live log has it, in two"
        );
        sink.seal(&whole);
        let sealed = sent(&mut rx);
        assert_eq!(sealed.len(), 1);
        assert!(sealed[0].first, "in the log's place");
        assert_eq!(
            sealed[0].chunk,
            format!(
                "key=***\n{}a\n{}{}{}",
                "a\n".repeat(38),
                log_capped_notice(100, 40),
                gap_line(3, 2, Gap::End),
                "a\n".repeat(20)
            )
        );
        let mut all = live;
        all.extend(sealed);
        assert_eq!(
            all.iter().skip(1).filter(|c| c.first).count(),
            1,
            "one reset"
        );
        assert!(!joined(&all).contains("s3cr3t"));
        assert!(joined(&all).ends_with(&"a\n".repeat(20)));

        // No secret cut in two: the end is added.
        let (sink, mut rx) = secret_sink_with_tail(100, 40);
        let mut whole = String::new();
        let mut write = |piece: &str| {
            whole.push_str(piece);
            sink.append(piece);
        };
        write("key=s3cr3t-token-value\n");
        for _ in 0..60 {
            write("a\n");
        }
        let live = sent(&mut rx);
        sink.seal(&whole);
        let sealed = sent(&mut rx);
        assert_eq!(sealed.len(), 1);
        assert!(!sealed[0].first, "added to the log");
        assert!(sealed[0].chunk.starts_with("[dagron] "));
        let mut all = live;
        all.extend(sealed);
        assert!(all.iter().skip(1).all(|c| !c.first));
    }

    /// [`a_log_masked_short_stops_once_what_the_task_wrote_is_past_its_cap`],
    /// with an end kept: the log ends on the line that says where it
    /// stopped, the line that says what was left out, and the masked end;
    /// and nothing is written again.
    #[test]
    fn a_log_masked_short_keeps_its_end_and_is_not_written_again() {
        let long = "Q".repeat(1000);
        let secret = dagron_core::dag::EnvVar {
            name: "BIG_TOKEN".to_string(),
            value: long.clone(),
            value_from: None,
        };
        let redactor = crate::redact::Redactor::from_task_env(&[secret]);
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let cap = 100_000;
        let sink = LogSink::new(tx, "t".to_string(), 1, redactor)
            .with_cap(cap)
            .with_tail(cap / 4);
        let mut gathered = Gathered::new(Some(&sink));
        let line = format!("{long}\n");
        for _ in 0..10_000 {
            gathered.push(&line);
        }
        gathered.seal();
        let chunks = sent(&mut rx);
        assert!(
            chunks.iter().skip(1).all(|c| !c.first),
            "the seal wrote nothing again"
        );
        let log = joined(&chunks);
        assert!(!log.contains('Q'));
        let stopped = format!("***\n{}", log_stopped_notice(cap, cap / 4));
        let (_, end) = log.split_once(&stopped).expect("the line it stopped on");
        assert!(
            end.starts_with("[dagron] ") && gap_in(end).is_some(),
            "{}",
            &end[..end.len().min(200)]
        );
        let tail = after_gap(end);
        assert!(!tail.is_empty() && tail.len() <= cap / 4);
        assert!(
            tail.split_inclusive('\n')
                .all(|l| l == "***\n" || l == "\n"),
            "{}",
            &tail[..tail.len().min(200)]
        );
        let (omitted, _) = sink.omitted().unwrap();
        assert!(omitted > 0);
    }

    /// [`a_log_capped_and_masked_shorter_is_sealed_where_it_stopped`], with
    /// an end kept: the one text the log is written again as holds it, from
    /// past the secret its window began in.
    #[test]
    fn a_log_capped_and_masked_shorter_is_sealed_with_its_end() {
        let long = "Z".repeat(100_000);
        let secret = dagron_core::dag::EnvVar {
            name: "BIG_TOKEN".to_string(),
            value: long.clone(),
            value_from: None,
        };
        let redactor = crate::redact::Redactor::from_task_env(&[secret]);
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let cap = 50_000;
        let sink = LogSink::new(tx, "t".to_string(), 1, redactor)
            .with_cap(cap)
            .with_tail(cap / 4);
        let mut gathered = Gathered::new(Some(&sink));
        gathered.push("before it\n");
        let text = format!("{long} and {long} and {long}\n");
        for piece in text.as_bytes().chunks(32 * 1024) {
            gathered.push(std::str::from_utf8(piece).unwrap());
        }
        gathered.seal();
        let last = sent(&mut rx).pop().unwrap();
        assert!(last.first);
        assert_eq!(
            last.chunk,
            format!(
                "before it\n***\n{}{}\n",
                log_capped_notice(cap, cap / 4),
                gap_line(200_010, 0, Gap::End)
            )
        );
    }

    /// [`a_log_stopped_in_a_secret_is_sealed_past_it`], with an end kept:
    /// the rest of what the task wrote follows where it stopped, past the
    /// secret, and nothing is left out.
    #[test]
    fn a_log_stopped_in_a_secret_is_sealed_past_it_with_the_rest() {
        let cap = 100_000;
        let (sink, mut rx) = secret_sink_with_tail(cap, cap / 4);
        let most = cap + SEAL_PAST_CAP;
        let mut gathered = Gathered::new(Some(&sink));
        let line = "s3cr3t-token-value\n";
        let mut raw = 0;
        while raw + line.len() + 100 < most {
            gathered.push(line);
            raw += line.len();
        }
        let end = "key=s3cr3t-";
        gathered.push(&format!("{}{end}", "x".repeat(most - raw - end.len())));
        gathered.push("token-value\nand on\n");
        gathered.seal();
        let last = sent(&mut rx).pop().unwrap();
        assert!(last.first, "the seal writes it again");
        assert!(!last.chunk.contains("s3cr3t"));
        assert!(last.chunk.ends_with(&format!(
            "xkey=***\n{}{}\nand on\n",
            log_stopped_notice(cap, cap / 4),
            gap_line(0, 0, Gap::Rest)
        )));
    }

    /// [`a_log_sent_again_as_a_whole_ends_where_it_stopped`], with an end
    /// kept: what the task wrote after the secret is not in it, but for its
    /// end; and the log has ended.
    #[test]
    fn a_log_sent_again_as_a_whole_carries_its_end() {
        let secret = dagron_core::dag::EnvVar {
            name: "DEPLOY_KEY".to_string(),
            value: "s3cr3t-token-value".to_string(),
            value_from: Some(dagron_core::dag::SecretRef {
                secret: "deploy-key".to_string(),
                optional: false,
            }),
        };
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(
            tx,
            "t".to_string(),
            1,
            crate::redact::Redactor::from_task_env(&[secret]),
        )
        .with_cap(12)
        .with_tail(3);
        sink.append("key=s3cr3t-");
        sink.append("token-value\n");
        sink.append("and a great deal more\n");
        sink.seal("key=s3cr3t-token-value\nand a great deal more\n");
        let last = sent(&mut rx).pop().unwrap();
        assert!(last.first, "it takes the log's place");
        assert_eq!(
            last.chunk,
            format!(
                "key=***\n{}{}re\n",
                log_capped_notice(12, 3),
                gap_line(20, 1, Gap::End)
            )
        );
        sink.append("late\n");
        assert!(rx.try_recv().is_err());
    }

    /// A note after an end that stopped in the middle of a line begins on a
    /// line of its own.
    #[test]
    fn a_note_after_the_tail_is_a_line_of_its_own() {
        let (sink, mut rx) = tail_sink(10, 10);
        let whole = "0123456789\ndownloading 40%";
        sink.append("0123456789\n");
        sink.append("downloading 40%");
        sink.seal(whole);
        sink.note("task timed out after 5s\n");
        let log = joined(&sent(&mut rx));
        assert!(
            log.ends_with(
                "left out here; what follows is its end\noading 40%\ntask timed out after 5s\n"
            ),
            "{log}"
        );
    }

    /// A log is sealed once: a second seal, an append, or a close after it
    /// sends nothing, and an append is not counted; a note is still added.
    #[test]
    fn a_sink_is_sealed_once() {
        let (sink, mut rx) = tail_sink(10, 10);
        let whole = "0123456789\n".repeat(5);
        for line in whole.split_inclusive('\n') {
            sink.append(line);
        }
        sink.seal(&whole);
        assert_eq!(sent(&mut rx).len(), 2, "the head, and its end");
        let omitted = sink.omitted();
        sink.seal(&whole);
        sink.append("late\n");
        sink.close();
        assert!(rx.try_recv().is_err(), "nothing more");
        assert_eq!(sink.omitted(), omitted);
        assert_eq!(
            sink.kept.lock().unwrap().total_raw,
            whole.len(),
            "not counted"
        );
        sink.note("the attempt failed\n");
        assert_eq!(sent(&mut rx)[0].chunk, "the attempt failed\n");
    }

    /// A log past its cap whose attempt ended with no seal says that its end
    /// was not kept, and how much that leaves out; one that keeps no end
    /// says nothing, and counts it all the same. Under its cap, nothing.
    #[test]
    fn a_capped_log_no_executor_sealed_says_its_end_was_not_kept() {
        let (sink, mut rx) = tail_sink(10, 10);
        for _ in 0..5 {
            sink.append("0123456789\n");
        }
        let _ = sent(&mut rx);
        sink.close();
        let chunks = sent(&mut rx);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk, gap_line(45, 5, Gap::Unkept));
        assert!(chunks[0]
            .chunk
            .ends_with("is left out here, and its end was not kept\n"));
        assert_eq!(sink.omitted(), Some((45, 5)));

        let (sink, mut rx) = capped_sink(10);
        for _ in 0..5 {
            sink.append("0123456789\n");
        }
        let _ = sent(&mut rx);
        sink.close();
        assert!(rx.try_recv().is_err());
        assert_eq!(sink.omitted(), Some((45, 5)));

        let (sink, mut rx) = tail_sink(100, 10);
        sink.append("0123456789\n");
        let _ = sent(&mut rx);
        sink.close();
        assert!(rx.try_recv().is_err());
        assert_eq!(sink.omitted(), None);
    }

    /// Where a log stops is a byte of what the task wrote: as far as its cap
    /// takes it, a NUL three bytes there; before a secret whose mask would
    /// cross the cap, not on a part of the mask; and at what the task wrote
    /// past what its log takes where that comes first, or at the same byte.
    #[test]
    fn the_head_ends_at_an_exact_byte_of_what_the_task_wrote() {
        // As far as the cap, a character not cut in two.
        let (sink, _rx) = capped_sink(10);
        sink.append("012345678é and more\n");
        assert_eq!(head_raw(&sink), 9);

        // A NUL is three bytes as the log keeps it.
        let (sink, mut rx) = capped_sink(10);
        sink.append("0123");
        sink.append("ab\0cdefgh\n");
        assert_eq!(head_raw(&sink), 8);
        let log = joined(&sent(&mut rx));
        assert_eq!(
            log,
            format!("0123ab\u{FFFD}c\n{}", log_capped_notice(10, 0))
        );

        // A secret whose mask would cross the cap: the log stops before it.
        let (sink, mut rx) = secret_sink(10);
        sink.append("012345");
        sink.append("ab s3cr3t-token-value cd\n");
        assert_eq!(head_raw(&sink), 9);
        let log = joined(&sent(&mut rx));
        assert_eq!(log, format!("012345ab \n{}", log_capped_notice(10, 0)));
        assert!(!log.contains("**\n"));

        // What the task wrote passes what the log takes first, then at the
        // same byte as the cap: the log says it stopped on what was written.
        let value = "s3cr3t-token-value!";
        for (secrets, stops_at) in [(4200, 85_536 - 4200 * 19), (4096, 7_712)] {
            let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
            let redactor = crate::redact::Redactor::default().with_values([value]);
            let sink = LogSink::new(tx, "t".to_string(), 1, redactor)
                .with_cap(20_000)
                .with_tail(0);
            for _ in 0..secrets {
                sink.append(value);
            }
            assert!(!sink.is_capped());
            sink.append(&"x".repeat(10_000));
            assert_eq!(head_raw(&sink), secrets * value.len() + stops_at);
            let log = joined(&sent(&mut rx));
            assert!(
                log.ends_with(&format!("x\n{}", log_stopped_notice(20_000, 0))),
                "{secrets}: {}",
                &log[log.len() - 200..]
            );
        }
    }

    /// A NUL in the end of a log past its cap is the mark of a byte that is
    /// no text, three bytes there, and the end is no more than its size so
    /// counted; what was left out is counted as the task wrote it.
    #[test]
    fn a_nul_in_the_tail_is_its_mark_and_the_tail_keeps_to_its_size() {
        let (sink, mut rx) = tail_sink(20, 12);
        let mut whole = "0123456789\n".to_string();
        sink.append("0123456789\n");
        for _ in 0..10 {
            whole.push_str("a\0\0\0\n");
            sink.append("a\0\0\0\n");
        }
        // It stopped inside a line of them, three bytes each there.
        assert_eq!(head_raw(&sink), 14);
        sink.seal(&whole);
        let log = joined(&sent(&mut rx));
        let tail = after_gap(&log);
        assert_eq!(tail, "a\u{FFFD}\u{FFFD}\u{FFFD}\n");
        assert!(tail.len() <= 12);
        assert!(!log.contains('\0'));
        let (omitted, _) = sink.omitted().unwrap();
        assert_eq!(14 + omitted + "a\0\0\0\n".len(), whole.len());
    }

    /// What [`Gathered`] keeps of an output's end is never less than the most
    /// of it a log's end takes, however often it drops its beginning.
    #[test]
    fn gathered_keeps_enough_of_its_end_for_a_log_s_tail() {
        let cap = 1000;
        let (sink, _rx) = tail_sink(cap, usize::MAX);
        assert_eq!(sink.tail(), cap, "the end is no more than the cap");
        let mut gathered = Gathered::new(Some(&sink));
        let (mut written, mut trims) = (0, 0);
        for n in 0..40_000 {
            let piece = format!("line {n:06}\n");
            let dropped = gathered.dropped;
            gathered.push(&piece);
            written += piece.len();
            trims += usize::from(gathered.dropped > dropped);
            assert!(gathered.tail.len() >= sink.tail().min(written));
            assert_eq!(gathered.dropped + gathered.tail.len(), written);
        }
        assert!(trims >= 2, "{trims}");
    }

    /// A tiny random number generator, for a test that is to be the same at
    /// every run: there are no dev-dependencies.
    struct Xorshift(u64);

    impl Xorshift {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n.max(1) as u64) as usize
        }

        fn letters(&mut self, n: usize) -> String {
            (0..n)
                .map(|_| ['Q', 'Z', 'X', 'J'][self.below(4)])
                .collect()
        }
    }

    /// Head and tail, and what the line between them says was left out, add
    /// up to what the task wrote, to the byte and the line, whatever it
    /// wrote and however it was read, its secrets among it; the end is what
    /// the task wrote last, masked, and no secret is anywhere in the log.
    /// Through what an executor gathers, and through a sink sealed with the
    /// whole.
    #[test]
    fn head_and_tail_add_up_to_what_the_task_wrote() {
        /// How many rounds went each way.
        #[derive(Debug, Default)]
        struct Seen {
            capped: usize,
            left_out: usize,
            written_again: usize,
            dropped: usize,
        }
        let mut seen = Seen::default();
        let mut rng = Xorshift(0x9e37_79b9_7f4a_7c15);
        for round in 0..240 {
            // Of letters the task's text has, and dagron's lines have not:
            // one in another, one of several lines.
            let mut values: Vec<String> = Vec::new();
            for k in 0..rng.below(4) {
                let value = match rng.below(3) {
                    0 => {
                        let (a, b) = (rng.below(5) + 8, rng.below(5) + 8);
                        format!("{}\n{}", rng.letters(a), rng.letters(b))
                    }
                    1 if k > 0 => {
                        let first = values[0].clone();
                        format!("{}{}", &first[first.len() / 2..], rng.letters(4))
                    }
                    _ => {
                        let n = rng.below(16) + 4;
                        rng.letters(n)
                    }
                };
                values.push(value);
            }
            let redactor = crate::redact::Redactor::default()
                .with_values(values.clone())
                .with_lines();
            let mut masked: Vec<String> = values.clone();
            for value in values.iter().filter(|v| v.contains('\n')) {
                masked.extend(value.lines().filter(|l| l.len() >= 8).map(str::to_string));
            }
            masked.retain(|v| v.chars().count() >= 4);
            // Read a few bytes at a time, or in pieces past the end a log
            // keeps and what an executor keeps past its cap; and, now and
            // then, so much that what an executor keeps of the end drops its
            // beginning.
            let small = rng.below(2) == 0;
            let size = if !small && rng.below(6) == 0 {
                150_000 + rng.below(100_000)
            } else {
                rng.below(20_000)
            };
            let mut whole = String::new();
            while whole.len() < size {
                match rng.below(12) {
                    0 | 1 => whole.push('\n'),
                    2 if !values.is_empty() => {
                        let k = rng.below(values.len());
                        whole.push_str(&values[k]);
                    }
                    3 => whole.push('\0'),
                    4 => whole.push('é'),
                    _ => {
                        let n = rng.below(30);
                        whole.push_str(&rng.letters(n).to_lowercase());
                        whole.push(' ');
                    }
                }
            }
            let cap = rng.below(4000) + 50;
            let tail = rng.below(cap + 1);
            let mut pieces = Vec::new();
            let mut at = 0;
            while at < whole.len() {
                let len = if small {
                    rng.below(20) + 1
                } else {
                    rng.below(cap + tail + 70_000) + 1
                };
                let mut end = (at + len).min(whole.len());
                while !whole.is_char_boundary(end) {
                    end += 1;
                }
                pieces.push(&whole[at..end]);
                at = end;
            }
            for gathered in [true, false] {
                let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
                let sink = LogSink::new(tx, "t".to_string(), 1, redactor.clone())
                    .with_cap(cap)
                    .with_tail(tail);
                if gathered {
                    let mut kept = Gathered::new(Some(&sink));
                    for piece in &pieces {
                        kept.push(piece);
                    }
                    kept.seal();
                } else {
                    for piece in &pieces {
                        sink.append(piece);
                    }
                    sink.seal(&whole);
                }
                let case = format!("round {round}, gathered {gathered}, cap {cap}, tail {tail}");
                let chunks = sent(&mut rx);
                seen.written_again += usize::from(chunks.iter().skip(1).any(|c| c.first));
                let log = joined(&chunks);
                for value in &masked {
                    assert!(
                        !log.contains(value.as_str()),
                        "{case}: {value:?} is in the log"
                    );
                }
                assert!(!log.contains('\0'), "{case}");
                let mask = |text: &str| without_nul(redactor.redact(text).into_owned());
                let Some((omitted, lines)) = sink.omitted() else {
                    assert!(!sink.is_capped(), "{case}");
                    assert_eq!(
                        log,
                        mask(&whole),
                        "{case}: a log under its cap is all of it"
                    );
                    continue;
                };
                let head = head_raw(&sink);
                let from = head + omitted;
                // Where the head ends and where the end begins, no secret
                // the task wrote is cut in two.
                let spans = |at: usize| {
                    masked.iter().any(|v| {
                        (at.saturating_sub(v.len() - 1)..at).any(|s| {
                            whole.is_char_boundary(s) && whole[s..].starts_with(v.as_str())
                        })
                    })
                };
                assert!(!spans(head), "{case}: a secret spans where the head ends");
                assert!(!spans(from), "{case}: a secret spans where the end begins");
                seen.capped += 1;
                seen.dropped += usize::from(gathered && whole.len() > 2 * (cap + OUTPUT_PAST_CAP));
                assert!(from <= whole.len(), "{case}");
                assert_eq!(
                    lines,
                    newlines(&whole[head..from]),
                    "{case}: lines left out"
                );
                assert!(log.len() <= cap + tail + 2048, "{case}: {}", log.len());
                let notice = log
                    .find("[dagron] this attempt's")
                    .expect("the line the log stopped on");
                let expected = mask(&whole[..head]);
                if expected.len() <= cap {
                    let newline = if expected.is_empty() || expected.ends_with('\n') {
                        ""
                    } else {
                        "\n"
                    };
                    assert_eq!(
                        &log[..notice],
                        format!("{expected}{newline}"),
                        "{case}: the head"
                    );
                }
                if tail == 0 {
                    assert_eq!(from, whole.len(), "{case}: all that was left out");
                    assert_eq!(gap_in(&log), None, "{case}");
                    continue;
                }
                assert_eq!(gap_in(&log), Some((omitted, lines)), "{case}");
                let end = after_gap(&log);
                assert_eq!(end, mask(&whole[from..]), "{case}: the end");
                assert!(stored_len(end) <= tail, "{case}");
                seen.left_out += usize::from(omitted > 0);
            }
        }
        // And it went where it was to go, each way, often enough to say so.
        assert!(
            seen.capped >= 100
                && seen.left_out >= 50
                && seen.written_again >= 10
                && seen.dropped >= 5,
            "{seen:?}"
        );
    }

    /// The end of a log past its cap is a whole number of bytes, `0` among
    /// them, a quarter of the cap where nothing says, and never more than
    /// the cap.
    #[test]
    fn the_tail_is_a_whole_number_of_bytes_and_at_most_the_cap() {
        assert_eq!(parse_log_tail(None), None);
        assert_eq!(parse_log_tail(Some("")), None);
        assert_eq!(parse_log_tail(Some("  ")), None);
        assert_eq!(parse_log_tail(Some("0")), Some(0));
        assert_eq!(parse_log_tail(Some(" 4096 ")), Some(4096));
        for wrong in ["-1", "16MiB", "1e3", "many"] {
            assert_eq!(parse_log_tail(Some(wrong)), None, "{wrong}");
        }
        assert_eq!(tail_of(None, 1000), 250);
        assert_eq!(tail_of(None, DEFAULT_LOG_TASK_CAP), 16 << 20);
        assert_eq!(tail_of(Some(0), 1000), 0);
        assert_eq!(tail_of(Some(300), 1000), 300);
        assert_eq!(tail_of(Some(5000), 1000), 1000, "no more than the cap");
        let (sink, _rx) = tail_sink(100, 1000);
        assert_eq!(sink.tail(), 100);
    }

    /// Each line dagron adds to a log is short, however long a log is, and
    /// all of them fit in what dagron-api keeps of a runner's log past twice
    /// its cap.
    #[test]
    fn every_line_dagron_adds_fits_the_floor_under_a_runner_s_log() {
        let lines = [
            log_capped_notice(usize::MAX, usize::MAX),
            log_stopped_notice(usize::MAX, usize::MAX),
            log_capped_notice(usize::MAX, 0),
            log_stopped_notice(usize::MAX, 0),
        ]
        .into_iter()
        .chain(
            [Gap::End, Gap::Rest, Gap::ToEnd, Gap::NoMore, Gap::Unkept]
                .map(|gap| gap_line(usize::MAX, usize::MAX, gap)),
        );
        for line in lines {
            assert!(line.len() < 1024, "{line}");
            assert!(line.ends_with('\n') && !line[..line.len() - 1].contains('\n'));
        }
        assert_eq!(
            gap_in(&gap_line(usize::MAX, 7, Gap::ToEnd)),
            Some((usize::MAX, 7))
        );
        for cap in [1, 1000, 1 << 20, 64 << 20] {
            assert!(dagron_core::runner::session_log_most(cap) >= 2 * cap + 2048);
        }
    }

    /// A command that writes far past its log's cap keeps its last lines:
    /// the log is its beginning, the line it was held at, the line that says
    /// what was left out, and its end; the three add up to what it wrote.
    #[tokio::test]
    async fn a_streamed_command_past_its_cap_keeps_its_last_lines() {
        let (sink, mut rx) = tail_sink(4096, 1024);
        let command = vec![
            "sh".to_string(),
            "-c".to_string(),
            "seq 1 200000".to_string(),
        ];
        let (code, out) = run_command_streaming(&command, Some(60), &[], None, &sink)
            .await
            .unwrap();
        assert_eq!(code, 0);
        let whole: String = (1..=200_000).map(|n| format!("{n}\n")).collect();
        assert_eq!(out, whole);
        let log = joined(&sent(&mut rx));
        let notice = log_capped_notice(4096, 1024);
        let (head, rest) = log.split_once(&notice).expect("the line it was held at");
        let newline = if whole[..4096].ends_with('\n') {
            ""
        } else {
            "\n"
        };
        assert_eq!(head, format!("{}{newline}", &whole[..4096]));
        let tail = after_gap(rest);
        assert!(tail.ends_with("\n200000\n") && tail.len() <= 1024, "{tail}");
        assert!(whole.ends_with(tail) && tail.as_bytes()[0] != b'\n');
        let (omitted, lines) = sink.omitted().unwrap();
        assert_eq!(4096 + omitted + tail.len(), whole.len());
        assert_eq!(lines, newlines(&whole[4096..whole.len() - tail.len()]));
    }

    /// A command's two streams, past its log's cap, are its log's in one
    /// order: no line twice, its end the last of what both wrote, and head,
    /// what was left out and end add up to all of it, however the two came.
    #[tokio::test]
    async fn both_streams_past_the_cap_keep_one_order() {
        let (sink, mut rx) = tail_sink(4096, 2048);
        let script = "seq 1 20000 | while read i; do echo \"out $i\"; echo \"err $i\" >&2; done";
        let command = vec!["sh".to_string(), "-c".to_string(), script.to_string()];
        let (code, out) = run_command_streaming(&command, Some(120), &[], None, &sink)
            .await
            .unwrap();
        assert_eq!(code, 0);
        let line = |stream: &str, i: usize| format!("{stream} {i}\n");
        assert_eq!(
            out,
            (1..=20_000).map(|i| line("out", i)).collect::<String>()
        );
        let total: usize = (1..=20_000)
            .map(|i| line("out", i).len() + line("err", i).len())
            .sum();
        let log = joined(&sent(&mut rx));
        let (head, rest) = log
            .split_once(&log_capped_notice(4096, 2048))
            .expect("the line it was held at");
        let tail = after_gap(rest);
        let (omitted, lines) = sink.omitted().unwrap();
        assert_eq!(4096 + omitted + tail.len(), total);
        let head_text = &head[..4096];
        assert_eq!(newlines(head_text) + lines + newlines(tail), 40_000);
        // Each stream's lines in order: from its first in the head, and to
        // its last in the end, which is the end of all both wrote.
        for stream in ["out", "err"] {
            let numbers = |text: &str| -> Vec<usize> {
                text.split_inclusive('\n')
                    .filter(|l| l.ends_with('\n'))
                    .filter_map(|l| l.trim_end().strip_prefix(&format!("{stream} ")))
                    .filter_map(|n| n.parse().ok())
                    .collect()
            };
            let first = numbers(head_text);
            assert!(
                first.iter().enumerate().all(|(k, n)| *n == k + 1),
                "{stream}: {first:?}"
            );
            let last = numbers(tail);
            assert!(
                last.windows(2).all(|w| w[1] == w[0] + 1)
                    && last.last().is_none_or(|n| *n == 20_000),
                "{stream}: {last:?}"
            );
            assert!(
                last.first()
                    .is_none_or(|n| first.last().is_none_or(|m| n > m)),
                "{stream}: no line twice"
            );
        }
    }

    /// A NUL is no text a database keeps: Postgres refuses the statement that
    /// holds one. What the sink sends holds none, in a chunk, in a note or in
    /// a log sent again whole; each is the mark a byte that is no text has.
    #[test]
    fn a_nul_is_sent_as_the_mark_of_a_byte_that_is_no_text() {
        assert_eq!(without_nul("a\0b\0".to_string()), "a\u{FFFD}b\u{FFFD}");
        assert_eq!(without_nul("plain\n".to_string()), "plain\n");

        let (sink, mut rx) = capped_sink(100);
        sink.append("a\0b\n");
        sink.note("the tool said: bad\0input\n");
        let log: String = sent(&mut rx).iter().map(|c| c.chunk.as_str()).collect();
        assert_eq!(log, "a\u{FFFD}b\nthe tool said: bad\u{FFFD}input\n");

        let secret = dagron_core::dag::EnvVar {
            name: "DEPLOY_KEY".to_string(),
            value: "s3cr3t-token-value".to_string(),
            value_from: Some(dagron_core::dag::SecretRef {
                secret: "deploy-key".to_string(),
                optional: false,
            }),
        };
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(
            tx,
            "t".to_string(),
            1,
            crate::redact::Redactor::from_task_env(&[secret]),
        );
        sink.seal("key=s3cr3t-token-value\0\n");
        assert_eq!(sent(&mut rx).last().unwrap().chunk, "key=***\u{FFFD}\n");
    }

    /// An output over the cap keeps its end, which is what a result is, under
    /// a line that says how much of its beginning is gone. One within the cap
    /// is as it was.
    #[test]
    fn an_output_over_the_cap_keeps_its_end() {
        assert_eq!(cap_output("all of it\n".to_string(), 10), "all of it\n");
        assert_eq!(cap_output(String::new(), 10), "");

        let output = format!("{}the result\n", "x".repeat(5000));
        let capped = cap_output(output, 11);
        let (notice, kept) = capped.split_once('\n').unwrap();
        assert_eq!(kept, "the result\n");
        assert!(
            notice.starts_with("[dagron] this attempt's output is over 11 bytes"),
            "{notice}"
        );
        assert!(
            notice.contains(ENV_LOG_TASK_CAP) && notice.contains("its first 4.8 KiB"),
            "{notice}"
        );

        // A character is not cut in two: "é" is two bytes, and the cap would
        // begin in the middle of the first.
        let capped = cap_output("ééé".to_string(), 3);
        assert_eq!(capped.split_once('\n').unwrap().1, "é");
    }

    /// The cap is a positive number of bytes. Anything else is the default
    /// and not a cap of nothing, which would keep no log of any task.
    #[test]
    fn the_cap_is_a_positive_number_of_bytes_or_the_default() {
        assert_eq!(parse_log_cap(None), DEFAULT_LOG_TASK_CAP);
        assert_eq!(parse_log_cap(Some("")), DEFAULT_LOG_TASK_CAP);
        assert_eq!(parse_log_cap(Some(" 1048576 ")), 1 << 20);
        for wrong in ["0", "-1", "64MiB", "1e6", "many"] {
            assert_eq!(parse_log_cap(Some(wrong)), DEFAULT_LOG_TASK_CAP, "{wrong}");
        }
        assert_eq!(DEFAULT_LOG_TASK_CAP, 67_108_864);
    }

    #[test]
    fn a_size_reads_as_a_person_says_it() {
        for (bytes, said) in [
            (0, "0 bytes"),
            (1023, "1023 bytes"),
            (1024, "1 KiB"),
            (1536, "1.5 KiB"),
            (DEFAULT_LOG_TASK_CAP, "64 MiB"),
            (3 << 30, "3 GiB"),
        ] {
            assert_eq!(human_bytes(bytes), said);
        }
    }

    /// A failing task's stderr is the half that says what went wrong — and on a
    /// GPU fleet it is where every CUDA error, NCCL warning and Python traceback
    /// is written. It was logged and then discarded, so the stored record (and
    /// the fault classifier that reads it) never saw any of it.
    #[tokio::test]
    async fn a_failing_command_carries_its_stderr_into_the_stored_output() {
        let ctx = ExecContext {
            command: vec![
                "sh".into(),
                "-c".into(),
                "echo step 100; echo 'RuntimeError: CUDA out of memory' >&2; exit 1".into(),
            ],
            timeout_secs: Some(10),
            docker_image: None,
            image_pull_policy: None,
            env: vec![],
            resources: None,
            service_account: None,
            isolation: None,
            workspace: None,
            services: Vec::new(),
            privileged: false,
            log_sink: None,
            identity: None,
            checkout_credential: None,
            checkout_patch: None,
            action_credentials: Vec::new(),
            forge_token: None,
            confinement: None,
            stop: Stop::default(),
        };
        let out = LocalExecutor::default().execute(&ctx).await.unwrap();
        assert!(!out.success);
        assert!(out.output.contains("step 100"), "stdout is kept: {:?}", out.output);
        assert!(out.output.contains("CUDA out of memory"), "stderr is appended: {:?}", out.output);
        // And it is now classifiable, which is the point.
        let c = dagron_core::fault::classify_text(&out.output).unwrap();
        assert_eq!(c.class, dagron_core::fault::FaultClass::GpuOom);
    }

    /// The streaming path (#17) must behave identically — a task with live logs
    /// enabled is not a task with worse diagnostics.
    #[tokio::test]
    async fn the_streaming_path_appends_stderr_on_failure_too() {
        let (tx, _rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(tx, "task-1".to_string(), 1, crate::redact::Redactor::default());
        let ctx = ExecContext {
            command: vec!["sh".into(), "-c".into(), "echo hi; echo 'Xid 79' >&2; exit 1".into()],
            timeout_secs: Some(10),
            docker_image: None,
            image_pull_policy: None,
            env: vec![],
            resources: None,
            service_account: None,
            isolation: None,
            workspace: None,
            services: Vec::new(),
            privileged: false,
            log_sink: Some(sink),
            identity: None,
            checkout_credential: None,
            checkout_patch: None,
            action_credentials: Vec::new(),
            forge_token: None,
            confinement: None,
            stop: Stop::default(),
        };
        let out = LocalExecutor::default().execute(&ctx).await.unwrap();
        assert!(!out.success);
        assert!(out.output.contains("hi"));
        assert!(out.output.contains("Xid 79"), "{:?}", out.output);
    }

    #[test]
    fn the_stderr_tail_is_bounded_and_starts_on_a_whole_line() {
        // A task dying in a loop can emit megabytes, once per attempt, into a
        // database column. The tail — not the head — because the fatal error is
        // the last thing a process prints.
        let noise = "startup banner line\n".repeat(4000);
        let err = format!("{noise}FATAL: Xid 79, GPU has fallen off the bus");
        let out = with_stderr_on_failure(1, String::new(), &err);
        assert!(out.len() <= STDERR_TAIL_LIMIT + 64, "bounded: {}", out.len());
        assert!(out.contains("Xid 79"), "the end is what is kept");
        assert!(out.starts_with("startup banner line"), "starts on a line: {:?}", &out[..40]);
    }

    #[test]
    fn a_single_oversized_line_keeps_its_end_rather_than_being_dropped() {
        // One line, no breaks, well over the limit. There is no line boundary
        // to cut on, so the tail starts mid-line — and that is the right
        // trade: the end of the line is where the error is.
        let err = format!("{}CUDA error: device-side assert triggered", "x".repeat(40_000));
        let out = with_stderr_on_failure(1, String::new(), &err);
        assert!(out.len() <= STDERR_TAIL_LIMIT + 64, "still bounded: {}", out.len());
        assert!(out.ends_with("device-side assert triggered"), "the end survives");
        // And it is still classifiable, which is the reason any of this is kept.
        assert_eq!(
            dagron_core::fault::classify_text(&out).unwrap().class,
            dagron_core::fault::FaultClass::UserCode
        );
    }

    #[test]
    fn stderr_is_not_appended_when_there_is_none_or_when_the_task_succeeded() {
        assert_eq!(with_stderr_on_failure(0, "out".into(), "noise"), "out");
        assert_eq!(with_stderr_on_failure(1, "out".into(), "   \n "), "out");
        // No stdout at all: the output is just the stderr, with no stray blank line.
        assert_eq!(with_stderr_on_failure(1, String::new(), "boom"), "boom");
    }

    /// Without an installation the workload is still fully labelled — just not
    /// claimable by a fleet sweep. That is the whole opt-in: the per-dispatch
    /// reap keeps working, and nothing sweeps on a scope nobody set.
    #[test]
    fn no_installation_still_labels_everything_else() {
        let l = ident().labels();
        assert!(!l.contains_key(LABEL_INSTALLATION), "nothing to scope a sweep by");
        assert_eq!(l.get(LABEL_MANAGED_BY).map(String::as_str), Some(MANAGED_BY));
        assert!(l.contains_key(LABEL_TASK_ID), "the per-task reap still works");
    }

    #[test]
    fn an_installation_is_carried_when_it_is_set() {
        let mut i = ident();
        i.installation = Some("prod-eu-1".into());
        assert_eq!(i.labels().get(LABEL_INSTALLATION).map(String::as_str), Some("prod-eu-1"));
    }

    /// An unusable installation contributes NO label rather than a truncated or
    /// sanitised one.
    ///
    /// A truncated value is the dangerous outcome: `prod-eu-1-very-long…` and
    /// `prod-eu-2-very-long…` can share the 63-character prefix a sanitiser
    /// would produce, and then one installation's sweep selects the other's
    /// pods — the exact cross-installation deletion the label exists to
    /// prevent. Contributing nothing leaves the workload unsweepable, which
    /// costs a leftover rather than live work.
    #[test]
    fn an_unusable_installation_contributes_no_label_rather_than_a_truncated_one() {
        for bad in ["has/slash", "has space", &"a".repeat(64), "", "-leading"] {
            let mut i = ident();
            i.installation = Some(bad.to_string());
            assert!(
                !i.labels().contains_key(LABEL_INSTALLATION),
                "{bad:?} must not become a label"
            );
            assert!(
                i.labels().contains_key(LABEL_TASK_ID),
                "{bad:?} must not cost the task label either"
            );
        }
    }

    /// The installation is not part of the per-task selector. That selector
    /// already narrows to one task id — a UUID — and adding the installation
    /// would make the per-dispatch reap silently stop working the moment an
    /// operator renamed their installation, leaving predecessors alive.
    #[test]
    fn the_per_task_selector_does_not_depend_on_the_installation() {
        let mut i = ident();
        i.installation = Some("prod-eu-1".into());
        let with = i.task_selector().expect("selector");
        i.installation = None;
        assert_eq!(with, i.task_selector().expect("selector"), "same either way");
        assert!(!with.contains("installation"));
    }

    fn labels_of(pairs: &[(&str, &str)]) -> std::collections::BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn a_well_labelled_workload_becomes_a_candidate() {
        let l = labels_of(&[
            (LABEL_MANAGED_BY, MANAGED_BY),
            (LABEL_TASK_ID, "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e"),
            (LABEL_RUN_ID, "9f8e7d6c-5b4a-4938-8271-0a1b2c3d4e5f"),
            (LABEL_ATTEMPT, "2"),
        ]);
        let w = ManagedWorkload::from_labels("pod-x".into(), Some("u-1".into()), |k| l.get(k)).expect("candidate");
        assert_eq!(w.task_id, "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e");
        assert_eq!(w.attempt.as_deref(), Some("2"));
    }

    /// The reason this validates rather than merely checking presence. A label
    /// SELECTOR constrains only the labels it names and says nothing about the
    /// rest — so a workload can match `managed-by` and `installation` exactly
    /// while carrying a task id that is empty or not a usable label value.
    ///
    /// Such a workload matches no live row, because no row ever had that id, and
    /// the sweep acts on absence — so without this it would be DELETED, which
    /// inverts the rule the rest of this module rests on.
    #[test]
    fn a_task_id_that_could_never_name_a_row_is_refused() {
        for bad in ["", "has/slash", "has space", &"a".repeat(64), "-leading"] {
            let l = labels_of(&[(LABEL_MANAGED_BY, MANAGED_BY), (LABEL_TASK_ID, bad)]);
            assert!(
                ManagedWorkload::from_labels("pod-x".into(), Some("u-1".into()), |k| l.get(k)).is_none(),
                "{bad:?} must not become a deletion candidate"
            );
        }
    }

    #[test]
    fn a_workload_with_no_task_id_at_all_is_refused() {
        let l = labels_of(&[(LABEL_MANAGED_BY, MANAGED_BY), (LABEL_ATTEMPT, "1")]);
        assert!(ManagedWorkload::from_labels("pod-x".into(), Some("u-1".into()), |k| l.get(k)).is_none());
    }

    /// `run_id` and `attempt` are for the log line, and no decision reads them,
    /// so a missing or odd value must not cost a real orphan its cleanup.
    #[test]
    fn the_informational_labels_are_not_validated() {
        let l = labels_of(&[
            (LABEL_TASK_ID, "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e"),
            (LABEL_RUN_ID, "not/a/label/value"),
        ]);
        let w = ManagedWorkload::from_labels("pod-x".into(), Some("u-1".into()), |k| l.get(k)).expect("candidate");
        assert_eq!(w.run_id.as_deref(), Some("not/a/label/value"));
        assert_eq!(w.attempt, None);
    }

    /// The pair of rules read together: what a sweep may delete is a workload
    /// whose task id is usable AND whose attempt, where present, is provably
    /// older. Neither rule covers for the other.
    #[test]
    fn identity_round_trips_from_labels_back_into_a_candidate() {
        let mut i = ident();
        i.installation = Some("prod-eu-1".into());
        let l = i.labels();
        let w = ManagedWorkload::from_labels("pod-x".into(), Some("u-1".into()), |k| l.get(k))
            .expect("what dagron writes, the sweep can read back");
        assert_eq!(w.task_id, i.task_id);
        assert_eq!(w.attempt.as_deref(), Some("3"));
    }

    /// The uid is the backend's, not a label, and it rides through untouched —
    /// including its absence, which is normal for a backend whose handle is
    /// already an identity.
    #[test]
    fn the_backends_own_identity_rides_through_unread() {
        let l = labels_of(&[(LABEL_TASK_ID, "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e")]);
        let with = ManagedWorkload::from_labels("pod-x".into(), Some("u-9".into()), |k| l.get(k))
            .expect("candidate");
        assert_eq!(with.uid.as_deref(), Some("u-9"));

        let without =
            ManagedWorkload::from_labels("c0ffee".into(), None, |k| l.get(k)).expect("candidate");
        assert_eq!(without.uid, None);
    }

    /// A service is a container, and a task here is a process of the engine's
    /// own: a task with one is refused, and nothing of it is run.
    #[tokio::test]
    async fn the_local_executor_runs_no_task_that_has_services() {
        let marker = std::env::temp_dir().join(format!(
            "dagron-local-services-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let mut ctx = ExecContext::new(
            vec![
                "sh".into(),
                "-c".into(),
                format!("touch {}", marker.display()),
            ],
            Some(10),
            None,
        );
        ctx.services = serde_json::from_value(serde_json::json!([
            { "name": "pg", "image": "postgres:16-alpine" },
        ]))
        .unwrap();
        let err = LocalExecutor::default()
            .execute(&ctx)
            .await
            .expect_err("refused")
            .to_string();
        assert!(
            err.contains("this task has a service ('pg')") && err.contains("EXECUTOR=local"),
            "{err}"
        );
        assert!(!marker.exists(), "nothing of the task ran");
    }
}
