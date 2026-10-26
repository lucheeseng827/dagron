//! Datastore facade.
//!
//! The datastore *is* the scheduler: every query here is a state-machine
//! transition on `task_runs`, and the lease + version + status contract is the
//! whole correctness story. Exactly one backend is compiled in, selected by
//! Cargo feature:
//!
//! * `sqlite` (default) — zero-infra single-node path; optimistic CAS claim.
//! * `postgres` — v2 horizontal scale; `FOR UPDATE SKIP LOCKED` claim +
//!   `LISTEN/NOTIFY` event-driven wake for N coordination-free workers.
//!
//! Both backends expose the identical API — `Pool`, `Waker`, and the `create_run`
//! / `claim_ready` / `mark_task_*` / `is_run_complete` family — so `main.rs` and
//! the reconcile loop are backend-agnostic. Switching backends is a feature flag
//! plus a connection string, exactly as the design intends.

#[cfg(all(feature = "sqlite", feature = "postgres"))]
compile_error!("enable only one DB backend: `sqlite` or `postgres`, not both");

#[cfg(not(any(feature = "sqlite", feature = "postgres")))]
compile_error!("enable a DB backend: build with `--features sqlite` (default) or `--features postgres`");

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::*;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::*;

/// What a reconcile loop was told has changed since it last asked: which
/// runs, or that it cannot say.
///
/// A notification on the wake channel carries the run a task changed state
/// in. A loop that has taken every one of them knows which runs can have
/// ended, and looks at those alone. Where it cannot know it has heard of
/// everything, it is told so, and looks at every run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Woken {
    /// Any run may have changed. Said by a waker that has just connected,
    /// by one whose connection was lost (what was sent meanwhile is gone),
    /// by one that was told of more runs than it keeps, and on SQLite by
    /// the first take after a start and by a take that failed.
    All,
    /// A task changed state in each of these runs, and in no other that
    /// was heard of.
    Runs(Vec<String>),
}

/// The most rows a sweep that reads in batches reads in one pass: the
/// fan-out barriers, the sub-workflow triggers, the time sensors
/// and the dataset sensors. A pass that read this many says so
/// ([`Pass::full`]): the sweep is due again at once on the engine that ran
/// it, which asks for the next batch on its next tick. Where the sweep is
/// shared, its slot is handed back too, and the next engine to ask takes
/// it: the one that handed it back, unless another asks before its loop
/// next wakes, which on a quiet engine can be most of an interval away. The
/// engines told the slot was taken in that window still wait a whole
/// interval.
pub const SWEEP_BATCH: i64 = 256;

/// What one pass of a sweep that reads in batches did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pass<T> {
    /// What it resolved, one entry each.
    pub done: Vec<T>,
    /// It read a whole [`SWEEP_BATCH`]: more may be due now.
    pub full: bool,
}

/// A sweep's slot that an engine took for the window
/// ([`claim_sweep_slots`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepSlot {
    /// The sweep's name ([`crate::sweep::Sweep::name`]).
    pub name: String,
    /// One more each time an engine takes the slot. A write of its cursor, or
    /// its hand-back, holds only at the turn it was taken at, so an engine
    /// that stalled after it took the slot changes nothing once another has
    /// taken it since. 0 where there is no table of slots (SQLite): every
    /// sweep is the one engine's.
    pub turn: i64,
    /// Where the sweep got to, as the engine before wrote it: the reap's
    /// last run.
    pub cursor: Option<String>,
}

/// `values` as the list of an SQL `IN (...)`: each quoted. For constants of
/// this crate only, never for what a caller hands in.
#[cfg(feature = "ops")]
pub(crate) fn sql_list(values: &[&str]) -> String {
    values
        .iter()
        .map(|v| format!("'{v}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The count of runs not yet finished, as dagron-api's health reads it. Its
/// `IN` list is [`crate::models::LIVE_RUN_STATUSES`] as [`sql_list`] writes it,
/// word for word: on SQLite the partial index of live runs (migrations/078)
/// is usable only by a read that carries its WHERE as written.
#[cfg(feature = "ops")]
pub const LIVE_RUNS_COUNT: &str =
    "SELECT COUNT(*) FROM workflow_runs WHERE status IN ('pending', 'running')";

/// The claim-time task lease window, in seconds: `LEASE_SECS` (default 30,
/// floor 3). Read once and cached — the claim path runs every tick. The floor
/// is 3 — not 1 — because the worker heartbeat renews every floor(lease/3) s
/// with a 1 s minimum: below a 3 s window that minimum interval could no
/// longer fit two missed renewals inside the lease, and the crash-recovery
/// sweep would race healthy tasks.
///
/// This was the hard-coded `+30 seconds` in both backends' claim SQL
/// (docs/HA.md planned the knob; the low-latency profile needs it: a crashed
/// scheduler's task waits out this window before any peer reclaims it, and 30 s
/// is an outage at market open). The lease heartbeat renews by this same value,
/// and the engine derives its renew interval as a third of it, so shortening
/// the window keeps the healthy-task contract: a live task's lease always sits
/// between ⅔ and one full window in the future, and two consecutive missed
/// renewals still leave headroom before the expired-lease sweep can reclaim it.
///
/// Shared by both backends so they can never drift.
pub fn lease_secs() -> i64 {
    static LEASE_SECS: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *LEASE_SECS.get_or_init(|| {
        std::env::var("LEASE_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .unwrap_or(30)
            .max(3)
    })
}

/// A run's `run_timeout_secs` deadline once the run is let in, having waited
/// for its workflow's cap since `queued_at`: `deadline_at` moved on by as
/// long as it waited, so the budget is for what the run runs. `None` for a
/// run with no deadline.
///
/// A time that cannot be read leaves the deadline where it was: a run is not
/// given a longer budget, or none, on text nobody could read.
///
/// Shared by both backends' `admit_waiting_runs`, so they cannot drift.
pub(crate) fn deadline_after_wait(
    deadline_at: Option<&str>,
    queued_at: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<String> {
    let deadline_at = deadline_at?;
    let read = |at: &str| chrono::DateTime::parse_from_rfc3339(at).ok().map(|at| at.with_timezone(&chrono::Utc));
    let (Some(deadline), Some(queued)) = (read(deadline_at), read(queued_at)) else {
        return Some(deadline_at.to_string());
    };
    // A clock that went back while the run waited takes nothing off.
    let waited = (now - queued).max(chrono::TimeDelta::zero());
    Some(deadline.checked_add_signed(waited).unwrap_or(deadline).to_rfc3339())
}

/// The deadlines of a run that is set running again after it ended, counted
/// from `now` as `create_run` counts a new run's from when it is made:
/// `(deadline_at, alert_deadline_at)`, from its definition's
/// `run_timeout_secs` and `deadline.in`. Kept as they were, a deadline set
/// when the run was made has passed by the time it is rerun, and the deadline
/// sweep fails the run before a task of it runs again. `None` for one the
/// definition does not set, or that cannot be read: the caller then leaves
/// that column as it was.
///
/// Shared by both backends' `rearm_deadlines`, so they cannot drift.
#[cfg(feature = "ops")]
pub(crate) fn deadlines_again(
    spec: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> (Option<String>, Option<String>) {
    let (timeout, alert) = crate::dag::DagGraph::stored_deadlines(spec);
    let at = |secs: u64| {
        chrono::TimeDelta::try_seconds(i64::try_from(secs).unwrap_or(i64::MAX))
            .and_then(|budget| now.checked_add_signed(budget))
            .map(|at| at.to_rfc3339())
    };
    (timeout.and_then(at), alert.and_then(at))
}

/// Of the tasks an engine dispatched (`held`: each id with its fence), those it
/// should stop: `rows` is what the datastore has for them now, as
/// `(id, version, claimed_by, status, stop_requested_at)`.
///
/// A task is still the engine's to run while its row is `running`, claimed by
/// `worker_id` under that fence, and not asked to stop. Anything else, a row
/// that is gone included, is a claim that was lost or a stop that was asked.
///
/// Shared by both backends' `tasks_to_stop`, so they cannot drift.
#[allow(clippy::type_complexity)]
pub(crate) fn not_held(
    worker_id: &str,
    held: &[(String, i64)],
    rows: Vec<(String, i64, Option<String>, String, Option<String>)>,
) -> Vec<String> {
    let rows: std::collections::HashMap<String, (i64, Option<String>, String, Option<String>)> =
        rows.into_iter().map(|(id, v, by, status, asked)| (id, (v, by, status, asked))).collect();
    held.iter()
        .filter(|(id, fence)| {
            !rows.get(id).is_some_and(|(version, by, status, asked)| {
                status == "running"
                    && by.as_deref() == Some(worker_id)
                    && version == fence
                    && asked.is_none()
            })
        })
        .map(|(id, _)| id.clone())
        .collect()
}

#[cfg(all(test, feature = "ops"))]
mod live_count_tests {
    use super::{sql_list, LIVE_RUNS_COUNT};

    /// The health count's list is the live statuses' as the other live
    /// reads write it, so that all of them carry the live index's WHERE.
    #[test]
    fn the_live_count_lists_the_live_statuses() {
        let list = sql_list(&crate::models::LIVE_RUN_STATUSES);
        assert!(
            LIVE_RUNS_COUNT.ends_with(&format!("IN ({list})")),
            "{LIVE_RUNS_COUNT}"
        );
    }
}

#[cfg(test)]
mod wait_tests {
    use super::deadline_after_wait;

    #[test]
    fn a_deadline_is_moved_on_by_as_long_as_the_run_waited() {
        let at = |text: &str| chrono::DateTime::parse_from_rfc3339(text).unwrap().with_timezone(&chrono::Utc);
        let queued = "2026-10-13T10:00:00+00:00";
        let deadline = "2026-10-13T10:30:00+00:00";
        let let_in = at("2026-10-13T10:07:30Z");
        // No deadline, none after.
        assert_eq!(deadline_after_wait(None, queued, let_in), None);
        // Thirty minutes from when it was made is thirty from when it is let in.
        assert_eq!(
            deadline_after_wait(Some(deadline), queued, let_in).as_deref(),
            Some("2026-10-13T10:37:30+00:00")
        );
        // A clock that went back takes nothing off the budget.
        assert_eq!(
            deadline_after_wait(Some(deadline), queued, at("2026-10-13T09:00:00Z")).as_deref(),
            Some(deadline)
        );
        // What cannot be read is left as it was.
        assert_eq!(deadline_after_wait(Some(deadline), "then", let_in).as_deref(), Some(deadline));
        assert_eq!(deadline_after_wait(Some("soon"), queued, let_in).as_deref(), Some("soon"));
    }
}

#[cfg(test)]
mod stop_tests {
    use super::not_held;

    #[test]
    fn a_task_is_held_while_its_row_is_running_under_its_claim_and_not_asked_to_stop() {
        let held = vec![
            ("held".to_string(), 3),
            ("asked".to_string(), 3),
            ("cancelled".to_string(), 3),
            ("taken".to_string(), 3),
            ("reclaimed".to_string(), 3),
            ("gone".to_string(), 3),
        ];
        let row = |id: &str, version: i64, by: Option<&str>, status: &str, asked: Option<&str>| {
            (id.to_string(), version, by.map(str::to_string), status.to_string(), asked.map(str::to_string))
        };
        let rows = vec![
            row("held", 3, Some("w"), "running", None),
            row("asked", 3, Some("w"), "running", Some("2026-10-08T00:00:00+00:00")),
            row("cancelled", 3, None, "cancelled", None),
            row("taken", 3, Some("another"), "running", None),
            row("reclaimed", 4, Some("w"), "running", None),
        ];
        assert_eq!(not_held("w", &held, rows), ["asked", "cancelled", "taken", "reclaimed", "gone"]);
    }
}

/// Bytes available to this process on the filesystem holding `path` — the
/// probe behind the SQLite free-disk admission floor (`DAGRON_MIN_FREE_BYTES`)
/// and the engine's constrained-host boot report.
///
/// ONE implementation, here, because two callers need it (the admission
/// check in `db::sqlite` and the engine's `pressure` module) and a second
/// statvfs wrapper is exactly the kind of thing that drifts. This one reports
/// `f_bavail` — blocks available to an *unprivileged* process; root's
/// reserved blocks are not headroom the daemon can use — times `f_frsize`,
/// the fragment size `f_bavail` is counted in (not `f_bsize`).
///
/// `path` may be a directory or a file on the filesystem of interest and must
/// exist. Errors are the OS's (`ENOENT`, `EACCES`, …). Callers on the
/// admission path FAIL OPEN on an error: a probe that cannot run is not
/// evidence that the disk is full.
#[cfg(unix)]
pub fn free_bytes(path: &std::path::Path) -> std::io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    // SAFETY: `statvfs` reads a NUL-terminated path (`CString` guarantees the
    // terminator) and writes into the zeroed, correctly-sized struct passed by
    // pointer; nothing escapes the call.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut st) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // The field types are libc-specific — u64 on glibc x86_64, but u32 /
    // c_ulong on 32-bit and the BSDs — so the widening casts are real on the
    // targets the edge profile is built for (an armv7 musl gateway among
    // them), even though clippy on this host sees them as no-ops.
    #[allow(clippy::unnecessary_cast)]
    let free = (st.f_bavail as u64).saturating_mul(st.f_frsize as u64);
    Ok(free)
}

/// Non-unix hosts have no statvfs; the probe reports `Unsupported` and every
/// caller fails open, exactly as it would on a probe error.
#[cfg(not(unix))]
pub fn free_bytes(_path: &std::path::Path) -> std::io::Result<u64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "free-disk probe (statvfs) is only implemented for unix hosts",
    ))
}

/// The variables `env_from` gives a task, as the engine and dagron-api (for a
/// remote runner's task) both give them: each the status of the task it
/// names, or the last line that task printed, as the run has them now. The
/// inner `Err` is why one cannot be given, which fails the task that asked
/// for it rather than running it with a value it was not given.
///
/// A last line is read from the end of what the task printed, and only
/// that much of it is fetched: an output may be megabytes. A line that does
/// not fit is refused, and so is one whose start is not in what was fetched,
/// which is the same line seen from its end.
pub async fn given_from_tasks(
    pool: &Pool,
    run_id: &str,
    env_from: &[crate::dag::EnvFrom],
) -> anyhow::Result<Result<Vec<crate::dag::EnvVar>, String>> {
    // A line's room and a little more, so that one too long is seen to be.
    const FETCHED: usize = crate::dag::LAST_LINE_MAX + 1024;
    let mut given: Vec<crate::dag::EnvVar> = Vec::with_capacity(env_from.len());
    let mut together = 0;
    for from in env_from {
        let Some((status, tail)) = task_outcome(pool, run_id, &from.task, FETCHED).await?
        else {
            return Ok(Err(format!(
                "{} reads task '{}', and this run has no task of that name",
                from.name, from.task
            )));
        };
        let value = match from.read {
            crate::dag::Read::Status => status,
            crate::dag::Read::LastLine => {
                let line = crate::dag::last_line(&tail);
                let cut = tail.chars().count() >= FETCHED;
                let begins_before = cut && std::ptr::eq(line.as_ptr(), tail.as_ptr());
                if line.len() > crate::dag::LAST_LINE_MAX || begins_before {
                    return Ok(Err(format!(
                        "{} reads the last line of task '{}', which is longer than the {} bytes \
                         a task hands on",
                        from.name,
                        from.task,
                        crate::dag::LAST_LINE_MAX
                    )));
                }
                line.to_string()
            }
        };
        together += value.len();
        if together > crate::dag::ENV_FROM_MAX {
            return Ok(Err(format!(
                "what this task reads of earlier tasks is more than the {} bytes a task is \
                 given together, at {}",
                crate::dag::ENV_FROM_MAX,
                from.name
            )));
        }
        given.push(crate::dag::EnvVar {
            name: from.name.clone(),
            value,
            value_from: None,
        });
    }
    Ok(Ok(given))
}

/// A consumer of the transactional outbox: the webhook, the
/// fleet uplink, or a part of the engine's own end-of-run work. Each keeps its
/// own delivery state of every event it is handed (`outbox_deliveries`), so
/// one sink taking an event never takes it from another, and a slow sink holds
/// back nothing but itself.
///
/// A sink is handed the events written after it was first configured (its
/// first claim, or [`register_outbox_sink`]), and of those, the ones of
/// `event_types` (every one where it is empty). One that `adopts_legacy` is
/// handed, too, as it is first configured, the events the single-consumer
/// outbox still held pending when this release's migration ran
/// (`outbox_epoch`): the webhook and the uplink, which drained it before.
/// Not those written since, which a sink configured later never saw.
#[derive(Debug, Clone, Copy)]
pub struct OutboxSink<'a> {
    pub name: &'a str,
    pub event_types: &'a [&'a str],
    pub adopts_legacy: bool,
}

/// dagron-ci's reporter's sink, which dagron-api hands out to it
/// (`POST /api/outbox/ci/claim`): what a run's commit statuses follow, a
/// task beginning, waiting on an approval and ending, the run ending, and
/// the run running again.
pub const CI_SINK: OutboxSink<'static> = OutboxSink {
    name: "ci",
    event_types: &[
        "task.started",
        "task.waiting",
        "task.finished",
        "run.completed",
        "run.rerun",
        "run.auto_rerun",
    ],
    adopts_legacy: false,
};

/// How long a task's event is kept ([`prune_task_events`]): a notice of a
/// change is worth something while a look can follow it, and the outbox is
/// otherwise never pruned.
pub const OUTBOX_TASK_EVENTS_KEPT_SECS: i64 = 24 * 3600;

/// How long after its last claim a sink that asks for task events is still
/// written them (`outbox_sinks.wants_tasks_until`): a sink that stops
/// claiming for good stops costing each task a row this long after.
pub const OUTBOX_TASK_EVENTS_SECS: i64 = 24 * 3600;

impl OutboxSink<'_> {
    /// Whether the sink asks for `task.started` / `task.finished`, which the
    /// datastore writes only while such a sink claims. A sink of every type
    /// (`event_types` empty) is not handed them: they are written for those
    /// that name them.
    pub fn wants_tasks(&self) -> bool {
        self.event_types.iter().any(|t| t.starts_with("task."))
    }
}

/// Whether a sink that asks for task events is to be written them for
/// longer, its `wants_tasks_until` being `until`: where it would run out
/// within the day less an hour, so that a sink claiming every few seconds
/// writes it once an hour.
pub(crate) fn outbox_task_events_renew(
    until: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<String> {
    let soon = (now + chrono::TimeDelta::seconds(OUTBOX_TASK_EVENTS_SECS - 3600)).to_rfc3339();
    match until {
        Some(u) if u >= soon.as_str() => None,
        _ => Some((now + chrono::TimeDelta::seconds(OUTBOX_TASK_EVENTS_SECS)).to_rfc3339()),
    }
}

/// The webhook delivery worker's sink (`dagron-events`), which drained the
/// single-consumer outbox before.
pub const WEBHOOK_SINK: OutboxSink<'static> = OutboxSink {
    name: "webhook",
    event_types: &[],
    adopts_legacy: true,
};

/// The fleet uplink's sink (`dagron-fleet-link`), which drained the
/// single-consumer outbox before.
pub const FLEET_SINK: OutboxSink<'static> = OutboxSink {
    name: "fleet",
    event_types: &[],
    adopts_legacy: true,
};

/// How late an event may commit after the time it carries (`created_at`)
/// and still be handed to every sink: a sink enrols from this far behind its
/// cursor. The transactions that write an event are short; this is also the
/// skew between the clocks of the replicas that write them.
pub const OUTBOX_LATE_COMMIT_SECS: i64 = 300;

/// The most events one enrolment hands a sink. A sink reconnecting after a
/// week catches up a batch at a time.
pub const OUTBOX_ENROLL_ROWS: i64 = 1000;

/// Where a sink enrols from: its cursor less [`OUTBOX_LATE_COMMIT_SECS`],
/// and never before `since`. Both are RFC 3339; one that does not parse is
/// taken as it is.
pub(crate) fn outbox_enrol_from(since: &str, cursor: &str) -> String {
    let from = chrono::DateTime::parse_from_rfc3339(cursor)
        .map(|c| (c - chrono::TimeDelta::seconds(OUTBOX_LATE_COMMIT_SECS)).to_rfc3339())
        .unwrap_or_else(|_| cursor.to_string());
    std::cmp::max(from, since.to_string())
}

/// A sink's cursor once it has enrolled `enrolled` events, the latest at
/// `latest`: that, and where fewer than a batch were found, also the time
/// before which every event has committed (`now` less
/// [`OUTBOX_LATE_COMMIT_SECS`]), so that a sink of few event types does not
/// scan again what others wrote since.
pub(crate) fn outbox_next_cursor(
    latest: Option<String>,
    enrolled: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<String> {
    let settled = (enrolled < OUTBOX_ENROLL_ROWS)
        .then(|| (now - chrono::TimeDelta::seconds(OUTBOX_LATE_COMMIT_SECS)).to_rfc3339());
    std::cmp::max(latest, settled)
}

/// Whether a sink's cursor is to be written: it moves on, and either an
/// event was enrolled or it moves on by a minute. A sink with nothing new
/// then writes nothing on most of its drains, which on SQLite each take the
/// database's one write lock; its window is at most a minute wider for it.
pub(crate) fn outbox_cursor_moves(cursor: &str, next: &str, enrolled: i64) -> bool {
    if next <= cursor {
        return false;
    }
    if enrolled > 0 {
        return true;
    }
    match (
        chrono::DateTime::parse_from_rfc3339(cursor),
        chrono::DateTime::parse_from_rfc3339(next),
    ) {
        (Ok(c), Ok(n)) => (n - c).num_seconds() >= 60,
        _ => true,
    }
}

/// The event types of `sink` as the enrolment binds them: a JSON array,
/// empty for every type but a task's (see [`OutboxSink::wants_tasks`]).
pub(crate) fn outbox_types_json(sink: &OutboxSink<'_>) -> String {
    serde_json::to_string(sink.event_types).expect("a list of strings serialises")
}

/// A caller's claim on an `Idempotency-Key`, as the run it submits is to
/// resolve it (`create_run_claimed`, on either backend): the row of `run_idempotency`
/// the caller inserted before the run existed, with `run_id` still NULL.
#[derive(Debug, Clone, Copy)]
pub struct IdempotencyClaim<'a> {
    /// Whose key it is.
    pub principal: &'a str,
    pub key: &'a str,
    /// `created_at` as this claim wrote it. A claim that goes unresolved for
    /// long enough may be taken over by a later submit of the key, which
    /// writes its own: this is what tells the two apart.
    pub claimed_at: &'a str,
}

/// The claim a run came to resolve was not this submit's any more: a later
/// submit of the key took it over while this one was in flight, or it was
/// released. Raised inside `create_run_inner`, so the run's transaction
/// aborts and nothing of the run is kept, and turned into `Ok(None)` by
/// `create_run_claimed`. It never leaves this module.
#[derive(Debug)]
struct IdempotencyClaimLost;

impl std::fmt::Display for IdempotencyClaimLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the idempotency claim is no longer this submit's")
    }
}

impl std::error::Error for IdempotencyClaimLost {}

/// The dead letter a redrive came to consume was already gone: someone else
/// redrove or discarded it first. Raised by the claim inside both backends'
/// `create_run_inner`, so the run's transaction aborts before anything is
/// written, and turned into `Ok(None)` by `create_run_from_dead_letter`. It
/// never leaves this module.
#[derive(Debug)]
struct DeadLetterGone;

impl std::fmt::Display for DeadLetterGone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("dead letter already redriven or discarded")
    }
}

impl std::error::Error for DeadLetterGone {}

/// Which records of `log_sweeps` a claim takes (the log store's GC): a
/// superseded writing's alone, which may hold text a seal masked and so goes
/// first, or any that is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimFirst {
    Superseded,
    Any,
}

/// A record of `log_sweeps` as a claim took it: a task whose log store holds
/// what may be no reader's, why it was recorded (`gone`, `superseded`, or
/// `orphan`, by hand), the record's stamp as the claim left it, which only a
/// finish at that stamp may finish, and how many removals of it failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSweep {
    pub task_id: String,
    pub run_id: Option<String>,
    pub reason: String,
    pub stamp: i64,
    pub tries: i64,
}

/// What waits in `log_sweeps`: how many records, how many of them failed
/// last time, and when the oldest was made (the database's clock, RFC 3339).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogSweepsWaiting {
    pub waiting: i64,
    pub failing: i64,
    pub oldest_marked_at: Option<String>,
}

#[cfg(test)]
mod free_bytes_tests {
    use super::free_bytes;

    /// The probe reports real headroom for a filesystem that exists, and the
    /// figure is the unprivileged view — never more than the whole disk.
    #[cfg(unix)]
    #[test]
    fn reports_headroom_for_the_temp_dir() {
        let dir = std::env::temp_dir();
        let free = free_bytes(&dir).expect("temp dir is on a real filesystem");
        assert!(free > 0, "a usable temp dir has some room");
        // A file on the same filesystem probes the same figure (modulo
        // concurrent writes): the caller may hand either.
        let file = dir.join(format!("m54-free-bytes-{}", uuid::Uuid::new_v4()));
        std::fs::write(&file, b"x").unwrap();
        let via_file = free_bytes(&file).unwrap();
        std::fs::remove_file(&file).unwrap();
        assert!(via_file.abs_diff(free) < 256 * 1024 * 1024, "dir={free} file={via_file}");
    }

    /// A path that does not exist is an error, not zero: zero would read as
    /// "full" and make a typo'd datastore path refuse every run.
    #[test]
    fn a_missing_path_is_an_error_not_zero() {
        let missing = std::env::temp_dir().join(format!("m54-no-such-{}", uuid::Uuid::new_v4()));
        assert!(free_bytes(&missing).is_err());
    }
}
