use serde::{Deserialize, Serialize};

/// What a cancel did to a run's tasks ([`crate::db::cancel_run`]).
///
/// A cancel asks. A task nothing was running for is `cancelled` at once. One a
/// worker holds is asked to stop, and stays `running` until that worker says
/// its work is gone; while any is, the run is `running` too.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Cancelled {
    /// Tasks that are `cancelled` now: they had not started, or were waiting.
    pub cancelled: u64,
    /// Tasks asked to stop, whose work is still to be seen gone. Zero: the run
    /// has ended. More: it ends when the last of them has stopped.
    pub stopping: u64,
}

/// `create_run` refused to start a run because the named workflow already has
/// `max_active_runs` runs in flight (parity fast-win #21 — Argo #12757 /
/// Prefect deployment concurrency limits). A typed error so callers can
/// distinguish "at capacity, try later" from a real failure: the API maps it to
/// HTTP 429, the queue source requeues the message instead of dead-lettering it,
/// and the schedule/backfill loops skip the fire (backfill releases its slot and
/// retries when capacity frees).
///
/// A workflow whose runs wait their turn (`when_full: queue`, `supersede`)
/// refuses one too, once as many wait as may
/// ([`crate::dag::max_waiting_runs`]): then `waiting` says how many.
#[derive(Debug, Clone)]
pub struct MaxActiveRunsReached {
    /// The workflow (definition) name that is at its concurrency cap.
    pub name: String,
    /// The configured `max_active_runs` for the workflow.
    pub max: u32,
    /// How many runs of the workflow were in flight when admission was
    /// refused: the ones that run, and the ones that wait to.
    pub active: i64,
    /// How many of `active` wait for their turn, and have started nothing.
    pub waiting: i64,
    /// The run's `concurrency_key` when the cap is per key rather than per
    /// workflow; `active` then counts only runs sharing this key.
    pub key: Option<String>,
}

impl std::fmt::Display for MaxActiveRunsReached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "max_active_runs ({}) reached for workflow '{}'", self.max, self.name)?;
        if let Some(key) = &self.key {
            write!(f, " and concurrency_key '{key}'")?;
        }
        match self.waiting {
            0 => write!(f, " ({} active)", self.active),
            waiting => write!(
                f,
                " ({} active, {waiting} of them waiting: no more may wait)",
                self.active
            ),
        }
    }
}

impl std::error::Error for MaxActiveRunsReached {}

/// A whole rerun (`db::rerun_all`) was refused because tasks of the run still
/// owe the teardown of a remote job they left running: a deferred task that
/// was cancelled while its job ran, or failed with its handle kept.
///
/// The rerun resets each task's remote job with the rest of its result, and a
/// handle it dropped would never be swept: the job would run on, and no
/// orphan would be counted for it. So the run is refused, with nothing
/// written, until the engine has torn the jobs down or given up on them, as
/// `gc_old_runs` refuses to collect such a run. A `409` that clears itself:
/// the same rerun goes ahead once the debt is settled.
#[derive(Debug, Clone)]
pub struct TeardownOwed {
    /// The run that was not rerun.
    pub run_id: String,
    /// How many of its tasks still owe a teardown.
    pub tasks: i64,
}

impl std::fmt::Display for TeardownOwed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "run '{}' still owes the teardown of {} remote job(s) it left running; it can be \
             rerun once the engine has torn them down",
            self.run_id, self.tasks
        )
    }
}

impl std::error::Error for TeardownOwed {}

/// The kinds of thing that start a run: `run_triggers.kind`.
pub mod trigger_kind {
    /// A person, or a token of theirs, through the gateway.
    pub const MANUAL: &str = "manual";
    /// A call on the engine's own API, which has no login.
    pub const API: &str = "api";
    /// A schedule's fire: a cron entry's, or a stored schedule's.
    pub const SCHEDULE: &str = "schedule";
    /// A backfill of a schedule: a missed fire caught up, a job's date.
    pub const BACKFILL: &str = "backfill";
    /// A dataset's update (`on_datasets:`).
    pub const DATASET: &str = "dataset";
    /// A task of a parent run (`type: workflow`).
    pub const WORKFLOW: &str = "workflow";
    /// A message of the engine's source: a file, a queue, a stream.
    pub const SOURCE: &str = "source";
    /// A CI service, for an event of a repository.
    pub const CI: &str = "ci";
}

/// What started a run, as whatever started it recorded it: one row of
/// `run_triggers`, written in the run's own transaction and never updated.
///
/// A run without one predates the record, and has its trigger derived when it
/// is read (a backfill, a schedule, or else manual). A gateway sets
/// `principal` from the caller it authenticated; a caller supplies the rest
/// and is believed only if it may set a trigger at all.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RunTrigger {
    /// The kind of thing that started the run: one of [`trigger_kind`].
    pub kind: String,
    /// What submitted it: `user:<who>` or `ci:<who>` from a gateway,
    /// [`RunTrigger::ENGINE`] for a run the engine started itself, and
    /// [`RunTrigger::OPS_API`] for one asked of its own API.
    pub principal: String,
    /// Who caused it at the source: the person who asked, the pusher, a pull
    /// request's author. A parent run's, for a run its task started.
    pub actor: Option<String>,
    /// What the submitter made of the event's origin.
    pub trust: Option<String>,
    /// What it came from, where that is no repository: `schedule:<id>`,
    /// `cron:<name>`, `dataset:<uri>`, `run:<parent run>/<task>`,
    /// `source:<name>`, `dead-letter:<id>`.
    pub source: Option<String>,
    pub forge_host: Option<String>,
    /// `owner/name` on that forge.
    pub repo: Option<String>,
    #[serde(rename = "ref")]
    pub git_ref: Option<String>,
    pub sha: Option<String>,
    pub base_sha: Option<String>,
    pub pr: Option<i64>,
    /// The id of the event at its source, to find it again there: a fire's
    /// time, a backfill's date, a source's position, a CI service's event.
    pub event_id: Option<String>,
}

impl RunTrigger {
    /// The principal of a run the engine started itself.
    pub const ENGINE: &'static str = "engine";
    /// The principal of a run asked of the engine's own API, which has no
    /// login and so nobody to name.
    pub const OPS_API: &'static str = "ops-api";

    /// A run the engine started itself, of `kind`, from `source`.
    pub fn engine(kind: &str, source: impl Into<String>) -> Self {
        Self {
            kind: kind.to_string(),
            principal: Self::ENGINE.to_string(),
            source: Some(source.into()),
            ..Self::default()
        }
    }

    /// A run asked of the engine's own API.
    pub fn ops_api() -> Self {
        Self {
            kind: trigger_kind::API.to_string(),
            principal: Self::OPS_API.to_string(),
            ..Self::default()
        }
    }

    /// The same record, of the event `id` at its source.
    pub fn of_event(mut self, id: impl Into<String>) -> Self {
        self.event_id = Some(id.into());
        self
    }

    /// The same record, from `source`.
    pub fn from_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// This record, for a run that is made again from `original`'s: a
    /// resubmit, which runs what that run ran.
    ///
    /// Who asked for it is this record's to say, and it is theirs. But where
    /// the workflow came from is not changed by who asks for it again: a
    /// workflow that a fork's pull request wrote is that, whoever presses
    /// the button. So where `original` was of a class that is not trusted,
    /// this record is of that class, for the same repository and commit.
    /// A trusted run's resubmit is its caller's own, as it was.
    pub fn copying(mut self, original: Option<&RunTrigger>) -> Self {
        let class = crate::envpolicy::class_of(original);
        if let (true, Some(original)) = (crate::confine::confined(class), original) {
            self.trust = Some(class.to_string());
            self.forge_host = original.forge_host.clone();
            self.repo = original.repo.clone();
            self.git_ref = original.git_ref.clone();
            self.sha = original.sha.clone();
            self.base_sha = original.base_sha.clone();
            self.pr = original.pr;
        }
        self
    }

    /// The record of a run that `task` of `parent_run` started.
    ///
    /// What started the child is the parent's task, and its kind and source
    /// say so. Where the work came from is where the parent's did: who caused
    /// it, how far its origin is trusted, the repository and commit, the
    /// event. A rule that reads those (an approval's `not_triggerer`, a
    /// policy on trust) then reads the same for a sub-workflow as for the
    /// run that called it, and cannot be stepped round by calling one.
    ///
    /// One thing is written out that the parent's record left unsaid. A CI
    /// service's record that names no class is read as `untrusted`, and it
    /// is read so by its kind ([`crate::envpolicy::class_of`]). The child's
    /// kind is its own, so the class its parent was read as is put in words.
    pub fn child_of(parent: Option<&RunTrigger>, parent_run: &str, task: &str) -> Self {
        let inherited = parent.cloned().unwrap_or_default();
        let trust = match parent {
            Some(p) if p.trust.is_none() && p.kind == trigger_kind::CI => {
                Some(crate::envpolicy::class_of(parent).to_string())
            }
            _ => inherited.trust.clone(),
        };
        Self {
            kind: trigger_kind::WORKFLOW.to_string(),
            principal: Self::ENGINE.to_string(),
            source: Some(format!("run:{parent_run}/{task}")),
            trust,
            ..inherited
        }
    }
}

/// A trigger-time parameter value broke the workflow's `param_schema` (missing
/// `required`, outside the `enum`, not matching the `pattern`).
///
/// Typed for the same reason as [`TaskBudgetExceeded`]: it is the caller's input
/// being refused, not a malformed spec, so the API says so plainly (400 with the
/// reason) instead of wrapping it in "invalid DAG:".
#[derive(Debug, Clone)]
pub struct ParameterInvalid {
    /// The parameter that was refused.
    pub name: String,
    /// Why, in a sentence a caller can act on.
    pub reason: String,
}

impl std::fmt::Display for ParameterInvalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "parameter '{}' {}", self.name, self.reason)
    }
}

impl std::error::Error for ParameterInvalid {}

/// A run was refused because the spec's declared `budget.tasks` is smaller than
/// the number of tasks the run would create.
///
/// A typed error so a caller can tell "this spec asked for more than it
/// budgeted" from "this spec is malformed" — the first is a deliberate,
/// informative refusal and the second is a mistake, and collapsing them into
/// one 400 makes the budget look like a parse failure.
///
/// Unlike [`MaxActiveRunsReached`] this is **not** transient: the same
/// submission will be refused identically forever, so retrying is wrong and the
/// API answers 400 rather than 429.
#[derive(Debug, Clone)]
pub struct TaskBudgetExceeded {
    /// The workflow whose budget was exceeded.
    pub name: String,
    /// The declared `budget.tasks`.
    pub max: u32,
    /// How many tasks the run would actually have created.
    pub planned: u64,
}

impl std::fmt::Display for TaskBudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "workflow '{}' would create {} tasks, over its budget.tasks of {}",
            self.name, self.planned, self.max
        )
    }
}

impl std::error::Error for TaskBudgetExceeded {}

/// A run was refused because the **declared** cost of its external work exceeds
/// the spec's `budget.external_cost`.
///
/// Sibling of [`TaskBudgetExceeded`] and refused for the same reason at the same
/// moment: after expansion the sum is exact, so the run never starts rather than
/// dying partway with cluster-hours already spent.
///
/// The numbers are the author's own — `defer.cost` per task, default 1 — so this
/// error reports a declaration against a declaration. It is not a statement
/// about money the engine observed, because the engine observes none.
///
/// Non-transient, like its sibling: the same submission is refused identically
/// forever, so the API answers 400 rather than 429.
#[derive(Debug, Clone)]
pub struct ExternalBudgetExceeded {
    /// The workflow whose ceiling was exceeded.
    pub name: String,
    /// The declared `budget.external_cost`.
    pub max: u64,
    /// The summed `defer.cost` the run would have committed to.
    pub planned: u64,
    /// How many deferred tasks contributed to that sum — the difference between
    /// "one very expensive job" and "a fan-out nobody meant to write".
    pub deferred_tasks: u64,
}

impl std::fmt::Display for ExternalBudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "workflow '{}' would submit {} deferred task(s) of declared cost {}, over its \
             budget.external_cost of {}. These are the costs the spec itself declares \
             (`defer.cost`, default 1) — raise the ceiling if you meant it, or lower the \
             fan-out",
            self.name, self.deferred_tasks, self.planned, self.max
        )
    }
}

impl std::error::Error for ExternalBudgetExceeded {}

/// A run was refused because the SQLite datastore's filesystem is under the
/// free-space floor (`DAGRON_MIN_FREE_BYTES`, constrained hosts).
///
/// A full flash device does not fail cleanly: a WAL commit needs headroom,
/// and past zero the failure mode is a torn datastore, not a refused write.
/// The floor refuses *new runs* while there is still room for the runs
/// already in flight to finish and for the WAL to checkpoint — admission is
/// the one write it is safe to say no to.
///
/// Typed like [`MaxActiveRunsReached`], and for the same reason: this is a
/// capacity condition, not a fault in the submission. The ingest actor nacks
/// the message and throttles (a valid spec is never dead-lettered for a full
/// disk), the ops API answers `507 Insufficient Storage` + `Retry-After`, and
/// the engine's own creators (sub-workflow triggers, dataset fires) retry on
/// a later tick. Only the SQLite backend ever produces it — a Postgres
/// datastore's disk is not the unit's to probe.
#[derive(Debug, Clone)]
pub struct DatastoreLowOnDisk {
    /// Bytes available to this process on the datastore's filesystem.
    pub free: u64,
    /// The floor it fell under (`DAGRON_MIN_FREE_BYTES`).
    pub floor: u64,
}

impl std::fmt::Display for DatastoreLowOnDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "datastore filesystem has {} bytes free, under the DAGRON_MIN_FREE_BYTES floor of {}",
            self.free, self.floor
        )
    }
}

impl std::error::Error for DatastoreLowOnDisk {}

/// Whether `e` is a **capacity refusal** — the datastore declining a new run
/// because there is no room right now, rather than rejecting the submission
/// itself.
///
/// The distinction decides what happens to the payload. A capacity refusal is
/// a "try later": the caller nacks it back to its source, throttles, and the
/// same spec starts once a slot or some disk frees. Anything else is a fault
/// in the submission, and repeating it forever is worse than parking it, so it
/// counts toward the dead-letter threshold.
///
/// This is a function rather than a downcast at each call site because getting
/// it wrong is silent and expensive: the ingest actor used to test only
/// [`MaxActiveRunsReached`], so a [`DatastoreLowOnDisk`] refusal — the one that
/// arrives on a full flash device, in bursts, for every message at once — was
/// dead-lettered after three redeliveries, acked off the broker, and left for
/// an operator to redrive by hand. Every new capacity condition belongs here,
/// and every classifier belongs on this function.
pub fn is_capacity_refusal(e: &anyhow::Error) -> bool {
    e.downcast_ref::<MaxActiveRunsReached>().is_some()
        || e.downcast_ref::<DatastoreLowOnDisk>().is_some()
}

/// Whether the engine's wall clock was trustworthy when a record was written
/// (clock discipline on disconnected units). Stamped on every run at creation
/// from [`crate::clock::current`].
///
/// Three states, deliberately not two. `Unknown` is the honest default on a
/// host nothing has assessed, and it must stay distinguishable from
/// `Drifted` — something looked and found the clock wrong. A regulator asks
/// different questions about each, and collapsing them would turn "we never
/// checked" into a verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClockConfidence {
    /// Positive evidence of synchronisation (the host's time daemon left
    /// `DAGRON_CLOCK_SYNC_FILE` behind).
    Synced,
    /// The wall clock stepped against the monotonic clock, or read earlier
    /// than the newest record on disk at boot.
    Drifted,
    /// No assessment has been made.
    #[default]
    Unknown,
}

impl ClockConfidence {
    /// Every state, in gauge order.
    pub const ALL: [ClockConfidence; 3] = [Self::Synced, Self::Drifted, Self::Unknown];

    /// The stored / serialized spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Synced => "synced",
            Self::Drifted => "drifted",
            Self::Unknown => "unknown",
        }
    }

    /// The `scheduler_clock_confidence` gauge value: `0` synced, `1` drifted,
    /// `2` unknown — ascending in how much a reader should worry, so `> 0`
    /// is the alert expression and no rule has to enumerate the states.
    pub fn gauge(self) -> u64 {
        match self {
            Self::Synced => 0,
            Self::Drifted => 1,
            Self::Unknown => 2,
        }
    }
}

impl std::fmt::Display for ClockConfidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ClockConfidence {
    type Err = anyhow::Error;

    /// Parse the stored lowercase spelling back — exactly that spelling, so a
    /// row a different writer produced with a different casing surfaces as an
    /// error rather than being silently reinterpreted.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "synced" => Self::Synced,
            "drifted" => Self::Drifted,
            "unknown" => Self::Unknown,
            other => anyhow::bail!("unknown clock confidence '{other}'"),
        })
    }
}

/// The trigger rules a task may declare (`trigger_rule:`), deciding whether it
/// runs once all its dependencies are terminal. `all_success` is the default
/// (and the historical behavior). Unknown values are rejected at validation.
pub const TRIGGER_RULES: &[&str] =
    &["all_success", "all_done", "one_failed", "all_failed", "none_failed"];

/// The default trigger rule when a task doesn't declare one.
pub const DEFAULT_TRIGGER_RULE: &str = "all_success";

/// Given the terminal statuses of a task's direct dependencies, decide whether a
/// task with `rule` should run (`true` → become `ready`) or be skipped (`false`).
/// Called by `advance_ready_tasks` once every dependency is terminal
/// (`remaining_deps == 0`). A root task (no dependencies) always runs.
///
/// `cancelled` counts as a failure for rule purposes; `skipped` counts as a
/// non-failure (a dependency that didn't run is not a failure).
pub fn trigger_rule_ready(rule: &str, dep_statuses: &[String]) -> bool {
    if dep_statuses.is_empty() {
        return true; // roots run regardless of rule
    }
    let is_failed = |s: &String| s == "failed" || s == "cancelled";
    let is_nonfailure = |s: &String| s == "succeeded" || s == "skipped";
    match rule {
        "all_done" => true,
        "one_failed" => dep_statuses.iter().any(is_failed),
        "all_failed" => dep_statuses.iter().all(is_failed),
        "none_failed" => dep_statuses.iter().all(is_nonfailure),
        // all_success (default + unknown-safe): every dependency succeeded.
        _ => dep_statuses.iter().all(|s| s == "succeeded"),
    }
}

/// Decide whether a **failed** task attempt should be retried (fast-win #24).
///
/// `attempt` is the attempt number that just ran (1-based). The normal rule is
/// "retry while attempts remain" (`attempt < max_attempts`). The one exception:
/// a task killed by its `timeout_secs` deadline (`timed_out`) whose
/// `retry_on_timeout` is `false` is **not** retried — a deadline kill usually
/// recurs, so retrying just burns the remaining budget and delays the failure
/// (Airflow #9232). Timeout-only: a non-zero exit or backend error
/// (`timed_out == false`) always follows the normal attempts rule.
pub fn should_retry_failed(
    attempt: i64,
    max_attempts: u32,
    timed_out: bool,
    retry_on_timeout: bool,
) -> bool {
    if timed_out && !retry_on_timeout {
        return false;
    }
    attempt < max_attempts as i64
}

/// Decide whether a failed attempt should be retried **given what broke**.
///
/// The [`should_retry_failed`] rule spends the same budget on every failure,
/// which is the behaviour every scheduler ships and the reason teams write
/// bespoke bash around it: an ECC error and a NaN loss get the same three
/// attempts, so infra faults give up too early and application faults burn
/// GPU-hours proving a determinism nobody doubted.
///
/// This resolves the budget in one place, most specific first:
///
/// 1. the task's own `retry_budgets:` entry for this class (author's word);
/// 2. the class's *disposition* default — infra 5, platform 3, application 1;
/// 3. `max_attempts`, when the failure is unclassified or the class declines
///    to have an opinion (`Unknown`).
///
/// The `retry_on_timeout` carve-out from #24 still applies first and still
/// wins: a deadline kill the author opted out of retrying is not resurrected
/// by a fault class.
///
/// `budget_override` is the workflow's `retry_budgets` lookup already
/// performed by the caller (the engine holds the parsed spec; this function
/// stays free of the DAG types so it can be unit-tested and reused by the
/// autopsy tool).
pub fn should_retry_failed_with_class(
    attempt: i64,
    max_attempts: u32,
    timed_out: bool,
    retry_on_timeout: bool,
    class: Option<crate::fault::FaultClass>,
    budget_override: Option<u32>,
) -> bool {
    if timed_out && !retry_on_timeout {
        return false;
    }
    attempt < effective_budget(max_attempts, class, budget_override) as i64
}

/// The attempt budget actually in force for a failure. Split out from
/// [`should_retry_failed_with_class`] so the engine can log the number it used
/// — "not retrying, budget 1 for nan-loss" is an answer; "not retrying" is not.
pub fn effective_budget(
    max_attempts: u32,
    class: Option<crate::fault::FaultClass>,
    budget_override: Option<u32>,
) -> u32 {
    // 1. The author said so for this exact class.
    if let Some(n) = budget_override {
        return n;
    }
    // 2. The class's disposition has an opinion (0 = it does not).
    if let Some(c) = class {
        let d = c.default_budget();
        if d > 0 {
            return d;
        }
    }
    // 3. Unclassified, *or* a class that declines: the task's own budget.
    //    "Declines" is the unknown disposition, whose default is 0 — today
    //    `nccl-timeout` and `unknown`. Those reach this line just as an
    //    unmatched failure does, which is deliberate for `nccl-timeout`: an
    //    uncorroborated collective timeout is a symptom, and a symptom must not
    //    set a retry policy. So this is the pre-attribution behaviour for those
    //    three cases only — a class with a real disposition took its default at
    //    step 2 whether or not the workflow asked for one.
    max_attempts
}

/// A claimed transactional-outbox event, handed to a delivery worker. Written in
/// the same transaction as the run finalization (see `db::reap_completed_runs`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEvent {
    pub id: String,
    pub run_id: String,
    pub event_type: String,
    /// JSON event body (the delivery worker forwards this verbatim).
    pub payload: String,
    /// Delivery attempts so far (0 on first claim).
    pub attempts: i64,
    /// When the event was written (RFC 3339): when what it tells of
    /// happened, which a retry keeps.
    #[serde(default)]
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")] // JSON matches the DB TEXT value (e.g. "running")
pub enum TaskStatus {
    Pending,
    Ready,
    Running,
    /// A `type: approval` gate whose dependencies are satisfied is parked here
    /// (never claimed by a worker) until an operator approves/rejects it or its
    /// timeout auto-resolves it (fast-win #19). Non-terminal, so the run waits.
    #[serde(rename = "awaiting_approval")]
    #[sqlx(rename = "awaiting_approval")]
    AwaitingApproval,
    Succeeded,
    Failed,
    Skipped,
    Cancelled,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::AwaitingApproval => "awaiting_approval",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
        };
        write!(f, "{s}")
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = anyhow::Error;

    /// Parse the lowercase `status` TEXT value back into the enum. Used by the
    /// Postgres backend, which maps rows manually to keep `status` a plain TEXT
    /// column rather than a native Postgres enum type.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "pending" => Self::Pending,
            "ready" => Self::Ready,
            "running" => Self::Running,
            "awaiting_approval" => Self::AwaitingApproval,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "skipped" => Self::Skipped,
            "cancelled" => Self::Cancelled,
            other => anyhow::bail!("unknown task status '{other}'"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")] // JSON matches the DB TEXT value (e.g. "running")
pub enum RunStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunStatus {
    /// Whether the run has reached a terminal (finished) state. A synchronous
    /// caller waiting on a run (`POST /runs?wait=true` / `GET /runs/{id}/wait`,
    /// fast-win #15) stops polling once this is true.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        };
        write!(f, "{s}")
    }
}

// Not ops-gated: besides the ops read queries' row mapping (get_run /
// list_runs), the reconcile loop reads statuses back when it claims runs that
// ended outside the reaper (`claim_pending_completions`) and re-reads a run
// after announcing its start (`run_status`), in the lean daemon too.
impl std::str::FromStr for RunStatus {
    type Err = anyhow::Error;

    /// Parse the lowercase `status` TEXT value back into the enum — used by the
    /// Postgres backend's manual row mapping (it keeps `status` a plain TEXT
    /// column rather than a native Postgres enum type).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            other => anyhow::bail!("unknown run status '{other}'"),
        })
    }
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct TaskRun {
    pub id: String,
    pub run_id: String,
    pub name: String,
    pub status: TaskStatus,
    pub attempt: i64,
    pub remaining_deps: i64,
    pub input: Option<String>,
    pub output: Option<String>,
    pub claimed_by: Option<String>,
    pub lease_expires_at: Option<String>,
    pub version: i64,
    pub scheduled_at: Option<String>,
    pub finished_at: Option<String>,
    /// Named concurrency pool this task draws a slot from (`pool:` in the spec),
    /// or `None` for an unpooled task. The claim gates pooled tasks against the
    /// pool's configured capacity (#21). `#[sqlx(default)]` so a projection that
    /// does not select `pool` still maps (→ `None`).
    #[sqlx(default)]
    pub pool: Option<String>,
    /// Why a task is **parked**. Every park form deliberately keeps the row
    /// `running` with a NULL lease (no new status, no CHECK rebuild) — which
    /// leaves a reader unable to tell "waiting on something" from "stuck". These
    /// four are the reason, exactly one of them set on a parked row:
    /// the time sensor's deadline (#27), the HTTP sensor's endpoint, the dataset
    /// sensor's URI, and the sub-workflow trigger's child run (#23).
    /// All `#[sqlx(default)]`, so leaner projections still map.
    /// Dispatch priority among simultaneously-ready tasks (#25), and whether the
    /// row was resolved from the memoization store rather than executed (#22) —
    /// a cache hit is otherwise indistinguishable from a fast success.
    #[sqlx(default)]
    pub priority: i64,
    #[sqlx(default)]
    pub cache_hit: bool,
    #[sqlx(default)]
    pub wake_at: Option<String>,
    #[sqlx(default)]
    pub wait_url: Option<String>,
    #[sqlx(default)]
    pub wait_dataset: Option<String>,
    #[sqlx(default)]
    pub sub_run_id: Option<String>,
    /// Submission generation for a `defer:` task (migration 042/053). Dispatch
    /// injects it as `DAGRON_EXTERNAL_EPOCH` so a step can name its remote job
    /// `dagron-<task_id>-<epoch>` — stable across lease recovery (so a
    /// post-crash resubmit adopts), fresh after a deliberate re-arm (so a retry
    /// does not adopt the job that just failed).
    /// `#[sqlx(default)]` → 0 on a projection that does not select it.
    #[sqlx(default)]
    pub external_epoch: i64,
    /// Fault attribution for the attempt that failed (migration 040/050):
    /// the kebab-case [`crate::fault::FaultClass`], the line that produced it,
    /// and how much the verdict should be trusted.
    ///
    /// `None` = unclassified, which is **not** the same as
    /// `Some("unknown")`: the first means nothing has looked, the second means
    /// something looked and could not tell. The retry budget treats them
    /// identically (both fall back to `max_attempts`), but an operator chasing
    /// coverage needs to tell them apart.
    #[sqlx(default)]
    pub fault_class: Option<String>,
    #[sqlx(default)]
    pub fault_detail: Option<String>,
    #[sqlx(default)]
    pub fault_confidence: Option<String>,
    /// Who resolved an approval gate and why (migration 047/061). `decided_by` is
    /// the approver, or `timeout` when the sweep resolved it; both `None` until
    /// resolved and on every non-approval row.
    #[sqlx(default)]
    pub decided_by: Option<String>,
    #[sqlx(default)]
    pub decision_comment: Option<String>,
    /// Quota back-pressure (migration 051/066): when the current wait began,
    /// the last refusal sentence (which quota; what was requested, used and
    /// limited), and how many times the pod has been refused.
    ///
    /// Set **only while the row is `ready` and waiting on a Kubernetes
    /// namespace `ResourceQuota`**; `None` / `0` on every other row. The
    /// columns behind them are never cleared, so a projection has to guard them
    /// with `status = 'ready' AND quota_wait_version = version` rather than
    /// select them bare — otherwise a task that waited an hour ago and has been
    /// running since still reads as waiting.
    #[sqlx(default)]
    pub quota_wait_since: Option<String>,
    #[sqlx(default)]
    pub quota_wait_detail: Option<String>,
    #[sqlx(default)]
    pub quota_waits: i64,
    /// When this task was asked to stop (migration 055/073): its run was
    /// cancelled or passed its deadline, or a gang sibling failed, while a
    /// worker held it. Set **only while the row is `running`**: the work is
    /// still to be seen gone, and the row becomes `cancelled`, with this
    /// cleared, once it has been. `None` on every other row.
    #[sqlx(default)]
    pub stop_requested_at: Option<String>,
}

/// What one sweep tick did to a parked runtime fan-out barrier.
///
/// Returned rather than only logged so the engine can meter it: an expansion is
/// N new tasks appearing mid-run, which is the one thing about this feature an
/// operator watching a dashboard needs to be able to see happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutOutcome {
    /// The producer's output was read and this many instance rows were
    /// inserted. The barrier stays parked, now as the join point.
    Expanded { instances: usize },
    /// Every instance reached a terminal state and the barrier resolved with
    /// them. `succeeded` is false when an instance failed and the barrier does
    /// not `allow_failure`.
    Joined { succeeded: bool, instances: usize },
    /// The fan-out could not be resolved — unreadable output, not a JSON
    /// array, duplicate labels, or over the run's task ceiling. The barrier is
    /// `failed` with `reason` as its output; nothing was inserted.
    Failed { reason: String },
}

/// One **superseded** attempt of a task: a `repeat:` iteration whose `until`
/// had not held yet, or an attempt that failed and is being retried.
///
/// The attempt currently on the row is deliberately absent from this type and
/// from the table behind it — it lives in `task_runs.output`, whole, where the
/// live tail has always read it. These are the ones that used to be overwritten
/// and are now kept as a bounded tail (see [`crate::attempt_log`]).
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct TaskAttempt {
    pub task_id: String,
    /// 1-based, and one count for the task's life: `attempt` on its row plus
    /// the attempts of the rounds a rerun or a clear ended (`attempt_base`).
    pub attempt: i64,
    /// Why the attempt is here. `iteration` | `failed`: a worker's own retry
    /// superseded it ([`crate::attempt_log::AttemptEnd`]). `rerun` | `retry`
    /// | `clear`: a person's did ([`RunAgain`]), and `status` says how the
    /// attempt had ended.
    pub reason: String,
    /// What the task read when a rerun, a retry or a clear replaced this
    /// attempt: `failed`, `cancelled`, `succeeded`, `skipped`. `None` on a
    /// row a worker's retry wrote, whose task was not terminal.
    pub status: Option<String>,
    /// When the attempt was queued. `None` as `status` is.
    pub scheduled_at: Option<String>,
    /// The retained tail, or `None` when the attempt printed nothing.
    pub output: Option<String>,
    /// Whether output was dropped to fit the cap. Stored rather than inferred:
    /// a reader must be able to say "there was more" without knowing what the
    /// cap happened to be when the row was written.
    pub truncated: bool,
    pub finished_at: String,
}

/// What put tasks of a run back to run again. A rerun, a retry and a clear
/// are asked for through two doors, the engine's API and dagron-api's, and
/// both call the same functions (`db::rerun_from_failed`, `db::rerun_all`,
/// `db::retry_task_from_ui`, `db::clear_task_with_downstream`). Each keeps
/// the attempts it replaces, asks the concurrency key when it sets an ended
/// run running again, and writes one outbox event, in one transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunAgain {
    /// A rerun of a failed or cancelled run from its failed tasks.
    Rerun,
    /// A rerun of an ended run whole, what succeeded included (`from: all`).
    RerunAll,
    /// A retry of one failed or cancelled task.
    Retry,
    /// A clear of one completed task, and of what depends on it.
    Clear,
    /// The engine's own rerun of a run it found failed (`AUTO_RERUN`).
    AutoRerun,
}

impl RunAgain {
    /// What a replaced attempt is kept under (`task_attempts.reason`), and
    /// `by` in the event. The engine's own rerun replaces as a person's does.
    pub fn reason(self) -> &'static str {
        match self {
            RunAgain::Rerun | RunAgain::RerunAll | RunAgain::AutoRerun => "rerun",
            RunAgain::Retry => "retry",
            RunAgain::Clear => "clear",
        }
    }

    /// The outbox event that says it happened: its type and its payload.
    /// `tasks` is how many were put back, and `task_id` the one a retry or a
    /// clear was asked for.
    ///
    /// The engine's own rerun keeps the event it always wrote
    /// (`run.auto_rerun`, with its `reason`), so nothing that reads it
    /// changes. The others are `run.rerun`, with `by`; a whole rerun's also
    /// says `"from": "all"`.
    pub fn event(self, run_id: &str, tasks: u64, task_id: Option<&str>) -> (&'static str, String) {
        match self {
            RunAgain::AutoRerun => (
                "run.auto_rerun",
                serde_json::json!({
                    "run_id": run_id,
                    "reset_tasks": tasks,
                    "reason": "auto_rerun_incomplete",
                })
                .to_string(),
            ),
            by => {
                let mut payload = serde_json::json!({
                    "run_id": run_id,
                    "by": by.reason(),
                    "reset_tasks": tasks,
                });
                if let Some(task_id) = task_id {
                    payload["task_id"] = serde_json::json!(task_id);
                }
                if by == RunAgain::RerunAll {
                    payload["from"] = serde_json::json!("all");
                }
                ("run.rerun", payload.to_string())
            }
        }
    }
}

// Constructed only by the ops read API (`db::get_run`); a lean build never
// materializes it.
#[cfg_attr(not(feature = "ops"), allow(dead_code))]
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct WorkflowRun {
    pub id: String,
    pub definition_id: String,
    pub status: RunStatus,
    pub input: Option<String>,
    pub output: Option<String>,
    pub created_at: String,
    pub finished_at: Option<String>,
    /// Clock confidence at creation (migration 041/052): `synced` /
    /// `drifted` / `unknown` as [`ClockConfidence::as_str`] spells them, the
    /// signed offset (ms) behind a `drifted` verdict, and what produced it
    /// (`sync-file` / `step` / `behind-datastore`). `None` only for a row
    /// written before the columns existed. All `#[sqlx(default)]`, so every
    /// other projection of `workflow_runs` — including the API gateway's —
    /// keeps mapping without selecting them.
    #[sqlx(default)]
    pub clock_confidence: Option<String>,
    #[sqlx(default)]
    pub clock_offset_ms: Option<i64>,
    #[sqlx(default)]
    pub clock_source: Option<String>,
    /// A run that was cancelled, or passed its deadline, while a worker held
    /// one of its tasks (migration 055/073): the status it takes once that
    /// work has stopped (`cancelled` or `failed`), and when it was asked.
    /// Set **only while the run is `running`**, and cleared at its end. A run
    /// with these set holds its concurrency key like any running run.
    #[sqlx(default)]
    pub stopping_as: Option<String>,
    #[sqlx(default)]
    pub stop_requested_at: Option<String>,
    /// A run that arrived while its workflow's cap was full, and was made to
    /// wait (`when_full`; migration 060/082): when it began to, and when it
    /// was let in. A run with the first and not the second **waits**: it
    /// reads `running`, has started nothing, and holds a place in its key.
    /// Neither is set on a run that never waited.
    #[sqlx(default)]
    pub queued_at: Option<String>,
    #[sqlx(default)]
    pub admitted_at: Option<String>,
}

impl WorkflowRun {
    /// Whether the run waits for its workflow's cap, having started nothing.
    pub fn waits(&self) -> bool {
        self.status == RunStatus::Running && self.queued_at.is_some() && self.admitted_at.is_none()
    }
}

/// One row of the run-list view (v5 management API). Joins
/// `workflow_definitions` so the DAG `name` travels with the run without a
/// second lookup. A lighter projection of [`WorkflowRun`] for listing.
#[cfg(feature = "ops")]
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct RunSummary {
    pub id: String,
    pub name: String,
    pub status: RunStatus,
    pub created_at: String,
    pub finished_at: Option<String>,
    /// The status a run that is being stopped will take ([`WorkflowRun::stopping_as`]).
    #[sqlx(default)]
    pub stopping_as: Option<String>,
    /// When a run that had to wait for its workflow's cap began to, and when
    /// it was let in ([`WorkflowRun::queued_at`]).
    #[sqlx(default)]
    pub queued_at: Option<String>,
    #[sqlx(default)]
    pub admitted_at: Option<String>,
}

/// Snapshot of run/task counts grouped by status, read fresh from the datastore
/// for the `/metrics` endpoint. The DB is the source of truth, so these gauges
/// are derived from it directly rather than from in-memory bookkeeping (which a
/// crash would lose). Process-lifetime counters live in
/// [`Metrics`](crate::metrics::Metrics) alongside.
#[cfg(feature = "ops")]
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub runs_by_status: Vec<(String, i64)>,
    pub tasks_by_status: Vec<(String, i64)>,
    /// Total rows in the dead-letter store (poison submissions parked for review).
    pub dead_letters: i64,
    /// Ready backlog per runner class (count + oldest wait) — the signal that a
    /// class no live scheduler serves is silently aging (runner segmentation).
    pub ready_by_class: Vec<ReadyClassBacklog>,
    /// Runs created in the last [`RECENT_RUNS_WINDOW_SECS`], grouped by
    /// workflow, environment and status. Bounded by the window rather than the
    /// whole table: a run carries its own definition row, so an unbounded
    /// per-workflow count is a join of two tables that only ever grow.
    pub recent_runs: Vec<RecentRuns>,
    /// Tasks that are not finished yet, as `(workflow, status, count)`.
    pub active_tasks: Vec<(String, String, i64)>,
    /// Dead letters grouped by the source that produced them.
    pub dead_letters_by_source: Vec<DeadLetterSource>,
    /// Tasks waiting on a Kubernetes namespace quota, and when the
    /// longest-waiting one was first refused (RFC-3339; `None` when none is).
    /// See [`crate::quota_wait`].
    pub quota_waiting: i64,
    pub quota_wait_oldest_since: Option<String>,
    /// Runs that wait for their workflow's cap (`when_full`), having started
    /// nothing, and when the one that has waited longest began to (RFC-3339;
    /// `None` when none waits).
    pub runs_waiting: i64,
    pub runs_waiting_oldest_since: Option<String>,
    /// Tasks whose logs the log store's GC is to look at (`log_sweeps`), how
    /// many of them its last removal failed for, and when the oldest was
    /// recorded (RFC-3339; `None` when none waits).
    pub log_sweeps_waiting: i64,
    pub log_sweeps_failing: i64,
    pub log_sweeps_oldest_marked_at: Option<String>,
    /// How old the history in this snapshot is, in seconds: the finished
    /// runs and tasks, and the recent runs that have finished
    /// ([`MetricsHistory`]), read less often than the rest. 0 where it was
    /// read with it.
    pub history_age_secs: f64,
}

/// The part of the `/metrics` gauges that grows with what has run: the runs
/// and tasks that have finished, by status, and the runs of the last
/// [`RECENT_RUNS_WINDOW_SECS`] that have. Read apart from the rest of the
/// [`MetricsSnapshot`], and cached longer.
#[cfg(feature = "ops")]
#[derive(Debug, Clone, Default)]
pub struct MetricsHistory {
    /// Runs in a status not in [`LIVE_RUN_STATUSES`], by status.
    pub runs_by_status: Vec<(String, i64)>,
    /// Tasks in a status not in [`LIVE_TASK_STATUSES`], by status.
    pub tasks_by_status: Vec<(String, i64)>,
    /// The recent runs in a status not in [`LIVE_RUN_STATUSES`].
    pub recent_runs: Vec<RecentRuns>,
}

/// The statuses of a run that has not finished, whose counts, and recent
/// runs, every read of the `/metrics` gauges takes. The others are the
/// history's.
#[cfg(feature = "ops")]
pub const LIVE_RUN_STATUSES: [&str; 2] = ["pending", "running"];

/// The statuses of a task that has not finished, as [`LIVE_RUN_STATUSES`]
/// are a run's.
#[cfg(feature = "ops")]
pub const LIVE_TASK_STATUSES: [&str; 4] = ["pending", "ready", "running", "awaiting_approval"];

#[cfg(feature = "ops")]
impl MetricsSnapshot {
    /// This snapshot, of what has not finished, with `history` read beside
    /// it, `age_secs` old: what one read of the whole datastore gives. The
    /// statuses of the two do not meet, so each status is counted once.
    pub fn with_history(mut self, history: &MetricsHistory, age_secs: f64) -> Self {
        self.runs_by_status
            .extend(history.runs_by_status.iter().cloned());
        self.tasks_by_status
            .extend(history.tasks_by_status.iter().cloned());
        self.recent_runs.extend(history.recent_runs.iter().cloned());
        self.history_age_secs = age_secs;
        self
    }
}

/// How far back the per-workflow run gauges look, in seconds (24 hours).
#[cfg(feature = "ops")]
pub const RECENT_RUNS_WINDOW_SECS: i64 = 86_400;

/// One `(workflow, environment, status)` cell of the recent-run counts.
#[cfg(feature = "ops")]
#[derive(Debug, Clone, Default)]
pub struct RecentRuns {
    pub workflow: String,
    /// The run's named `environment:`, or `None` when it declared none.
    pub environment: Option<String>,
    pub status: String,
    pub count: i64,
}

/// Dead letters from one source: how many are parked and when the oldest
/// arrived (`first_seen_at`, RFC-3339).
#[cfg(feature = "ops")]
#[derive(Debug, Clone, Default)]
pub struct DeadLetterSource {
    pub source: String,
    pub count: i64,
    pub oldest_first_seen_at: Option<String>,
}

#[cfg(feature = "ops")]
impl DeadLetterSource {
    /// Seconds the oldest dead letter from this source has been parked.
    pub fn oldest_age_secs(&self, now: chrono::DateTime<chrono::Utc>) -> i64 {
        self.oldest_first_seen_at
            .as_deref()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds().max(0))
            .unwrap_or(0)
    }
}

/// Per-runner-class dispatch backlog: how many `ready` tasks are waiting and
/// when the oldest became eligible. In a segmented fleet (`RUNNER_CLASSES`) a
/// class every scheduler is restricted away from drains nowhere — its
/// `oldest_scheduled_at` just ages. Surfaced as `/metrics` gauges and watched
/// by the engine's stale-ready alert loop.
#[cfg(feature = "ops")]
#[derive(Debug, Clone, Serialize)]
pub struct ReadyClassBacklog {
    pub runner_class: String,
    pub count: i64,
    /// `scheduled_at` of the oldest ready task in the class (RFC-3339).
    pub oldest_scheduled_at: Option<String>,
}

#[cfg(feature = "ops")]
impl ReadyClassBacklog {
    /// Seconds the oldest ready task in this class has been eligible, clamped
    /// at 0 (a future `scheduled_at` — a retry backoff — is not a wait).
    pub fn oldest_age_secs(&self, now: chrono::DateTime<chrono::Utc>) -> i64 {
        self.oldest_scheduled_at
            .as_deref()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds().max(0))
            .unwrap_or(0)
    }
}

/// A parked poison submission (v4 dead-letter routing). Surfaced by the
/// management API for inspection / redrive / discard.
#[cfg(feature = "ops")]
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct DeadLetter {
    pub id: String,
    pub payload: String,
    pub error: String,
    pub source: String,
    pub failures: i64,
    pub first_seen_at: String,
    pub last_error_at: String,
}

/// A due workflow schedule (v7 UI). Read by the engine's leadership-gated
/// schedule loop: the workflow `spec` to fire + the `cron_expr` to advance from.
#[cfg(feature = "ops")]
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DueSchedule {
    pub id: String,
    pub cron_expr: String,
    pub spec: String,
    /// The nominal (scheduled) time this row was due at — injected into the
    /// fired run as its `scheduled_time` parameter so tasks can reference their
    /// logical date (the data-interval idiom).
    pub next_fire_at: String,
    /// IANA timezone the `cron_expr` is evaluated in (default `UTC`). The engine
    /// advances `next_fire_at` in this zone so DST shifts the UTC instant, not
    /// the wall-clock fire time.
    pub timezone: String,
    /// Optional per-fire conditional gate (`when:`). Evaluated against the
    /// scheduled time's calendar fields before firing; a false result skips the
    /// fire (only `next_fire_at` advances). `None` = always fire.
    pub when_expr: Option<String>,
    /// Optional auto-stop expression (`stopStrategy`). Evaluated against this
    /// schedule's run outcome counts before firing; a true result stops the
    /// schedule instead of firing. `None` = never auto-stop.
    pub stop_expr: Option<String>,
}

/// A schedule opted into automatic catch-up (QW3 auto-catchup). Read by the engine's
/// leadership-gated auto-backfill loop: it enumerates the cron fire-times missed
/// between `last_fired_at` (bounded by `catchup_window_secs`) and now, then
/// materializes each through the `schedule_backfills` dedup ledger.
///
/// `catchup_window_secs` / `catchup_max_runs` are per-schedule overrides of the
/// engine's env defaults — `None` (NULL column) means "use the engine default".
#[cfg(feature = "enterprise")]
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CatchupSchedule {
    pub id: String,
    pub cron_expr: String,
    pub spec: String,
    /// IANA timezone the `cron_expr` is evaluated in (default `UTC`) — the
    /// catch-up sweep enumerates missed fire-times in this zone.
    pub timezone: String,
    /// Last time the normal schedule loop fired this row (the catch-up lower
    /// bound). `None` before the schedule has ever fired.
    pub last_fired_at: Option<String>,
    /// How far back to look for misses, overriding the engine default.
    pub catchup_window_secs: Option<i64>,
    /// Per-sweep run cap, overriding the engine default.
    pub catchup_max_runs: Option<i64>,
}

/// A first-class backfill job (fast-win #18). A durable,
/// listable, cancellable object the engine's leadership-gated loop *paces* — it
/// fires a few due fire-times per tick, advances `cursor`, and completes when the
/// range is exhausted or `max_runs` is reached. `spec`/`cron_expr`/`timezone` are
/// snapshotted at creation so the job is stable if the schedule later changes.
#[cfg(feature = "ops")]
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct BackfillJob {
    pub id: String,
    pub schedule_id: String,
    pub cron_expr: String,
    pub timezone: String,
    /// Denormalized workflow spec snapshot the paced runs are created from.
    /// Skipped from the API projection (large; the schedule already carries it).
    #[serde(skip_serializing)]
    pub spec: String,
    pub range_from: String,
    pub range_to: String,
    /// Exclusive lower bound for the next fire-time enumeration; advances as the
    /// job paces. Starts at `range_from`, so (like the synchronous backfill) the
    /// job fires the cron times strictly after `range_from` through `range_to`.
    pub cursor: String,
    /// `running` | `completed` | `cancelled`.
    pub status: String,
    pub max_runs: i64,
    /// Total fire-times in the range (progress denominator).
    pub requested: i64,
    /// Runs fired so far.
    pub fired: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// A dataset trigger the sweep just claimed (data-aware scheduling). Produced by
/// `db::claim_due_dataset_triggers` when a registered workflow's subscribed
/// dataset(s) recorded new updates: the claimer CAS-advanced the subscription
/// cursor(s), so exactly one scheduler owns this fire (HA-safe, no leadership).
/// The engine then loads the workflow's spec and creates the run; if run
/// creation is refused (e.g. `max_active_runs`), it restores the cursors from
/// `advanced` so the fire retries on a later sweep.
#[derive(Debug, Clone)]
pub struct DatasetFire {
    /// Registered workflow to fire.
    pub workflow_name: String,
    /// The subscribed dataset whose update triggered this fire (with several
    /// fresh datasets in one sweep, the first — updates coalesce into one run).
    pub trigger_uri: String,
    /// Every cursor this claim advanced: `(uri, previous, new)`. Kept for
    /// rollback when the fire cannot create its run.
    pub advanced: Vec<(String, i64, i64)>,
}

/// One parked external job due for a poll — what a sweep needs to ask the
/// remote system "is it done?" without reading the workflow spec again.
///
/// Everything here is on the task row, which is the point: the handle is
/// durable state, not an in-memory registration. Any scheduler that survives
/// can poll a job any other scheduler submitted, because the row is the whole
/// contract.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ExternalPark {
    /// The parked `task_runs.id`.
    pub id: String,
    /// The task's name, for the lineage row a `produces:` records. Read off the
    /// row the sweep already fetched rather than through a second query.
    pub name: String,
    /// The run it belongs to — the poller's identity for logs and, later, for
    /// resolving a connection.
    pub run_id: String,
    /// Which poller resolves it (`defer.kind`).
    pub external_kind: String,
    /// The remote job's opaque identity.
    pub external_handle: String,
    /// Where to reach it, pinned at submit. `None` when the handle suffices.
    pub external_endpoint: Option<String>,
    /// Submission generation — part of the remote job's name.
    pub external_epoch: i64,
    /// When to stop waiting, or `None` for no ceiling beyond the run's own.
    pub external_deadline_at: Option<String>,
    /// The task's persisted spec JSON (`task_runs.input`), which carries
    /// `defer.http` — the URL, headers and predicates the built-in poller needs.
    ///
    /// Read from the row rather than from the workflow registry on purpose: this
    /// is the spec that actually ran, so editing the workflow while a six-hour
    /// job is in flight cannot redirect the poll of a job that was submitted
    /// against the old definition.
    pub input: Option<String>,
}

/// One row that owes a remote teardown: it still holds an `external_handle`
/// but is no longer parked, so a cancelled (or otherwise terminated) task left
/// a job running on someone else's cluster.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ExternalCancel {
    pub id: String,
    pub run_id: String,
    pub external_kind: String,
    pub external_handle: String,
    pub external_endpoint: Option<String>,
    pub external_epoch: i64,
    /// Failed teardown attempts so far. Only a genuine failure increments it —
    /// a sweep that found no poller for the kind tried nothing.
    pub external_cancel_attempts: i64,
    /// The task's persisted spec JSON (`task_runs.input`), which carries the
    /// `defer.http.cancel` block the teardown sweep sends. Read off the row for
    /// the same reason [`ExternalPark::input`] is.
    pub input: Option<String>,
    /// When the task went terminal. The wall-clock give-up bound is measured
    /// from here, so teardown terminates regardless of how many replicas can or
    /// cannot reach the remote system.
    pub finished_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(statuses: &[&str]) -> Vec<String> {
        statuses.iter().map(|s| s.to_string()).collect()
    }

    /// What started a sub-workflow's run is its parent's task, and the record
    /// says so. Where the work came from is where the parent's did, and the
    /// record keeps all of that: a rule on a run's origin reads the same for
    /// the child as for the run that called it.
    #[test]
    fn a_sub_workflow_s_record_names_its_task_and_keeps_where_the_work_came_from() {
        let parent = RunTrigger {
            kind: trigger_kind::CI.to_string(),
            principal: "ci:svc@example.com".to_string(),
            actor: Some("octocat".to_string()),
            trust: Some("untrusted".to_string()),
            source: None,
            forge_host: Some("github.com".to_string()),
            repo: Some("acme/widgets".to_string()),
            git_ref: Some("refs/pull/7/head".to_string()),
            sha: Some("a1".to_string()),
            base_sha: Some("b2".to_string()),
            pr: Some(7),
            event_id: Some("e-1".to_string()),
        };
        let child = RunTrigger::child_of(Some(&parent), "run-1", "deploy");
        assert_eq!(
            (
                child.kind.as_str(),
                child.principal.as_str(),
                child.source.as_deref()
            ),
            ("workflow", "engine", Some("run:run-1/deploy"))
        );
        // Everything else is the parent's.
        let rest = |t: &RunTrigger| RunTrigger {
            kind: String::new(),
            principal: String::new(),
            source: None,
            ..t.clone()
        };
        assert_eq!(rest(&child), rest(&parent));

        // And a child of that child says the same again, of its own parent.
        let grandchild = RunTrigger::child_of(Some(&child), "run-2", "notify");
        assert_eq!(grandchild.source.as_deref(), Some("run:run-2/notify"));
        assert_eq!(rest(&grandchild), rest(&parent));

        // A parent with no record (a run from before every run had one)
        // leaves its child with what the child itself knows.
        let orphan = RunTrigger::child_of(None, "run-0", "t");
        assert_eq!(
            orphan,
            RunTrigger::engine(trigger_kind::WORKFLOW, "run:run-0/t")
        );
    }

    #[test]
    fn the_engine_s_own_records_say_what_made_the_run() {
        let fire = RunTrigger::engine(trigger_kind::SCHEDULE, "schedule:s1")
            .of_event("2026-01-01T00:00:00+00:00");
        assert_eq!(
            (
                fire.kind.as_str(),
                fire.principal.as_str(),
                fire.source.as_deref(),
                fire.event_id.as_deref()
            ),
            (
                "schedule",
                "engine",
                Some("schedule:s1"),
                Some("2026-01-01T00:00:00+00:00")
            )
        );
        assert_eq!((fire.actor, fire.trust, fire.repo), (None, None, None));

        let asked = RunTrigger::ops_api().from_source("dead-letter:d1");
        assert_eq!(
            (
                asked.kind.as_str(),
                asked.principal.as_str(),
                asked.source.as_deref()
            ),
            ("api", "ops-api", Some("dead-letter:d1"))
        );
    }

    #[test]
    fn trigger_rules_decide_correctly() {
        // Roots (no deps) always run, whatever the rule.
        assert!(trigger_rule_ready("one_failed", &[]));
        assert!(trigger_rule_ready("all_success", &[]));

        // all_success: every dep must have succeeded.
        assert!(trigger_rule_ready("all_success", &v(&["succeeded", "succeeded"])));
        assert!(!trigger_rule_ready("all_success", &v(&["succeeded", "failed"])));
        assert!(!trigger_rule_ready("all_success", &v(&["succeeded", "skipped"])));

        // all_done: runs regardless of outcomes.
        assert!(trigger_rule_ready("all_done", &v(&["succeeded", "failed", "skipped"])));

        // one_failed: runs iff at least one dep failed (cancelled counts as failed).
        assert!(trigger_rule_ready("one_failed", &v(&["succeeded", "failed"])));
        assert!(trigger_rule_ready("one_failed", &v(&["cancelled"])));
        assert!(!trigger_rule_ready("one_failed", &v(&["succeeded", "succeeded"])));

        // all_failed: every dep failed.
        assert!(trigger_rule_ready("all_failed", &v(&["failed", "cancelled"])));
        assert!(!trigger_rule_ready("all_failed", &v(&["failed", "succeeded"])));

        // none_failed: no dep failed (succeeded/skipped ok).
        assert!(trigger_rule_ready("none_failed", &v(&["succeeded", "skipped"])));
        assert!(!trigger_rule_ready("none_failed", &v(&["succeeded", "failed"])));

        // Unknown rule falls back to all_success semantics (safe default).
        assert!(trigger_rule_ready("bogus", &v(&["succeeded"])));
        assert!(!trigger_rule_ready("bogus", &v(&["failed"])));
    }

    #[test]
    fn retry_gate_honors_retry_on_timeout() {
        // Non-timeout failure: the normal attempts rule, regardless of the flag.
        assert!(should_retry_failed(1, 3, false, true), "attempts remain → retry");
        assert!(should_retry_failed(1, 3, false, false), "flag is timeout-only");
        assert!(!should_retry_failed(3, 3, false, true), "no attempts left → fail");

        // Timeout failure with retry_on_timeout=true: normal attempts rule.
        assert!(should_retry_failed(1, 3, true, true), "timeout still retried by default");
        assert!(!should_retry_failed(3, 3, true, true), "…but not past max_attempts");

        // Timeout failure with retry_on_timeout=false: never retried, even with
        // attempts to spare — the #24 behavior.
        assert!(!should_retry_failed(1, 3, true, false), "opted-out timeout fails at once");
        assert!(!should_retry_failed(1, 10, true, false));
    }

    #[test]
    fn an_unclassified_failure_keeps_the_tasks_own_budget() {
        // The compatibility guarantee: every workflow that predates fault
        // attribution retries exactly as it did before.
        assert_eq!(effective_budget(3, None, None), 3);
        assert!(should_retry_failed_with_class(1, 3, false, true, None, None));
        assert!(!should_retry_failed_with_class(3, 3, false, true, None, None));
    }

    #[test]
    fn an_infra_fault_gets_a_wider_budget_than_the_task_asked_for() {
        use crate::fault::FaultClass;
        // max_attempts=1 (the default: no retries) but the GPU fell off the
        // bus. The task did nothing wrong and the retry lands elsewhere.
        assert_eq!(effective_budget(1, Some(FaultClass::GpuFallenOffBus), None), 5);
        assert!(should_retry_failed_with_class(
            1, 1, false, true,
            Some(FaultClass::GpuFallenOffBus), None
        ));
    }

    #[test]
    fn an_application_fault_is_not_retried_however_generous_max_attempts_is() {
        use crate::fault::FaultClass;
        // The money case. max_attempts=10 on a thousand-GPU job whose loss went
        // NaN: nine more attempts reproduce it exactly, at full cluster cost.
        assert_eq!(effective_budget(10, Some(FaultClass::NanLoss), None), 1);
        assert!(!should_retry_failed_with_class(
            1, 10, false, true,
            Some(FaultClass::NanLoss), None
        ));
    }

    #[test]
    fn an_uncorroborated_collective_timeout_falls_back_rather_than_guessing() {
        use crate::fault::FaultClass;
        // NcclTimeout has an Unknown disposition on purpose: it must neither
        // claim the infra budget nor be starved to one attempt. It defers.
        assert_eq!(effective_budget(4, Some(FaultClass::NcclTimeout), None), 4);
        assert!(should_retry_failed_with_class(
            2, 4, false, true,
            Some(FaultClass::NcclTimeout), None
        ));
    }

    #[test]
    fn an_explicit_per_class_budget_beats_the_disposition_default() {
        use crate::fault::FaultClass;
        // The author overrides both directions: infra down to 2, and an
        // application fault they genuinely want retried once more.
        assert_eq!(effective_budget(3, Some(FaultClass::GpuEcc), Some(2)), 2);
        assert_eq!(effective_budget(1, Some(FaultClass::NanLoss), Some(2)), 2);
        assert!(!should_retry_failed_with_class(
            2, 3, false, true,
            Some(FaultClass::GpuEcc), Some(2)
        ));
    }

    #[test]
    fn the_no_retry_on_timeout_carve_out_still_wins_over_any_class() {
        use crate::fault::FaultClass;
        // #24's rule is about a deadline the author declared un-retryable. A
        // fault class must not resurrect it, even an infrastructural one.
        assert!(!should_retry_failed_with_class(
            1, 5, true, false,
            Some(FaultClass::GpuFallenOffBus), None
        ));
        // ...and a timeout that *is* retryable still respects the class budget.
        assert!(should_retry_failed_with_class(
            1, 1, true, true,
            Some(FaultClass::FabricIb), None
        ));
    }

    #[test]
    fn a_zero_budget_stops_the_task_without_underflowing() {
        use crate::fault::FaultClass;
        // An author writing `retry_budgets: { nan-loss: 0 }` means "never run
        // this again", including the attempt that just ran.
        assert_eq!(effective_budget(5, Some(FaultClass::NanLoss), Some(0)), 0);
        assert!(!should_retry_failed_with_class(
            1, 5, false, true,
            Some(FaultClass::NanLoss), Some(0)
        ));
    }

    /// Clock confidence round-trips through its stored spelling, defaults to
    /// `unknown` (a value, never a guess), rejects any other spelling, and
    /// orders its gauge so `> 0` means "not synced".
    #[test]
    fn clock_confidence_round_trips_and_gauges() {
        for c in ClockConfidence::ALL {
            assert_eq!(c.as_str().parse::<ClockConfidence>().unwrap(), c);
            assert_eq!(c.to_string(), c.as_str());
        }
        assert_eq!(ClockConfidence::default(), ClockConfidence::Unknown);
        assert!("Synced".parse::<ClockConfidence>().is_err(), "stored spelling is lowercase, exactly");
        assert!("".parse::<ClockConfidence>().is_err());
        assert_eq!(ClockConfidence::Synced.gauge(), 0);
        assert!(ClockConfidence::Drifted.gauge() > 0);
        assert!(ClockConfidence::Unknown.gauge() > ClockConfidence::Drifted.gauge());
        assert_eq!(serde_json::to_string(&ClockConfidence::Drifted).unwrap(), "\"drifted\"");
    }

    /// The disk-floor refusal names both numbers and the knob, so whoever
    /// reads a 507 (or a nack log line) knows what to free or raise — and it
    /// travels through anyhow as a typed error, which is how every caller
    /// tells it from a real failure.
    #[test]
    fn datastore_low_on_disk_names_both_numbers_and_the_knob() {
        let e = DatastoreLowOnDisk { free: 12_345, floor: 67_108_864 };
        let msg = e.to_string();
        assert!(msg.contains("12345"), "{msg}");
        assert!(msg.contains("67108864"), "{msg}");
        assert!(msg.contains("DAGRON_MIN_FREE_BYTES"), "{msg}");
        let any = anyhow::Error::new(e);
        assert!(any.downcast_ref::<DatastoreLowOnDisk>().is_some());
        assert!(any.downcast_ref::<MaxActiveRunsReached>().is_none());
    }
}

/// A ready entry of a path cache (`path_caches`): one a task may restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathCacheEntry {
    pub id: String,
    pub cache_key: String,
    pub object_key: String,
    pub size_bytes: Option<i64>,
}

/// An entry a task was given a URL to save to: `pending` until the engine's
/// cache sweep has seen its task succeed and its manifest in the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPathCache {
    pub id: String,
    /// The forge host and repository: the unit of the quota.
    pub repo: String,
    /// The repository and ref: where it is read from and written to.
    pub scope: String,
    pub prefix: String,
    pub version: String,
    pub object_key: String,
    pub task_id: String,
}

/// An artifact a workflow's job uploaded (`actions/upload-artifact`), kept
/// by the engine's results service for its run, by its name, until it
/// expires. `size_bytes` and `sha256` are `None` until the upload is
/// complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowArtifact {
    pub id: String,
    /// The number the job knows it by: below 2^53.
    pub artifact_id: i64,
    pub run_id: String,
    /// The task that uploaded it.
    pub task_id: String,
    pub name: String,
    pub object_key: String,
    pub size_bytes: Option<i64>,
    pub sha256: Option<String>,
    pub created_at: String,
    pub expires_at: String,
}

/// A pending entry, with where its task stands: `None` when the task's row
/// is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingPathCache {
    pub id: String,
    pub object_key: String,
    pub created_at: String,
    pub task_status: Option<String>,
}

/// A request credential given to a task that asks for an identity
/// (`oidc_requests`): dagron-api signs a token for it while its task runs.
/// The credential itself is never kept, only its SHA-256.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcRequest {
    pub token_sha256: String,
    pub run_id: String,
    pub task_id: String,
    pub task_name: String,
    /// The attempt it was given for, 1 for the first.
    pub attempt: i64,
    pub audiences: Vec<String>,
    /// The task's environment, where it names one.
    pub environment: Option<String>,
    pub expires_at: String,
}

/// What a remote runner said of a task, taken by an engine to apply as
/// it applies a result of its own workers ([`crate::db::take_runner_results`]).
#[derive(Debug, Clone)]
pub struct RunnerResult {
    pub task_id: String,
    /// The claim the runner held, and its fencing token: what the result is
    /// applied under, so that it changes nothing once the claim is gone.
    pub claimed_by: String,
    pub fence: i64,
    /// `succeeded`, `failed`, `timed_out`, `backpressure` or `cancelled`
    /// ([`crate::runner::Outcome`]).
    pub outcome: String,
    pub exit_code: Option<i32>,
    pub output: Option<String>,
    /// The task's row, as it is now: `None` for a task that is gone.
    pub run_id: Option<String>,
    /// The attempt before the claim (the claim counted one more).
    pub attempt: i64,
    /// The task's stored spec.
    pub input: Option<String>,
}
