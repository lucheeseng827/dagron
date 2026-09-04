use serde::{Deserialize, Serialize};

/// `create_run` refused to start a run because the named workflow already has
/// `max_active_runs` runs in flight (parity fast-win #21 — Argo #12757 /
/// Prefect deployment concurrency limits). A typed error so callers can
/// distinguish "at capacity, try later" from a real failure: the API maps it to
/// HTTP 429, the queue source requeues the message instead of dead-lettering it,
/// and the schedule/backfill loops skip the fire (backfill releases its slot and
/// retries when capacity frees).
#[derive(Debug, Clone)]
pub struct MaxActiveRunsReached {
    /// The workflow (definition) name that is at its concurrency cap.
    pub name: String,
    /// The configured `max_active_runs` for the workflow.
    pub max: u32,
    /// How many runs of the workflow were active when admission was refused.
    pub active: i64,
}

impl std::fmt::Display for MaxActiveRunsReached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "max_active_runs ({}) reached for workflow '{}' ({} active)",
            self.max, self.name, self.active
        )
    }
}

impl std::error::Error for MaxActiveRunsReached {}

/// A run was refused because the spec's declared `budget.tasks` is smaller than
/// the number of tasks the run would create (G-AG3).
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

// Used only by the Postgres backend's manual row mapping for the ops read
// queries (get_run / list_runs); gated so a lean build carries no dead code.
#[cfg(feature = "ops")]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn v(statuses: &[&str]) -> Vec<String> {
        statuses.iter().map(|s| s.to_string()).collect()
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
