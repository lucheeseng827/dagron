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
}
