use serde::{Deserialize, Serialize};

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
    /// Last time the normal schedule loop fired this row (the catch-up lower
    /// bound). `None` before the schedule has ever fired.
    pub last_fired_at: Option<String>,
    /// How far back to look for misses, overriding the engine default.
    pub catchup_window_secs: Option<i64>,
    /// Per-sweep run cap, overriding the engine default.
    pub catchup_max_runs: Option<i64>,
}
