//! Control mutations: cancel a run, retry a dead task, submit a new DAG.
//!
//! These write via `write_pool` and fire `pg_notify('task_events', run_id)` in
//! the same transaction so any open SSE stream (this replica or another) reflects
//! the change immediately.
//!
//! NOTE: the cancel/retry/submit SQL is intentionally inlined here rather than
//! reused from the dagron engine crate. The engine's `db.rs` carries a
//! `compile_error!` that fires when both the `sqlite` and `postgres` features are
//! enabled; depending on the engine crate from this Postgres-only service would
//! trip that under Cargo's workspace feature unification. The inlined statements
//! mirror engine `db::postgres::{cancel_run, retry_task_from_ui, create_run}` —
//! keep them in sync if the engine schema changes. The submitted task `input`
//! JSON must match the engine's `dag::TaskSpec` shape (see `TaskSpecInput`).

use std::collections::{HashMap, HashSet};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use petgraph::algo::is_cyclic_directed;
use petgraph::graph::DiGraph;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::state::AppState;

// ── Cancel ────────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct CancelResponse {
    pub cancelled: u64,
}

/// `POST /api/runs/:id/cancel` — flip all non-terminal tasks to cancelled.
pub async fn cancel_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<CancelResponse>, StatusCode> {
    // 404 if the run doesn't exist.
    let exists: Option<String> =
        sqlx::query_scalar("SELECT id FROM workflow_runs WHERE id = $1")
            .bind(&id)
            .fetch_optional(&state.write_pool)
            .await
            .map_err(internal)?;
    if exists.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = state.write_pool.begin().await.map_err(internal)?;
    let cancelled = sqlx::query(
        "UPDATE task_runs
         SET status = 'cancelled', finished_at = $1, claimed_by = NULL
         WHERE run_id = $2 AND status IN ('pending', 'ready', 'running')",
    )
    .bind(&now)
    .bind(&id)
    .execute(&mut *tx)
    .await
    .map_err(internal)?
    .rows_affected();

    if cancelled > 0 {
        // Reflect the cancellation on the run itself so GET /api/runs and
        // GET /api/runs/:id don't read a stale 'running'/'pending' status.
        sqlx::query(
            "UPDATE workflow_runs
             SET status = 'cancelled', finished_at = $1
             WHERE id = $2 AND status IN ('pending', 'running')",
        )
        .bind(&now)
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;

        notify(&mut tx, &id).await?;
    }
    tx.commit().await.map_err(internal)?;
    Ok(Json(CancelResponse { cancelled }))
}

// ── Retry ─────────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct RetryResponse {
    pub retried: bool,
}

/// `POST /api/runs/:id/tasks/:tid/retry` — resurrect a failed/cancelled task.
/// 409 if the task isn't in a retryable state; 404 if it doesn't belong to the run.
pub async fn retry_task(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
) -> Result<Json<RetryResponse>, StatusCode> {
    // Scope-check: the task must belong to this run.
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status FROM task_runs WHERE id = $1 AND run_id = $2",
    )
    .bind(&tid)
    .bind(&id)
    .fetch_optional(&state.write_pool)
    .await
    .map_err(internal)?;
    let Some(status) = status else {
        return Err(StatusCode::NOT_FOUND);
    };
    if status != "failed" && status != "cancelled" {
        return Err(StatusCode::CONFLICT);
    }

    let mut tx = state.write_pool.begin().await.map_err(internal)?;
    let updated = sqlx::query(
        "UPDATE task_runs
         SET status = 'ready', claimed_by = NULL, lease_expires_at = NULL,
             scheduled_at = NULL, finished_at = NULL, output = NULL, version = version + 1
         WHERE id = $1 AND status IN ('failed', 'cancelled')",
    )
    .bind(&tid)
    .execute(&mut *tx)
    .await
    .map_err(internal)?
    .rows_affected();

    if updated == 0 {
        // Raced to terminal-but-not-retryable between the check and the update.
        tx.rollback().await.map_err(internal)?;
        return Err(StatusCode::CONFLICT);
    }

    // Re-arm a run already finalized failed so the reconcile loop re-engages.
    sqlx::query(
        "UPDATE workflow_runs SET status = 'running', finished_at = NULL
         WHERE id = $1 AND status = 'failed'",
    )
    .bind(&id)
    .execute(&mut *tx)
    .await
    .map_err(internal)?;

    notify(&mut tx, &id).await?;
    tx.commit().await.map_err(internal)?;
    Ok(Json(RetryResponse { retried: true }))
}

#[derive(Serialize)]
pub struct ClearResponse {
    pub run_id: String,
    pub task_id: String,
    pub cleared: u64,
}

/// `POST /api/runs/:id/tasks/:tid/clear` — clear + downstream: reset a
/// completed task and every terminal task that transitively depends on it to
/// `pending`, recompute `remaining_deps`, and re-arm the run so the reconcile
/// loop re-runs just that sub-DAG. Unlike `retry_task` (a single failed task),
/// this re-runs a still-`succeeded` node on demand and cascades to its
/// downstream. Mirrors engine `db::clear_task_with_downstream` (keep in sync).
/// 404 unknown run/task, 409 task not in a completed state.
pub async fn clear_task(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
) -> Result<Json<ClearResponse>, (StatusCode, String)> {
    let mut tx = state.write_pool.begin().await.map_err(internal_msg)?;

    // Lock the target row and re-check inside the tx so a concurrent clear/retry
    // serializes rather than double-resetting.
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status FROM task_runs WHERE id = $1 AND run_id = $2 FOR UPDATE",
    )
    .bind(&tid)
    .bind(&id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal_msg)?;
    let Some(status) = status else {
        return Err((StatusCode::NOT_FOUND, format!("task '{tid}' not found in run '{id}'")));
    };
    if !matches!(status.as_str(), "succeeded" | "failed" | "skipped" | "cancelled") {
        return Err((
            StatusCode::CONFLICT,
            format!("task '{tid}' is not in a clearable (completed) state"),
        ));
    }

    let now = chrono::Utc::now().to_rfc3339();

    // Reset the target + its transitive downstream cone (terminal tasks only).
    let cleared = sqlx::query(
        "WITH RECURSIVE cone(id) AS (
             SELECT id FROM task_runs WHERE id = $1 AND run_id = $2
             UNION
             SELECT td.dependent_id FROM task_dependencies td
             JOIN cone c ON td.dependency_id = c.id
         )
         UPDATE task_runs
         SET status='pending', attempt=0, claimed_by=NULL, lease_expires_at=NULL,
             output=NULL, finished_at=NULL, scheduled_at=$3, version=version+1
         WHERE id IN (SELECT id FROM cone)
           AND status IN ('succeeded','failed','skipped','cancelled')",
    )
    .bind(&tid)
    .bind(&id)
    .bind(&now)
    .execute(&mut *tx)
    .await
    .map_err(internal_msg)?
    .rows_affected();

    // Recompute remaining_deps for the reset frontier in a second statement so it
    // sees the post-reset statuses (a just-reset dependency must count as
    // outstanding — a single self-referential UPDATE can't guarantee that). Only
    // *non-terminal* deps count: a terminal failed/cancelled upstream outside the
    // reset cone never decrements again, so counting it would strand the task.
    sqlx::query(
        "UPDATE task_runs
         SET remaining_deps = (
                 SELECT COUNT(*) FROM task_dependencies d
                 JOIN task_runs dep ON dep.id = d.dependency_id
                 WHERE d.dependent_id = task_runs.id
                   AND dep.status NOT IN ('succeeded','skipped','failed','cancelled')
             )
         WHERE run_id = $1 AND status = 'pending'",
    )
    .bind(&id)
    .execute(&mut *tx)
    .await
    .map_err(internal_msg)?;

    // Re-arm a run that had finished so the reconcile loop resumes.
    sqlx::query(
        "UPDATE workflow_runs SET status='running', finished_at=NULL, output=NULL
         WHERE id=$1 AND status IN ('succeeded','failed','cancelled')",
    )
    .bind(&id)
    .execute(&mut *tx)
    .await
    .map_err(internal_msg)?;

    notify(&mut tx, &id).await.map_err(|s| (s, "internal server error".to_string()))?;
    tx.commit().await.map_err(internal_msg)?;

    tracing::info!(run_id = %id, task_id = %tid, cleared, "task cleared with downstream");
    Ok(Json(ClearResponse { run_id: id, task_id: tid, cleared }))
}

#[derive(Serialize)]
pub struct ApprovalResponse {
    pub run_id: String,
    pub task_id: String,
    pub resolution: String,
}

/// `POST /api/runs/:id/tasks/:tid/approve` — approve a `type: approval` gate (#19):
/// the task succeeds and its dependents advance. Mirrors engine
/// `db::resolve_approval`. 404 unknown run/task, 409 not awaiting approval.
pub async fn approve_task(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
) -> Result<Json<ApprovalResponse>, (StatusCode, String)> {
    resolve_approval(&state, &id, &tid, true).await
}

/// `POST /api/runs/:id/tasks/:tid/reject` — reject a gate: the task fails and its
/// `all_success` dependents skip. Same status codes as approve.
pub async fn reject_task(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
) -> Result<Json<ApprovalResponse>, (StatusCode, String)> {
    resolve_approval(&state, &id, &tid, false).await
}

async fn resolve_approval(
    state: &AppState,
    id: &str,
    tid: &str,
    approve: bool,
) -> Result<Json<ApprovalResponse>, (StatusCode, String)> {
    let now = chrono::Utc::now().to_rfc3339();
    let (status, output) =
        if approve { ("succeeded", "approved") } else { ("failed", "rejected") };
    let mut tx = state.write_pool.begin().await.map_err(internal_msg)?;
    let rows = sqlx::query(
        "UPDATE task_runs SET status = $1, finished_at = $2, output = $3
         WHERE id = $4 AND run_id = $5 AND status = 'awaiting_approval'",
    )
    .bind(status)
    .bind(&now)
    .bind(output)
    .bind(tid)
    .bind(id)
    .execute(&mut *tx)
    .await
    .map_err(internal_msg)?
    .rows_affected();

    if rows == 0 {
        tx.rollback().await.map_err(internal_msg)?;
        // Disambiguate: unknown task → 404, existing-but-not-awaiting → 409.
        let known: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM task_runs WHERE id = $1 AND run_id = $2")
                .bind(tid)
                .bind(id)
                .fetch_optional(&state.read_pool)
                .await
                .map_err(internal_msg)?;
        return if known.is_some() {
            Err((StatusCode::CONFLICT, format!("task '{tid}' is not awaiting approval")))
        } else {
            Err((StatusCode::NOT_FOUND, format!("task '{tid}' not found in run '{id}'")))
        };
    }

    // Terminal transition → decrement dependents so their trigger_rule evaluates.
    sqlx::query(
        "UPDATE task_runs SET remaining_deps = remaining_deps - 1
         WHERE id IN (
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1
         ) AND status = 'pending'",
    )
    .bind(tid)
    .execute(&mut *tx)
    .await
    .map_err(internal_msg)?;

    notify(&mut tx, id).await.map_err(|s| (s, "internal server error".to_string()))?;
    tx.commit().await.map_err(internal_msg)?;

    let resolution = if approve { "approved" } else { "rejected" };
    tracing::info!(run_id = %id, task_id = %tid, resolution, "approval gate resolved");
    Ok(Json(ApprovalResponse {
        run_id: id.to_string(),
        task_id: tid.to_string(),
        resolution: resolution.to_string(),
    }))
}

// ── Rerun (cascade resume from failure) ────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
pub struct RerunBody {
    /// Rerun mode. Only `failed` (the default) is supported; `task:<id>` is reserved.
    #[serde(default)]
    from: Option<String>,
    /// Deep-merged into each reset task's stored TaskSpec `input` (behind the
    /// `enterprise` feature). Accepted by all builds so clients get an explicit
    /// 400 rather than a silent no-op when `params` is supplied against a build
    /// without the feature.
    #[serde(default)]
    params: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub struct RerunResponse {
    pub run_id: String,
    pub rerun: u64,
}

/// `POST /api/runs/:id/rerun` — cascade rerun a failed/cancelled run from its
/// failure frontier: every failed/cancelled task resets to `pending` and the run
/// re-arms, while succeeded tasks stay intact. `remaining_deps` is recomputed so
/// the frontier becomes ready and tasks behind a reset dependency wait for it to
/// re-succeed. Mirrors engine `db::postgres::rerun_from_failed` (keep in sync),
/// plus the optional `params` input override (QW4). 404 unknown / 409 not
/// rerunnable / 400 bad mode or non-object params.
pub async fn rerun_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<RerunBody>>,
) -> Result<Json<RerunResponse>, (StatusCode, String)> {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    if let Some(from) = &body.from {
        if from != "failed" {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("unsupported rerun mode '{from}'; only 'failed' is supported"),
            ));
        }
    }
    // Non-enterprise builds accept the field (so serde doesn't silently discard
    // it) but must reject it explicitly so callers know params had no effect.
    #[cfg(not(feature = "enterprise"))]
    if body.params.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            "`params` rerun override requires the `enterprise` feature".to_string(),
        ));
    }
    #[cfg(feature = "enterprise")]
    if let Some(p) = &body.params {
        if !p.is_object() {
            return Err((StatusCode::BAD_REQUEST, "params must be a JSON object".to_string()));
        }
    }

    let mut tx = state.write_pool.begin().await.map_err(internal_msg)?;

    // Lock the run row and re-check its state inside the transaction so concurrent
    // reruns serialize: the loser blocks here until the winner commits, then reads
    // 'running' and gets a 409 instead of committing a false success.
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = $1 FOR UPDATE")
            .bind(&id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(internal_msg)?;
    let Some(status) = status else {
        return Err((StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    };
    if status != "failed" && status != "cancelled" {
        return Err((
            StatusCode::CONFLICT,
            format!("run '{id}' is not in a rerunnable state (failed/cancelled)"),
        ));
    }

    let now = chrono::Utc::now().to_rfc3339();

    // Behind the `enterprise` feature: capture the broken cone's specs before
    // reset and deep-merge `params` into each task's `input`. The reset UPDATE
    // below never touches `input`
    // (it carries the command), so these per-row overrides are applied alongside it.
    #[cfg(feature = "enterprise")]
    let overrides: Vec<(String, String)> = if let Some(params) = body.params.as_ref() {
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT id, input FROM task_runs WHERE run_id = $1 AND status IN ('failed','cancelled')",
        )
        .bind(&id)
        .fetch_all(&mut *tx)
        .await
        .map_err(internal_msg)?;
        let mut out = Vec::new();
        for (task_id, input) in rows {
            let Some(input) = input else { continue };
            // Merge into the stored TaskSpec's `input` field; skip rows whose
            // persisted spec this service can't parse rather than failing the rerun.
            if let Ok(mut spec) = serde_json::from_str::<serde_json::Value>(&input) {
                if let Some(obj) = spec.as_object_mut() {
                    let slot = obj.entry("input").or_insert(serde_json::Value::Null);
                    deep_merge(slot, params.clone());
                    out.push((task_id, spec.to_string()));
                }
            }
        }
        out
    } else {
        Vec::new()
    };
    #[cfg(not(feature = "enterprise"))]
    let overrides: Vec<(String, String)> = Vec::new();

    let reset = sqlx::query(
        "UPDATE task_runs
         SET status='pending', attempt=0, claimed_by=NULL, lease_expires_at=NULL,
             output=NULL, finished_at=NULL, scheduled_at=$1, version=version+1,
             remaining_deps=(
                 SELECT COUNT(*) FROM task_dependencies d
                 JOIN task_runs dep ON dep.id=d.dependency_id
                 WHERE d.dependent_id=task_runs.id AND dep.status NOT IN ('succeeded','skipped')
             )
         WHERE run_id=$2 AND status IN ('failed','cancelled')",
    )
    .bind(&now)
    .bind(&id)
    .execute(&mut *tx)
    .await
    .map_err(internal_msg)?
    .rows_affected();

    for (task_id, new_input) in &overrides {
        sqlx::query("UPDATE task_runs SET input = $1 WHERE id = $2")
            .bind(new_input)
            .bind(task_id)
            .execute(&mut *tx)
            .await
            .map_err(internal_msg)?;
    }

    sqlx::query(
        "UPDATE workflow_runs SET status='running', finished_at=NULL, output=NULL
         WHERE id=$1 AND status IN ('failed','cancelled')",
    )
    .bind(&id)
    .execute(&mut *tx)
    .await
    .map_err(internal_msg)?;

    notify(&mut tx, &id).await.map_err(|s| (s, "internal server error".to_string()))?;
    tx.commit().await.map_err(internal_msg)?;

    tracing::info!(run_id = %id, reset, overrides = overrides.len(), "run reran from failure");
    Ok(Json(RerunResponse { run_id: id, rerun: reset }))
}

/// Recursively merge `overlay` into `base`: matching object keys merge; every
/// other shape (scalars, arrays, type mismatches) is replaced by `overlay`.
#[cfg(feature = "enterprise")]
fn deep_merge(base: &mut serde_json::Value, overlay: serde_json::Value) {
    match (base, overlay) {
        (serde_json::Value::Object(b), serde_json::Value::Object(o)) => {
            for (k, v) in o {
                deep_merge(b.entry(k).or_insert(serde_json::Value::Null), v);
            }
        }
        (b, o) => *b = o,
    }
}

// ── Submit ────────────────────────────────────────────────────────────────────

/// Mirror of engine `dag::TaskSpec` — the submitted `input` JSON must match this
/// shape so the engine's worker can deserialize it.
///
/// Fields are `pub(crate)` so the `workflow_ref` expander (`crate::expand`) can
/// read a call task and synthesize the inlined leaf tasks.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct TaskSpecInput {
    pub(crate) name: String,
    // A leaf task runs a `command`; a call task sets `workflow_ref` instead. The
    // field defaults to empty so a `workflow_ref` task (no command) still parses,
    // and is skipped when empty so an expanded leaf serializes cleanly for the
    // engine. Expansion guarantees every surviving task carries a command.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) command: Vec<String>,
    #[serde(default)]
    pub(crate) depends_on: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) input: Option<serde_json::Value>,
    /// Trigger rule (engine `trigger_rule`): when this task runs relative to its
    /// deps' outcomes. Round-tripped into the stored task JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) trigger_rule: Option<String>,
    #[serde(default = "default_max_attempts")]
    pub(crate) max_attempts: u32,
    #[serde(default)]
    pub(crate) retry_delay_secs: u64,
    /// Optional clamp on the exponential retry backoff (engine
    /// `retry_max_delay_secs`); round-tripped into the stored task JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) retry_max_delay_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) docker_image: Option<String>,
    /// Runner-pool routing (engine `runner_class`): stamped on the task row so
    /// a segmented scheduler (`RUNNER_CLASSES`) claims it. Falls back to the
    /// spec-level class, then `"default"` — mirroring engine `create_run`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) runner_class: Option<String>,
    /// Chain another **saved** workflow as this step. At run-creation dagron-api
    /// loads that workflow's spec and inlines its tasks in place of this one
    /// (see [`crate::expand`]). A task is either a leaf (`command`) or a call
    /// (`workflow_ref`), never both. Resolved away before the run is created, so
    /// it never reaches the engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) workflow_ref: Option<String>,
    /// Task kind (engine `type`). `type: approval` makes this a human approval
    /// gate (#19) — a command-less leaf that parks in `awaiting_approval`.
    /// Round-tripped into the stored task JSON so the engine sees it.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) task_type: Option<String>,
    /// Approval timeout knobs (engine `approval_timeout_secs`/`approval_on_timeout`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) approval_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) approval_on_timeout: Option<String>,
    /// Environment variables (engine `env`): literal `value` or
    /// `value_from: {secret: NAME}` resolved at dispatch (environment secret
    /// store first, then process env / secrets dir).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) env: Vec<EnvVarInput>,
    /// Conditional gate (engine `when`). On this path two forms are accepted:
    /// a parameter condition (evaluated at submit — false drops the task, its
    /// dependents proceed) or a runtime condition referencing
    /// `{{ tasks.<dep>.output }}` (persisted; the engine evaluates it at
    /// readiness so an upstream's result can branch the DAG).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) when: Option<String>,
    /// Loop operator (engine `repeat`): re-run until `until` holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) repeat: Option<RepeatInput>,
}

/// Mirror of engine `dag::EnvVar` (same field names → same stored JSON).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct EnvVarInput {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) value_from: Option<SecretRefInput>,
}

/// Mirror of engine `dag::SecretRef`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct SecretRefInput {
    pub(crate) secret: String,
}

/// Mirror of engine `dag::RepeatSpec`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct RepeatInput {
    pub(crate) until: String,
    pub(crate) max_iterations: u32,
    #[serde(default)]
    pub(crate) delay_secs: u64,
}

/// Mirror of engine `dag::TaskDefaults` (the DRY block): fields set here fill
/// every task that doesn't override them, exactly like the engine-side merge.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct TaskDefaultsInput {
    #[serde(default)]
    pub(crate) max_attempts: Option<u32>,
    #[serde(default)]
    pub(crate) retry_delay_secs: Option<u64>,
    #[serde(default)]
    pub(crate) retry_max_delay_secs: Option<u64>,
    #[serde(default)]
    pub(crate) timeout_secs: Option<u64>,
    #[serde(default)]
    pub(crate) docker_image: Option<String>,
    #[serde(default)]
    pub(crate) runner_class: Option<String>,
    #[serde(default)]
    pub(crate) env: Vec<EnvVarInput>,
}

impl TaskSpecInput {
    /// Whether this task is a `type: approval` human gate (#19).
    pub(crate) fn is_approval(&self) -> bool {
        self.task_type.as_deref() == Some("approval")
    }
}

fn default_max_attempts() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct DagSpecInput {
    pub(crate) name: String,
    /// Run-level wall-clock budget in seconds (engine `run_timeout_secs`);
    /// stamped as `workflow_runs.deadline_at` at run creation so the engine's
    /// deadline sweep enforces it.
    #[serde(default)]
    pub(crate) run_timeout_secs: Option<u64>,
    /// Name of the task whose output becomes the run's result (engine
    /// `result_from`, fast-win #15); stamped as `workflow_runs.result_from` so a
    /// waiter (`GET /api/runs/:id/wait`) gets a single return value. Must name a
    /// real task.
    #[serde(default)]
    pub(crate) result_from: Option<String>,
    /// Template parameters: `{{ name }}` references in task commands /
    /// docker_image / env values / when conditions are substituted at submit.
    #[serde(default)]
    pub(crate) parameters: std::collections::BTreeMap<String, String>,
    /// Named environment (variable set + secrets): its variables join the
    /// substitution scope as `{{ env.NAME }}`, its name is stamped on the run
    /// so the engine resolves its secrets at dispatch. Unknown = 400.
    #[serde(default)]
    pub(crate) environment: Option<String>,
    /// Workflow-wide task defaults (the DRY block), merged into every task at
    /// submit — a task wins by setting its own value.
    #[serde(default)]
    pub(crate) task_defaults: Option<TaskDefaultsInput>,
    /// Spec-level runner class (engine `DagSpec::runner_class`): the fallback
    /// for tasks that don't set their own, applied at run creation.
    #[serde(default)]
    pub(crate) runner_class: Option<String>,
    pub(crate) tasks: Vec<TaskSpecInput>,
}

/// Parse + validate a DAG YAML (duplicate-name, unknown-dep, cycle). Shared by
/// submit and dead-letter redrive. Returns the spec or a (status, message) error.
/// This validates the *authored* spec; `workflow_ref` call tasks are checked for
/// structure here and resolved later by [`crate::expand`].
pub(crate) fn parse_and_validate(yaml: &str) -> Result<DagSpecInput, (StatusCode, String)> {
    let spec: DagSpecInput = serde_yaml::from_str(yaml)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid YAML: {e}")))?;
    if spec.run_timeout_secs == Some(0) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid run_timeout_secs=0 in DAG '{}'; expected >= 1 (or omit)", spec.name),
        ));
    }
    if let Some(class) = &spec.runner_class {
        validate_runner_class(class).map_err(|e| {
            (StatusCode::BAD_REQUEST, format!("invalid runner_class in DAG '{}': {e}", spec.name))
        })?;
    }
    if let Some(rf) = &spec.result_from {
        if !spec.tasks.iter().any(|t| &t.name == rf) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("result_from names unknown task '{rf}' in DAG '{}'", spec.name),
            ));
        }
    }
    validate_graph(&spec.name, &spec.tasks)?;
    Ok(spec)
}

/// Runner-class validation, mirroring core `dag::validate_runner_class`
/// (dagron-api cannot depend on dagron-core — see `src/tmpl.rs` for why):
/// lowercase `[a-z0-9_-]`, 1–64 chars, and not the reserved `"other"` (the
/// metrics tail bucket).
fn validate_runner_class(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err(format!("runner_class must be 1-64 characters, got {} ('{name}')", name.len()));
    }
    if !name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '-'))
    {
        return Err(format!("runner_class '{name}' may only contain [a-z0-9_-]"));
    }
    if name == "other" {
        return Err("runner_class 'other' is reserved (it is the metrics tail bucket)".to_string());
    }
    Ok(())
}

/// Structural DAG validation: every task is a leaf or a chain (not both/neither),
/// task names are unique, every `depends_on` resolves, and the graph is acyclic.
/// Reused on both the authored spec and the flattened spec the `workflow_ref`
/// expander produces.
pub(crate) fn validate_graph(
    name: &str,
    tasks: &[TaskSpecInput],
) -> Result<(), (StatusCode, String)> {
    let mut graph = DiGraph::<(), ()>::new();
    let mut idx = HashMap::new();
    let mut names = HashSet::new();
    const TRIGGER_RULES: &[&str] =
        &["all_success", "all_done", "one_failed", "all_failed", "none_failed"];
    for t in tasks {
        if !names.insert(t.name.clone()) {
            return Err((StatusCode::BAD_REQUEST, format!("duplicate task name '{}'", t.name)));
        }
        if let Some(rule) = &t.trigger_rule {
            if !TRIGGER_RULES.contains(&rule.as_str()) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("task '{}' has invalid trigger_rule '{rule}' (expected one of {TRIGGER_RULES:?})", t.name),
                ));
            }
        }
        // `repeat:` loop-operator validation (mirrors core from_spec).
        if let Some(rep) = &t.repeat {
            if rep.until.trim().is_empty() {
                return Err((StatusCode::BAD_REQUEST, format!("task '{}' repeat.until is empty", t.name)));
            }
            if rep.max_iterations == 0 {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("task '{}' repeat.max_iterations must be >= 1", t.name),
                ));
            }
            if t.is_approval() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("task '{}' cannot combine `repeat` with an approval gate", t.name),
                ));
            }
        }
        if let Some(class) = &t.runner_class {
            validate_runner_class(class).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("invalid runner_class for task '{}': {e}", t.name),
                )
            })?;
        }
        // A runtime `when` may only reference tasks it depends on — an output
        // the gate is guaranteed to have when the engine evaluates readiness.
        if let Some(cond) = &t.when {
            for referenced in crate::tmpl::when_output_refs(cond) {
                if !t.depends_on.contains(&referenced) {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!(
                            "task '{}' when references '{{{{ tasks.{referenced}.output }}}}' but does \
                             not depend on '{referenced}' — add it to depends_on",
                            t.name
                        ),
                    ));
                }
            }
        }
        // An approval gate (#19) is a command-less leaf that waits for a human, so
        // it is exempt from the leaf/chain rule (but must not carry either).
        if t.is_approval() {
            if !t.command.is_empty() || t.workflow_ref.is_some() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("approval task '{}' cannot set `command` or `workflow_ref`", t.name),
                ));
            }
        } else {
            // A task is exactly one of: a leaf (`command`) or a chain (`workflow_ref`).
            // Catches a command-less task on the no-reference path too (where the
            // expander, which also checks this, is skipped).
            match (!t.command.is_empty(), t.workflow_ref.is_some()) {
                (true, true) => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!("task '{}' sets both `command` and `workflow_ref` — use exactly one", t.name),
                    ));
                }
                (false, false) => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!("task '{}' needs a `command` (leaf) or a `workflow_ref` (chain)", t.name),
                    ));
                }
                _ => {}
            }
        }
        idx.insert(t.name.clone(), graph.add_node(()));
    }
    for t in tasks {
        let to = idx[&t.name];
        for dep in &t.depends_on {
            let Some(&from) = idx.get(dep) else {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("task '{}' depends on unknown task '{}'", t.name, dep),
                ));
            };
            graph.add_edge(from, to, ());
        }
    }
    if is_cyclic_directed(&graph) {
        return Err((StatusCode::BAD_REQUEST, format!("DAG '{}' contains a cycle", name)));
    }
    Ok(())
}

#[derive(Deserialize)]
pub struct SubmitBody {
    yaml: String,
}

#[derive(Serialize)]
pub struct SubmitResponse {
    pub run_id: String,
}

/// `POST /api/runs` — submit a DAG (YAML in `{ "yaml": "..." }`). Parses and
/// cycle-checks server-side (the authoritative validation), resolves any
/// `workflow_ref` chains into a flat DAG, then creates the run. The original
/// (un-expanded) YAML is stored as the run's definition.
pub async fn submit_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Json(body): Json<SubmitBody>,
) -> Result<(StatusCode, Json<SubmitResponse>), (StatusCode, String)> {
    let spec = parse_and_validate(&body.yaml)?;
    let expanded = crate::expand::expand_workflow_refs(&state, spec).await?;
    let prepared = prepare_spec(&state, expanded).await?;
    let run_id = create_run(&state, &prepared, &body.yaml).await.map_err(|e| {
        tracing::error!(error = ?e, "create_run failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal server error".to_string(),
        )
    })?;

    Ok((StatusCode::CREATED, Json(SubmitResponse { run_id })))
}

/// `POST /api/runs/:id/resubmit` — start a **fresh** run from this run's stored
/// definition (re-trigger). Unlike `rerun` (which resumes a failed/cancelled run
/// in place), this works for any run — including a succeeded one — and produces a
/// brand-new run_id.
pub async fn resubmit_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<SubmitResponse>), (StatusCode, String)> {
    let yaml: Option<String> = sqlx::query_scalar(
        "SELECT d.spec FROM workflow_runs r
         JOIN workflow_definitions d ON d.id = r.definition_id
         WHERE r.id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal_msg)?;
    let yaml = yaml.ok_or((StatusCode::NOT_FOUND, format!("run '{id}' not found")))?;

    let spec = parse_and_validate(&yaml)?;
    let expanded = crate::expand::expand_workflow_refs(&state, spec).await?;
    let prepared = prepare_spec(&state, expanded).await?;
    let run_id = create_run(&state, &prepared, &yaml).await.map_err(|e| {
        tracing::error!(error = ?e, "resubmit create_run failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
    })?;
    Ok((StatusCode::CREATED, Json(SubmitResponse { run_id })))
}

/// Submit-time preparation, run after `workflow_ref` expansion and before
/// [`create_run`]: merge `task_defaults` into every task, build the `{{ }}`
/// substitution scope (spec `parameters` + the declared environment's
/// variables as `env.*` keys), evaluate parameter-time `when:` conditions
/// (false ⇒ the task is dropped and scrubbed from dependents' `depends_on`,
/// so they proceed on their remaining deps — skip semantics), and substitute
/// the scope into task fields. Runtime `when:` conditions (referencing
/// `tasks.*.output`) survive verbatim for the engine.
pub(crate) async fn prepare_spec(
    state: &AppState,
    mut spec: DagSpecInput,
) -> Result<DagSpecInput, (StatusCode, String)> {
    // task_defaults merge (same rules as engine expand::apply_task_defaults).
    if let Some(d) = spec.task_defaults.take() {
        for t in spec.tasks.iter_mut() {
            if t.max_attempts == 1 {
                if let Some(v) = d.max_attempts {
                    t.max_attempts = v;
                }
            }
            if t.retry_delay_secs == 0 {
                if let Some(v) = d.retry_delay_secs {
                    t.retry_delay_secs = v;
                }
            }
            if t.retry_max_delay_secs.is_none() {
                t.retry_max_delay_secs = d.retry_max_delay_secs;
            }
            if t.timeout_secs.is_none() {
                t.timeout_secs = d.timeout_secs;
            }
            if t.docker_image.is_none() {
                t.docker_image = d.docker_image.clone();
            }
            if t.runner_class.is_none() {
                t.runner_class = d.runner_class.clone();
            }
            if !d.env.is_empty() {
                let mut env = d.env.clone();
                env.append(&mut t.env);
                t.env = env;
            }
        }
    }

    // Substitution scope: parameters + `env.NAME` variables. A declared but
    // unknown environment is a 400 — a spec pinned to prod must not silently
    // run without prod's variables.
    let mut ctx = spec.parameters.clone();
    if let Some(env_name) = &spec.environment {
        let vars: Option<String> =
            sqlx::query_scalar("SELECT variables FROM environments WHERE name = $1")
                .bind(env_name)
                .fetch_optional(&state.read_pool)
                .await
                .map_err(internal_msg)?;
        let Some(vars) = vars else {
            return Err((StatusCode::BAD_REQUEST, format!("environment '{env_name}' not found")));
        };
        // Malformed variables fail as loudly as an unknown environment — a spec
        // pinned to prod must not silently run without prod's variables.
        let vars: std::collections::BTreeMap<String, String> =
            serde_json::from_str(&vars).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("environment '{env_name}' has malformed variables: {e}"),
                )
            })?;
        for (k, v) in vars {
            ctx.insert(format!("env.{k}"), v);
        }
    }

    // Parameter-time `when:`: decide task fate now; keep runtime gates.
    let mut dropped: HashSet<String> = HashSet::new();
    for t in spec.tasks.iter_mut() {
        let Some(cond) = &t.when else { continue };
        let resolved = crate::tmpl::substitute(cond, &ctx);
        if crate::tmpl::when_output_refs(&resolved).is_empty() {
            let keep = crate::tmpl::eval_when(&resolved).map_err(|e| {
                (StatusCode::BAD_REQUEST, format!("task '{}' when: {e}", t.name))
            })?;
            if !keep {
                dropped.insert(t.name.clone());
            }
            t.when = None; // consumed at submit
        } else {
            t.when = Some(resolved); // runtime gate, engine evaluates
        }
    }
    if !dropped.is_empty() {
        spec.tasks.retain(|t| !dropped.contains(&t.name));
        for t in spec.tasks.iter_mut() {
            t.depends_on.retain(|d| !dropped.contains(d));
        }
        // Re-validate: a surviving runtime `when` may reference a task that was
        // just dropped (its dep got scrubbed above) — that gate could never be
        // evaluated, so reject it at submit rather than strand the run.
        validate_graph(&spec.name, &spec.tasks)?;
    }

    // Substitute the scope into task string fields (mirror of core build_leaf
    // coverage for the fields this path models).
    if !ctx.is_empty() {
        for t in spec.tasks.iter_mut() {
            t.command = t.command.iter().map(|a| crate::tmpl::substitute(a, &ctx)).collect();
            t.docker_image = t.docker_image.as_ref().map(|s| crate::tmpl::substitute(s, &ctx));
            t.runner_class = t.runner_class.as_ref().map(|s| crate::tmpl::substitute(s, &ctx));
            for e in t.env.iter_mut() {
                e.value = crate::tmpl::substitute(&e.value, &ctx);
            }
            if let Some(rep) = t.repeat.as_mut() {
                rep.until = crate::tmpl::substitute(&rep.until, &ctx);
            }
        }
    }

    Ok(spec)
}

/// Insert definition + run + tasks + edges, mirroring engine `db::postgres::create_run`.
pub(crate) async fn create_run(
    state: &AppState,
    spec: &DagSpecInput,
    yaml: &str,
) -> anyhow::Result<String> {
    let def_id = Uuid::new_v4().to_string();
    let run_id = Uuid::new_v4().to_string();
    let created = chrono::Utc::now();
    let now = created.to_rfc3339();
    // Mirrors engine create_run: persist the absolute run deadline so the
    // engine's sweep enforces `run_timeout_secs` on UI-submitted runs too.
    let deadline_at = spec
        .run_timeout_secs
        .map(|secs| (created + chrono::TimeDelta::seconds(secs.min(i64::MAX as u64) as i64)).to_rfc3339());

    // in-degree per task = remaining_deps seed
    let mut indeg: HashMap<&str, i64> = spec.tasks.iter().map(|t| (t.name.as_str(), 0)).collect();
    for t in &spec.tasks {
        for _dep in &t.depends_on {
            *indeg.get_mut(t.name.as_str()).unwrap() += 1;
        }
    }

    let mut tx = state.write_pool.begin().await?;
    sqlx::query("INSERT INTO workflow_definitions (id, name, spec, created_at) VALUES ($1,$2,$3,$4)")
        .bind(&def_id).bind(&spec.name).bind(yaml).bind(&now)
        .execute(&mut *tx).await?;
    sqlx::query("INSERT INTO workflow_runs (id, definition_id, status, created_at, deadline_at, result_from, environment) VALUES ($1,$2,'running',$3,$4,$5,$6)")
        .bind(&run_id).bind(&def_id).bind(&now).bind(&deadline_at).bind(&spec.result_from).bind(&spec.environment)
        .execute(&mut *tx).await?;

    let mut ids: HashMap<String, String> = HashMap::new();
    for t in &spec.tasks {
        let task_id = Uuid::new_v4().to_string();
        let input_json = serde_json::to_string(t)?; // matches dag::TaskSpec shape
        let trigger_rule = t.trigger_rule.as_deref().unwrap_or("all_success");
        // Approval gate (#19): the engine's advance_ready_tasks keys off the
        // `is_approval` COLUMN (not the input JSON) to park a ready task in
        // `awaiting_approval` instead of dispatching it. Engine create_run sets
        // it; this API mirror must too, or a `type: approval` task submitted via
        // POST /api/runs lands with is_approval=0 and the executor runs it as a
        // command-less task ("empty command" failure). Timeout knobs pair with it.
        let is_approval = i64::from(t.is_approval());
        let approval_timeout = t.approval_timeout_secs.map(|s| s as i64);
        let approval_on_timeout = t.approval_on_timeout.as_deref();
        // Task → spec-level → 'default', mirroring engine create_run: the
        // segmented claim path filters on this COLUMN, not the input JSON.
        let runner_class = t
            .runner_class
            .as_deref()
            .or(spec.runner_class.as_deref())
            .unwrap_or("default");
        sqlx::query(
            "INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input, scheduled_at, trigger_rule,
                                    is_approval, approval_timeout_secs, approval_on_timeout, runner_class)
             VALUES ($1,$2,$3,'pending',$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(&task_id).bind(&run_id).bind(&t.name)
        .bind(indeg[t.name.as_str()]).bind(&input_json).bind(&now).bind(trigger_rule)
        .bind(is_approval).bind(approval_timeout).bind(approval_on_timeout).bind(runner_class)
        .execute(&mut *tx).await?;
        ids.insert(t.name.clone(), task_id);
    }
    for t in &spec.tasks {
        let dependent = &ids[&t.name];
        for dep in &t.depends_on {
            sqlx::query("INSERT INTO task_dependencies (dependent_id, dependency_id) VALUES ($1,$2)")
                .bind(dependent).bind(&ids[dep])
                .execute(&mut *tx).await?;
        }
    }

    notify_tx(&mut tx, &run_id).await?;
    tx.commit().await?;
    Ok(run_id)
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// NOTIFY inside a control handler's transaction; errors map to 500.
async fn notify(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
) -> Result<(), StatusCode> {
    notify_tx(tx, run_id).await.map_err(|e| {
        tracing::error!(error = ?e, "pg_notify failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

async fn notify_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
) -> anyhow::Result<()> {
    sqlx::query("SELECT pg_notify('task_events', $1)")
        .bind(run_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

fn internal_msg(err: sqlx::Error) -> (StatusCode, String) {
    tracing::error!(error = ?err, "db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
}
