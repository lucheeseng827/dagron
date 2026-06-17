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

// ── Rerun (cascade resume from failure) ────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
pub struct RerunBody {
    /// Rerun mode. Only `failed` (the default) is supported; `task:<id>` is reserved.
    #[serde(default)]
    from: Option<String>,
    /// QW4 — deep-merged into each reset task's stored TaskSpec `input`, so a
    /// fix-forward rerun can change task inputs without re-authoring the workflow.
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

    // QW4: capture the broken cone's specs before reset and merge params into each
    // task's `input`. The reset UPDATE below never touches `input` (it carries the
    // command), so these per-row overrides are applied alongside it.
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
    #[serde(default = "default_max_attempts")]
    pub(crate) max_attempts: u32,
    #[serde(default)]
    pub(crate) retry_delay_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) docker_image: Option<String>,
    /// Chain another **saved** workflow as this step. At run-creation dagron-api
    /// loads that workflow's spec and inlines its tasks in place of this one
    /// (see [`crate::expand`]). A task is either a leaf (`command`) or a call
    /// (`workflow_ref`), never both. Resolved away before the run is created, so
    /// it never reaches the engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) workflow_ref: Option<String>,
}

fn default_max_attempts() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct DagSpecInput {
    pub(crate) name: String,
    pub(crate) tasks: Vec<TaskSpecInput>,
}

/// Parse + validate a DAG YAML (duplicate-name, unknown-dep, cycle). Shared by
/// submit and dead-letter redrive. Returns the spec or a (status, message) error.
/// This validates the *authored* spec; `workflow_ref` call tasks are checked for
/// structure here and resolved later by [`crate::expand`].
pub(crate) fn parse_and_validate(yaml: &str) -> Result<DagSpecInput, (StatusCode, String)> {
    let spec: DagSpecInput = serde_yaml::from_str(yaml)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid YAML: {e}")))?;
    validate_graph(&spec.name, &spec.tasks)?;
    Ok(spec)
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
    for t in tasks {
        if !names.insert(t.name.clone()) {
            return Err((StatusCode::BAD_REQUEST, format!("duplicate task name '{}'", t.name)));
        }
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
    let run_id = create_run(&state, &expanded, &body.yaml).await.map_err(|e| {
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
    let run_id = create_run(&state, &expanded, &yaml).await.map_err(|e| {
        tracing::error!(error = ?e, "resubmit create_run failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
    })?;
    Ok((StatusCode::CREATED, Json(SubmitResponse { run_id })))
}

/// Insert definition + run + tasks + edges, mirroring engine `db::postgres::create_run`.
pub(crate) async fn create_run(
    state: &AppState,
    spec: &DagSpecInput,
    yaml: &str,
) -> anyhow::Result<String> {
    let def_id = Uuid::new_v4().to_string();
    let run_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

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
    sqlx::query("INSERT INTO workflow_runs (id, definition_id, status, created_at) VALUES ($1,$2,'running',$3)")
        .bind(&run_id).bind(&def_id).bind(&now)
        .execute(&mut *tx).await?;

    let mut ids: HashMap<String, String> = HashMap::new();
    for t in &spec.tasks {
        let task_id = Uuid::new_v4().to_string();
        let input_json = serde_json::to_string(t)?; // matches dag::TaskSpec shape
        sqlx::query(
            "INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input, scheduled_at)
             VALUES ($1,$2,$3,'pending',$4,$5,$6)",
        )
        .bind(&task_id).bind(&run_id).bind(&t.name)
        .bind(indeg[t.name.as_str()]).bind(&input_json).bind(&now)
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
