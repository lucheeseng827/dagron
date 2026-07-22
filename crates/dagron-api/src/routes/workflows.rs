//! First-class workflows: named, reusable DAG definitions managed via the UI.
//!
//! Distinct from the engine's per-run `workflow_definitions`. A `workflow` row is
//! authored/edited here; "running" it submits its `spec` through the same
//! create_run path the submit endpoint uses, producing an ordinary run. The
//! engine never reads this table.

use std::collections::HashMap;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::routes::control;
use crate::state::AppState;

#[derive(Serialize, sqlx::FromRow)]
pub struct Workflow {
    pub id: String,
    pub name: String,
    pub spec: String,
    pub description: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Deserialize)]
pub struct UpsertBody {
    pub name: Option<String>,
    pub spec: String,
    #[serde(default)]
    pub description: Option<String>,
}

/// Enriched list row for the Workflows table/board view: definition + its
/// schedule + a digest of its recent runs (last/history/success), all derived
/// from real data. Runs are matched to a workflow by definition name.
#[derive(Serialize)]
pub struct WorkflowRow {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    /// "git" (namespaced name from the operator/GitOps) or "manual".
    pub source: String,
    pub created_at: String,
    pub updated_at: String,
    pub schedule_id: Option<String>,
    pub cron_expr: Option<String>,
    pub next_fire_at: Option<String>,
    pub paused: bool,
    pub has_schedule: bool,
    pub last_status: Option<String>,
    pub last_at: Option<String>,
    /// Up to 14 recent run statuses, oldest → newest (for the sparkline).
    pub history: Vec<String>,
    pub success_rate: Option<i64>,
    pub run_count: i64,
}

#[derive(sqlx::FromRow)]
struct WfBase {
    id: String,
    name: String,
    description: Option<String>,
    created_at: String,
    updated_at: String,
}
#[derive(sqlx::FromRow)]
struct SchedRow {
    workflow_id: String,
    schedule_id: String,
    cron_expr: String,
    enabled: i64,
    next_fire_at: Option<String>,
}
#[derive(sqlx::FromRow)]
struct RunStat {
    name: String,
    status: String,
    created_at: String,
}

/// `GET /api/workflows` — enriched rows (definition + schedule + run digest).
pub async fn list_workflows(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<WorkflowRow>>, StatusCode> {
    let wfs = sqlx::query_as::<_, WfBase>(
        "SELECT id, name, description, created_at, updated_at FROM workflows ORDER BY name",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    // Ordered by created_at so that, when a workflow has more than one schedule,
    // selection below is deterministic (the oldest wins) rather than relying on
    // arbitrary row order.
    let scheds = sqlx::query_as::<_, SchedRow>(
        "SELECT workflow_id, id AS schedule_id, cron_expr, enabled, next_fire_at
         FROM schedules ORDER BY created_at",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    // Last 14 runs per workflow name (window function), newest first.
    let runs = sqlx::query_as::<_, RunStat>(
        "SELECT name, status, created_at FROM (
            SELECT d.name AS name, r.status AS status, r.created_at AS created_at,
                   row_number() OVER (PARTITION BY d.name ORDER BY r.created_at DESC) AS rn
            FROM workflow_runs r JOIN workflow_definitions d ON d.id = r.definition_id
         ) t WHERE rn <= 14
         ORDER BY name, created_at DESC",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    // Pre-index so the per-workflow loop is O(1) lookups instead of rescanning
    // the full vectors. `or_insert` keeps the first (oldest) schedule per
    // workflow, matching the ORDER BY above.
    let mut sched_by_workflow: HashMap<&str, &SchedRow> = HashMap::new();
    for s in &scheds {
        sched_by_workflow.entry(s.workflow_id.as_str()).or_insert(s);
    }
    let mut runs_by_name: HashMap<&str, Vec<&RunStat>> = HashMap::new();
    for r in &runs {
        runs_by_name.entry(r.name.as_str()).or_default().push(r);
    }

    let mut rows = Vec::with_capacity(wfs.len());
    for w in wfs {
        let sched = sched_by_workflow.get(w.id.as_str()).copied();
        // runs for this workflow name, newest first
        let mine: Vec<&RunStat> = runs_by_name.get(w.name.as_str()).cloned().unwrap_or_default();
        let total = mine.len() as i64;
        let succeeded = mine.iter().filter(|r| r.status == "succeeded").count() as i64;
        let success_rate = if total > 0 { Some((succeeded * 100) / total) } else { None };
        // oldest → newest for the left-to-right sparkline
        let history: Vec<String> = mine.iter().rev().map(|r| r.status.clone()).collect();
        let last = mine.first();

        rows.push(WorkflowRow {
            source: if w.name.contains('/') { "git".into() } else { "manual".into() },
            schedule_id: sched.map(|s| s.schedule_id.clone()),
            cron_expr: sched.map(|s| s.cron_expr.clone()),
            next_fire_at: sched.and_then(|s| s.next_fire_at.clone()),
            paused: sched.map(|s| s.enabled == 0).unwrap_or(false),
            has_schedule: sched.is_some(),
            last_status: last.map(|r| r.status.clone()),
            last_at: last.map(|r| r.created_at.clone()),
            history,
            success_rate,
            run_count: total,
            id: w.id,
            name: w.name,
            description: w.description,
            created_at: w.created_at,
            updated_at: w.updated_at,
        });
    }
    Ok(Json(rows))
}

/// `GET /api/workflows/:id` — full workflow incl. spec.
pub async fn get_workflow(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Workflow>, StatusCode> {
    let wf = sqlx::query_as::<_, Workflow>(
        "SELECT id, name, spec, description, created_at, updated_at FROM workflows WHERE id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?
    .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(wf))
}

/// `POST /api/workflows` — create. Validates the DAG (cycle/dup/unknown-dep) and
/// derives the name from the spec unless one is given. 409 on duplicate name.
pub async fn create_workflow(
    _auth: AuthUser,
    State(state): State<AppState>,
    Json(body): Json<UpsertBody>,
) -> Result<(StatusCode, Json<Workflow>), (StatusCode, String)> {
    let spec = control::parse_and_validate(&body.spec)?;
    let name = body.name.unwrap_or(spec.name);
    let description = body.description.filter(|d| !d.trim().is_empty());
    let now = chrono::Utc::now().to_rfc3339();
    let id = Uuid::new_v4().to_string();

    sqlx::query(
        "INSERT INTO workflows (id, name, spec, description, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$5)",
    )
    .bind(&id)
    .bind(&name)
    .bind(&body.spec)
    .bind(&description)
    .bind(&now)
    .execute(&state.write_pool)
    .await
    .map_err(|e| dup_or_internal(e, &name))?;

    Ok((
        StatusCode::CREATED,
        Json(Workflow { id, name, spec: body.spec, description, created_at: now.clone(), updated_at: now }),
    ))
}

/// `PUT /api/workflows/:id` — update spec (+ optional rename). Re-validates.
pub async fn update_workflow(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<UpsertBody>,
) -> Result<Json<Workflow>, (StatusCode, String)> {
    let spec = control::parse_and_validate(&body.spec)?;
    let name = body.name.unwrap_or(spec.name);
    let description = body.description.filter(|d| !d.trim().is_empty());
    let now = chrono::Utc::now().to_rfc3339();

    let res = sqlx::query(
        "UPDATE workflows SET name = $1, spec = $2, description = $3, updated_at = $4 WHERE id = $5",
    )
    .bind(&name)
    .bind(&body.spec)
    .bind(&description)
    .bind(&now)
    .bind(&id)
    .execute(&state.write_pool)
    .await
    .map_err(|e| dup_or_internal(e, &name))?;

    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, format!("workflow '{id}' not found")));
    }

    let created_at: String = sqlx::query_scalar("SELECT created_at FROM workflows WHERE id = $1")
        .bind(&id)
        .fetch_one(&state.read_pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    Ok(Json(Workflow { id, name, spec: body.spec, description, created_at, updated_at: now }))
}

/// `DELETE /api/workflows/:id`. 404 if absent.
pub async fn delete_workflow(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let n = sqlx::query("DELETE FROM workflows WHERE id = $1")
        .bind(&id)
        .execute(&state.write_pool)
        .await
        .map_err(internal)?
        .rows_affected();
    if n == 0 {
        Err(StatusCode::NOT_FOUND)
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

/// `POST /api/workflows/:id/run` — submit the stored spec as a new run.
pub async fn run_workflow(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let spec_yaml: Option<String> = sqlx::query_scalar("SELECT spec FROM workflows WHERE id = $1")
        .bind(&id)
        .fetch_optional(&state.read_pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    let spec_yaml = spec_yaml.ok_or((StatusCode::NOT_FOUND, format!("workflow '{id}' not found")))?;

    let spec = control::parse_and_validate(&spec_yaml)?;
    // Resolve any `workflow_ref` chains (this workflow calling other saved
    // workflows) into one flat DAG before the run is created.
    let expanded = crate::expand::expand_workflow_refs(&state, spec).await?;
    let prepared = control::prepare_spec(&state, expanded).await?;
    let run_id = control::create_run(&state, &prepared, &spec_yaml)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    Ok(Json(serde_json::json!({ "run_id": run_id, "workflow_id": id })))
}

#[derive(Deserialize)]
pub struct WorkflowRunsParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// `GET /api/workflows/:id/runs?limit=&offset=` — this workflow's run history,
/// newest first. Runs are matched by definition **name** — the only linkage
/// that exists: each run snapshots its own `workflow_definitions` row (a fresh
/// id per run), so there is no FK from runs to the `workflows` table, and the
/// list digest in `list_workflows` uses the same name rule. Consequence: a
/// renamed workflow starts a fresh history (documented in API.md).
/// Backs the read-oriented workflow detail page.
pub async fn workflow_runs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<WorkflowRunsParams>,
) -> Result<Json<Vec<crate::routes::runs::RunSummary>>, (StatusCode, String)> {
    let name: Option<String> = sqlx::query_scalar("SELECT name FROM workflows WHERE id = $1")
        .bind(&id)
        .fetch_optional(&state.read_pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    let name = name.ok_or((StatusCode::NOT_FOUND, format!("workflow '{id}' not found")))?;

    let limit = params.limit.unwrap_or(50).clamp(1, 500);
    let offset = params.offset.unwrap_or(0).max(0);
    let rows = sqlx::query_as::<_, crate::routes::runs::RunSummary>(&format!(
        "{}
         WHERE d.name = $1
         ORDER BY wr.created_at DESC
         LIMIT $2 OFFSET $3",
        crate::routes::runs::SUMMARY_SELECT
    ))
    .bind(&name)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.read_pool)
    .await
    .map_err(|e| {
        tracing::error!(error = ?e, "workflow runs query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
    })?;
    Ok(Json(rows))
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

/// Map a UNIQUE-violation to 409, anything else to 500.
fn dup_or_internal(err: sqlx::Error, name: &str) -> (StatusCode, String) {
    if let sqlx::Error::Database(db) = &err {
        if db.is_unique_violation() {
            return (StatusCode::CONFLICT, format!("workflow name '{name}' already exists"));
        }
    }
    tracing::error!(error = ?err, "db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
}
