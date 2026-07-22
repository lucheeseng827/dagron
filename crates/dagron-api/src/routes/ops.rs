//! Observability + dead-letter endpoints for the authenticated UI edge.
//!
//! These surface engine-side ops capabilities (which `src/api.rs` also exposes
//! unauthenticated, in-process) through the JWT-gated public gateway, so the UI
//! has ONE coherent authed backend. Like control.rs the SQL is inlined and
//! mirrors the engine's `db` functions (dead_letters table, metrics gauges).
//! Redrive reuses control.rs's create_run.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::routes::control;
use crate::state::AppState;

// ── Metrics (JSON gauges from the datastore) ────────────────────────────────

#[derive(Serialize, sqlx::FromRow)]
pub struct StatusCount {
    pub status: String,
    pub count: i64,
}

#[derive(Serialize)]
pub struct MetricsResponse {
    pub runs_by_status: Vec<StatusCount>,
    pub tasks_by_status: Vec<StatusCount>,
    pub dead_letters: i64,
}

/// `GET /api/metrics` — live run/task counts by status + dead-letter total.
/// JSON (UI-friendly) rather than the engine's Prometheus text at `/metrics`.
pub async fn metrics(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<MetricsResponse>, StatusCode> {
    let runs = sqlx::query_as::<_, StatusCount>(
        "SELECT status, COUNT(*) AS count FROM workflow_runs GROUP BY status",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    let tasks = sqlx::query_as::<_, StatusCount>(
        "SELECT status, COUNT(*) AS count FROM task_runs GROUP BY status",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    let dead_letters: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dead_letters")
        .fetch_one(&state.read_pool)
        .await
        .map_err(internal)?;

    Ok(Json(MetricsResponse { runs_by_status: runs, tasks_by_status: tasks, dead_letters }))
}

// ── Metrics time-series ─────────────────────────────────────────────────────

#[derive(Serialize, sqlx::FromRow)]
pub struct DayBucket {
    /// Bucket day, `YYYY-MM-DD` (UTC).
    pub day: String,
    pub succeeded: i64,
    pub failed: i64,
    pub cancelled: i64,
    /// Still pending/running when queried.
    pub active: i64,
    /// Mean wall-clock of finished runs that day, seconds. Null = none finished.
    pub avg_duration_secs: Option<f64>,
    pub max_duration_secs: Option<f64>,
}

#[derive(Deserialize)]
pub struct TimeseriesParams {
    /// Look-back window in days (default 14, clamped to [1, 90]).
    pub days: Option<i64>,
    /// Restrict to one workflow (definition name, exact match).
    pub name: Option<String>,
}

/// `GET /api/metrics/timeseries?days=&name=` — per-day run counts by outcome and
/// duration stats, for the Metrics charts and the workflow detail trend.
/// Timestamps are stored as RFC-3339 TEXT, so buckets cast through timestamptz.
pub async fn metrics_timeseries(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<TimeseriesParams>,
) -> Result<Json<Vec<DayBucket>>, StatusCode> {
    let days = params.days.unwrap_or(14).clamp(1, 90);
    let rows = sqlx::query_as::<_, DayBucket>(
        "SELECT to_char(date_trunc('day', wr.created_at::timestamptz), 'YYYY-MM-DD') AS day,
                COUNT(*) FILTER (WHERE wr.status = 'succeeded') AS succeeded,
                COUNT(*) FILTER (WHERE wr.status = 'failed') AS failed,
                COUNT(*) FILTER (WHERE wr.status = 'cancelled') AS cancelled,
                COUNT(*) FILTER (WHERE wr.status IN ('pending','running')) AS active,
                AVG(EXTRACT(EPOCH FROM (wr.finished_at::timestamptz - wr.created_at::timestamptz))::float8)
                    FILTER (WHERE wr.finished_at IS NOT NULL) AS avg_duration_secs,
                MAX(EXTRACT(EPOCH FROM (wr.finished_at::timestamptz - wr.created_at::timestamptz))::float8)
                    FILTER (WHERE wr.finished_at IS NOT NULL) AS max_duration_secs
         FROM workflow_runs wr
         LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.created_at::timestamptz >= date_trunc('day', now()) - make_interval(days => $1::int)
           AND ($2::text IS NULL OR d.name = $2)
         GROUP BY 1 ORDER BY 1",
    )
    .bind(days)
    .bind(&params.name)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    Ok(Json(rows))
}

// ── Pending approval gates ──────────────────────────────────────────────────

#[derive(Serialize, sqlx::FromRow)]
pub struct PendingApproval {
    pub run_id: String,
    pub task_id: String,
    pub task_name: String,
    pub workflow_name: Option<String>,
    /// When the gate parked (task scheduled_at), oldest first.
    pub since: Option<String>,
}

/// `GET /api/approvals` — every task parked in `awaiting_approval`, oldest
/// first: the human-in-the-loop worklist behind the sidebar badge.
pub async fn list_approvals(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<PendingApproval>>, StatusCode> {
    let rows = sqlx::query_as::<_, PendingApproval>(
        "SELECT t.run_id, t.id AS task_id, t.name AS task_name,
                d.name AS workflow_name, t.scheduled_at AS since
         FROM task_runs t
         JOIN workflow_runs wr ON wr.id = t.run_id
         LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE t.status = 'awaiting_approval'
         ORDER BY t.scheduled_at ASC NULLS FIRST",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    Ok(Json(rows))
}

// ── Dead letters ────────────────────────────────────────────────────────────

#[derive(Serialize, sqlx::FromRow)]
pub struct DeadLetter {
    pub id: String,
    pub payload: String,
    pub error: String,
    pub source: String,
    pub failures: i64,
    pub first_seen_at: String,
    pub last_error_at: String,
}

#[derive(Deserialize)]
pub struct ListParams {
    pub limit: Option<i64>,
}

/// `GET /api/dead-letters?limit=` — parked poison submissions, newest first.
pub async fn list_dead_letters(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<DeadLetter>>, StatusCode> {
    let limit = params.limit.unwrap_or(100).clamp(1, 500);
    // Newest-first by most-recent failure (last_error_at), not first_seen_at
    // (which is when the poison was first parked).
    let rows = sqlx::query_as::<_, DeadLetter>(
        "SELECT id, payload, error, source, failures, first_seen_at, last_error_at
         FROM dead_letters ORDER BY last_error_at DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    Ok(Json(rows))
}

#[derive(Serialize)]
pub struct RedriveResponse {
    pub run_id: String,
    pub redriven_from: String,
}

/// `POST /api/dead-letters/:id/redrive` — re-submit a parked payload as a run.
/// Mirrors engine api.rs: delete-as-claim first (serializes concurrent redrives),
/// then create the run from the payload. 404 if already redriven/discarded,
/// 400 if the parked payload is itself an invalid DAG.
pub async fn redrive_dead_letter(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RedriveResponse>, (StatusCode, String)> {
    let dl = sqlx::query_as::<_, DeadLetter>(
        "SELECT id, payload, error, source, failures, first_seen_at, last_error_at
         FROM dead_letters WHERE id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.write_pool)
    .await
    .map_err(|e| internal_msg(e))?
    .ok_or((StatusCode::NOT_FOUND, format!("dead letter '{id}' not found")))?;

    // Validate the parked payload before claiming.
    let spec = control::parse_and_validate(&dl.payload)?;

    // Claim by delete: only the caller that removes the row proceeds to create.
    let claimed = sqlx::query("DELETE FROM dead_letters WHERE id = $1")
        .bind(&id)
        .execute(&state.write_pool)
        .await
        .map_err(internal_msg)?
        .rows_affected();
    if claimed == 0 {
        return Err((
            StatusCode::NOT_FOUND,
            format!("dead letter '{id}' was already redriven or discarded"),
        ));
    }

    // Resolve any `workflow_ref` chains so redrive behaves exactly like submit.
    let expanded = crate::expand::expand_workflow_refs(&state, spec).await?;
    let prepared = control::prepare_spec(&state, expanded).await?;
    let run_id = control::create_run(&state, &prepared, &dl.payload)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    Ok(Json(RedriveResponse { run_id, redriven_from: id }))
}

/// `DELETE /api/dead-letters/:id` — discard a parked payload. 404 if absent.
pub async fn delete_dead_letter(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let n = sqlx::query("DELETE FROM dead_letters WHERE id = $1")
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

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

fn internal_msg(err: sqlx::Error) -> (StatusCode, String) {
    tracing::error!(error = ?err, "db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
}
