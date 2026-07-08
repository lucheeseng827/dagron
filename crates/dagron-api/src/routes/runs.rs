//! Run list + detail read endpoints (read pool, all parameterized, auth-gated).

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::state::AppState;

/// Default page size for `GET /api/runs`, capped at MAX_LIMIT.
const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 500;

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct RunSummary {
    pub id: String,
    pub definition_id: String,
    pub status: String,
    pub created_at: String,
    pub finished_at: Option<String>,
    /// Workflow/DAG name from the run's definition (LEFT JOIN; None if the
    /// definition row is missing). Surfaced as the "Workflow" column in the UI.
    pub name: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct TaskRow {
    pub id: String,
    pub name: String,
    pub status: String,
    pub attempt: i64,
    pub output: Option<String>,
    pub scheduled_at: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RunDetail {
    pub id: String,
    pub definition_id: String,
    pub status: String,
    pub input: Option<String>,
    pub output: Option<String>,
    pub created_at: String,
    pub finished_at: Option<String>,
    pub tasks: Vec<TaskRow>,
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    pub status: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// `GET /api/runs?status=&limit=&offset=` — most-recent-first run list.
pub async fn list_runs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<RunSummary>>, StatusCode> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = params.offset.unwrap_or(0).max(0);

    // Bind status as an optional filter: when None, the `$1 IS NULL` branch keeps
    // every row, so one parameterized query serves both filtered and unfiltered.
    let rows = sqlx::query_as::<_, RunSummary>(
        "SELECT wr.id, wr.definition_id, wr.status, wr.created_at, wr.finished_at, d.name
         FROM workflow_runs wr
         LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE ($1::text IS NULL OR wr.status = $1)
         ORDER BY wr.created_at DESC
         LIMIT $2 OFFSET $3",
    )
    .bind(&params.status)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    Ok(Json(rows))
}

/// `GET /api/runs/:id` — one run plus its task rows. 404 if the run is unknown.
pub async fn get_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunDetail>, StatusCode> {
    let run = sqlx::query_as::<_, RunSummaryFull>(
        "SELECT id, definition_id, status, input, output, created_at, finished_at
         FROM workflow_runs WHERE id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?
    .ok_or(StatusCode::NOT_FOUND)?;

    let tasks = sqlx::query_as::<_, TaskRow>(
        "SELECT id, name, status, attempt, output, scheduled_at, finished_at
         FROM task_runs WHERE run_id = $1 ORDER BY name",
    )
    .bind(&id)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    Ok(Json(RunDetail {
        id: run.id,
        definition_id: run.definition_id,
        status: run.status,
        input: run.input,
        output: run.output,
        created_at: run.created_at,
        finished_at: run.finished_at,
        tasks,
    }))
}

#[derive(sqlx::FromRow)]
struct RunSummaryFull {
    id: String,
    definition_id: String,
    status: String,
    input: Option<String>,
    output: Option<String>,
    created_at: String,
    finished_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WaitParams {
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct RunResult {
    pub run_id: String,
    pub status: String,
    pub finished: bool,
    /// The `result_from` task's output on success, else null (fast-win #15).
    pub result: Option<String>,
}

/// `GET /api/runs/:id/wait?timeout_secs=` — synchronous invocation (fast-win
/// #15): long-poll a run until it reaches a terminal state (or the timeout
/// elapses) and return its status + result (the `result_from` task's output on
/// success). 404 if the run is unknown; a timed-out wait returns `finished:false`
/// with the live status so the caller re-polls. Backend-agnostic short poll — the
/// engine (which may be a separate process) drives the run to completion.
pub async fn wait_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<WaitParams>,
) -> Result<Json<RunResult>, StatusCode> {
    let timeout =
        std::time::Duration::from_secs(params.timeout_secs.unwrap_or(30).clamp(1, 600));
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let row: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT status, output FROM workflow_runs WHERE id = $1")
                .bind(&id)
                .fetch_optional(&state.read_pool)
                .await
                .map_err(internal)?;
        let Some((status, output)) = row else {
            return Err(StatusCode::NOT_FOUND);
        };
        let finished = matches!(status.as_str(), "succeeded" | "failed" | "cancelled");
        if finished || tokio::time::Instant::now() >= deadline {
            // Per the contract, `result` is the run output only on success; a
            // failed/cancelled run's output is an error message, not a result.
            let result = if status == "succeeded" { output } else { None };
            return Ok(Json(RunResult { run_id: id, status, finished, result }));
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Map any DB error to 500 without leaking internals to the client.
fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}
