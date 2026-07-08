//! DAG graph + task-log read endpoints.
//!
//! `graph` returns nodes (task_runs) and edges (task_dependencies) shaped so the
//! React Flow client consumes edges as `source`/`target` directly.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::state::AppState;

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct GraphNode {
    pub id: String,
    pub name: String,
    pub status: String,
    pub attempt: i64,
    pub scheduled_at: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
}

#[derive(Debug, Serialize)]
pub struct GraphResponse {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Debug, sqlx::FromRow)]
struct TaskLogRow {
    task_id: String,
    name: String,
    status: String,
    attempt: i64,
    output: Option<String>,
}

/// Task log response with tail metadata (#17). `output` is the full output when
/// no `offset` is given (back-compat), or the slice from `offset` when tailing.
/// A client polls with `?offset=next_offset` until `eof` (the task is terminal).
/// Offsets are Unicode-scalar counts, so they never split a multibyte character.
#[derive(Debug, Serialize)]
pub struct TaskLogs {
    pub task_id: String,
    pub name: String,
    pub status: String,
    pub attempt: i64,
    pub output: Option<String>,
    /// Offset (char count) this response starts at.
    pub offset: usize,
    /// Resume point for the next poll — the char length of the full output.
    pub next_offset: usize,
    /// True once the task is terminal: no more output will arrive.
    pub eof: bool,
}

#[derive(Debug, Deserialize)]
pub struct LogParams {
    /// Resume tailing from this char offset. Omit for the full output.
    pub offset: Option<usize>,
}

/// `GET /api/runs/:id/graph` — task nodes + dependency edges for one run.
/// Returns empty arrays (not 404) for a real run with no tasks.
pub async fn get_graph(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<GraphResponse>, StatusCode> {
    let nodes = sqlx::query_as::<_, GraphNode>(
        "SELECT id, name, status, attempt, scheduled_at, finished_at
         FROM task_runs WHERE run_id = $1",
    )
    .bind(&id)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    // edge: dependency_id (source) → dependent_id (target). Scope to this run's
    // tasks so a caller can't enumerate edges across runs.
    let edges = sqlx::query_as::<_, GraphEdge>(
        "SELECT dependency_id AS source, dependent_id AS target
         FROM task_dependencies
         WHERE dependent_id IN (SELECT id FROM task_runs WHERE run_id = $1)",
    )
    .bind(&id)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    Ok(Json(GraphResponse { nodes, edges }))
}

/// `GET /api/runs/:id/tasks/:tid/logs[?offset=N]` — one task's output, scoped to
/// the run (so a task id can't be probed against the wrong run). 404 if not found.
/// With `?offset=` it returns only the output past that char offset for live
/// tailing (#17): poll with `?offset=next_offset` until `eof`.
pub async fn get_task_logs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
    Query(params): Query<LogParams>,
) -> Result<Json<TaskLogs>, StatusCode> {
    let row = sqlx::query_as::<_, TaskLogRow>(
        "SELECT id AS task_id, name, status, attempt, output
         FROM task_runs WHERE id = $1 AND run_id = $2",
    )
    .bind(&tid)
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?
    .ok_or(StatusCode::NOT_FOUND)?;

    let full = row.output.unwrap_or_default();
    let total = full.chars().count();
    let eof = matches!(row.status.as_str(), "succeeded" | "failed" | "skipped" | "cancelled");
    // Slice on a char boundary by skipping whole scalars — never panics on UTF-8.
    let output = match params.offset {
        Some(off) if off < total => Some(full.chars().skip(off).collect::<String>()),
        Some(_) => Some(String::new()), // caller is caught up
        None => Some(full),
    };
    let offset = params.offset.unwrap_or(0).min(total);
    Ok(Json(TaskLogs {
        task_id: row.task_id,
        name: row.name,
        status: row.status,
        attempt: row.attempt,
        output,
        offset,
        next_offset: total,
        eof,
    }))
}

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}
