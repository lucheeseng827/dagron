//! DAG graph + task-log read endpoints.
//!
//! `graph` returns nodes (task_runs) and edges (task_dependencies) shaped so the
//! React Flow client consumes edges as `source`/`target` directly.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;

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

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct TaskLogs {
    pub task_id: String,
    pub name: String,
    pub status: String,
    pub attempt: i64,
    pub output: Option<String>,
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

/// `GET /api/runs/:id/tasks/:tid/logs` — one task's output, scoped to the run
/// (so a task id can't be probed against the wrong run). 404 if not found.
pub async fn get_task_logs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
) -> Result<Json<TaskLogs>, StatusCode> {
    let logs = sqlx::query_as::<_, TaskLogs>(
        "SELECT id AS task_id, name, status, attempt, output
         FROM task_runs WHERE id = $1 AND run_id = $2",
    )
    .bind(&tid)
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?
    .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(logs))
}

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}
