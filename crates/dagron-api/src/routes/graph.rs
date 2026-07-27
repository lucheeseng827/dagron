//! DAG graph read endpoint.
//!
//! `graph` returns nodes (task_runs) and edges (task_dependencies) shaped so the
//! React Flow client consumes edges as `source`/`target` directly.
//!
//! Log reads used to live here too; they moved to [`super::logs`] when they grew
//! a filter grammar, so the graph endpoint and the log views don't share a file
//! just because they both read `task_runs`.

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
    /// Why this node is parked, if it is. A parked task (wait sensor or
    /// sub-workflow trigger) is `running` with a NULL lease, so on the graph it
    /// is a node that looks busy forever. At most one of these is set; the graph
    /// only needs to know *that* it is parked, so the values are carried for the
    /// tooltip rather than re-fetched per node.
    pub wake_at: Option<String>,
    pub wait_url: Option<String>,
    pub wait_dataset: Option<String>,
    pub sub_run_id: Option<String>,
    /// Resolved from the memoization store rather than executed (#22) — the
    /// reason a node can be green with no runtime.
    pub cache_hit: bool,
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

/// `GET /api/runs/:id/graph` — task nodes + dependency edges for one run.
/// Returns empty arrays (not 404) for a real run with no tasks.
pub async fn get_graph(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<GraphResponse>, StatusCode> {
    let nodes = sqlx::query_as::<_, GraphNode>(
        "SELECT id, name, status, attempt, scheduled_at, finished_at,
                wake_at, wait_url, wait_dataset, sub_run_id, cache_hit
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

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}
