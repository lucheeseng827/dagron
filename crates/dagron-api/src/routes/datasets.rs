//! Dataset registry + lineage reads for the console (data-aware scheduling).
//!
//! The engine's own ops API already serves `GET /datasets` and
//! `GET /datasets/events`, but that surface is unauthenticated and the console
//! never talks to it — so the registry and the update trail, which are the whole
//! point of `produces:`, were `curl`-only. These are the same two reads behind
//! this gateway's auth, off the read pool, plus the `on_datasets:` subscriptions
//! so a viewer can see *who wakes up* when a dataset updates.
//!
//! Read-only by design. Recording an update is `produces:` on a task (open) or
//! `POST /datasets/events` on the engine (Enterprise); neither belongs here.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::state::AppState;

const DEFAULT_LIST_LIMIT: i64 = 100;
const MAX_LIST_LIMIT: i64 = 500;

fn internal(e: sqlx::Error) -> StatusCode {
    tracing::error!(error = %e, "dataset query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

fn clamp_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(DEFAULT_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT)
}

/// One row of the dataset registry: the current state of a dataset.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct DatasetRow {
    pub uri: String,
    pub updated_at: String,
    pub last_run_id: Option<String>,
    pub last_task: Option<String>,
    pub updates: i64,
    /// Workflows subscribed to this dataset via `on_datasets:` — the consumers a
    /// producer wakes. Filled per row from `dataset_triggers`.
    #[sqlx(default)]
    pub consumers: Vec<String>,
}

/// One append-only lineage entry: who updated a dataset, and when.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct DatasetEventRow {
    pub id: i64,
    pub uri: String,
    pub workflow: Option<String>,
    pub run_id: Option<String>,
    pub task_id: Option<String>,
    pub task_name: Option<String>,
    pub source: String,
    pub at: String,
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct EventParams {
    /// Restrict the trail to one dataset. Omitted = the whole ledger, newest first.
    pub uri: Option<String>,
    pub limit: Option<i64>,
}

/// `GET /api/datasets` — the registry, most recently updated first.
pub async fn list_datasets(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(q): Query<ListParams>,
) -> Result<Json<Vec<DatasetRow>>, StatusCode> {
    let mut rows = sqlx::query_as::<_, DatasetRow>(
        "SELECT uri, updated_at, last_run_id, last_task, updates
         FROM datasets ORDER BY updated_at DESC LIMIT $1",
    )
    .bind(clamp_limit(q.limit))
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    // Subscriptions for exactly the datasets listed — one extra query, not one
    // per row. A dataset nobody subscribes to simply keeps its empty vec.
    let uris: Vec<String> = rows.iter().map(|r| r.uri.clone()).collect();
    let subs: Vec<(String, String)> = sqlx::query_as(
        "SELECT uri, workflow_name FROM dataset_triggers
         WHERE uri = ANY($1) ORDER BY workflow_name",
    )
    .bind(&uris)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    for row in &mut rows {
        row.consumers =
            subs.iter().filter(|(uri, _)| *uri == row.uri).map(|(_, wf)| wf.clone()).collect();
    }

    Ok(Json(rows))
}

/// `GET /api/datasets/events` — the lineage ledger, newest first, optionally
/// scoped to one `uri`.
pub async fn list_dataset_events(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(q): Query<EventParams>,
) -> Result<Json<Vec<DatasetEventRow>>, StatusCode> {
    let limit = clamp_limit(q.limit);
    let rows = match q.uri {
        Some(uri) => {
            sqlx::query_as::<_, DatasetEventRow>(
                "SELECT id, uri, workflow, run_id, task_id, task_name, source, at
                 FROM dataset_events WHERE uri = $1 ORDER BY id DESC LIMIT $2",
            )
            .bind(uri)
            .bind(limit)
            .fetch_all(&state.read_pool)
            .await
        }
        None => {
            sqlx::query_as::<_, DatasetEventRow>(
                "SELECT id, uri, workflow, run_id, task_id, task_name, source, at
                 FROM dataset_events ORDER BY id DESC LIMIT $1",
            )
            .bind(limit)
            .fetch_all(&state.read_pool)
            .await
        }
    }
    .map_err(internal)?;

    Ok(Json(rows))
}
