//! HTTP management API (v5).
//!
//! A read/write control surface over the datastore — list runs, inspect a run's
//! task state, submit a new DAG, cancel a run, and scrape metrics. Built on
//! `axum`. Every handler is a thin shell over the same `db` facade the reconcile
//! loop uses: the API never holds scheduler state of its own, so it works
//! unchanged whether it runs in-process beside the loop or as a standalone
//! read-only sidecar against the shared Postgres backend.
//!
//! | Method & path        | Purpose                                            |
//! |----------------------|----------------------------------------------------|
//! | `GET  /healthz`      | liveness probe                                     |
//! | `GET  /metrics`      | Prometheus exposition (process counters + DB gauges)|
//! | `GET  /runs`         | list runs (`?status=`, `?limit=`)                  |
//! | `POST /runs`         | submit a DAG (YAML/JSON body) → `{ run_id }`        |
//! | `GET  /runs/{id}`    | run detail + its task rows                          |
//! | `POST /runs/{id}/cancel` | cancel a running run                           |
//! | `GET  /dead-letters` | list parked poison submissions                     |
//! | `POST /dead-letters/{id}/redrive` | re-attempt a dead letter as a run     |
//! | `DELETE /dead-letters/{id}` | discard a dead letter                       |
//! | `GET  /openapi.yaml` | this API's OpenAPI 3.0 spec (YAML)                 |
//! | `GET  /openapi.json` | the same spec as JSON                              |
//! | `GET  /docs`         | Swagger UI rendering the spec                      |
//!
//! The OpenAPI document (`openapi.yaml`, embedded at build time) is the source of
//! truth for request/response shapes — keep it in sync when changing an endpoint.

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use tracing::{error, info};

/// The OpenAPI spec, embedded at compile time so the binary is self-describing.
const OPENAPI_YAML: &str = include_str!("../openapi.yaml");

use crate::dag::DagGraph;
use crate::db;
use crate::metrics::Metrics;
use crate::models::RunStatus;

/// Shared handler state. `Pool` and `Arc<Metrics>` are both cheap to clone, so
/// the whole struct is `Clone` as axum requires.
#[derive(Clone)]
pub struct ApiState {
    pub pool: db::Pool,
    pub metrics: Arc<Metrics>,
    /// Admission cap for `POST /runs`: when the datastore already holds this many
    /// active (pending/running) runs, the submit path sheds load with `429 Too
    /// Many Requests` + `Retry-After` instead of growing an unbounded backlog.
    /// `0` disables the cap (the historical "accept everything" behaviour).
    pub max_inflight_runs: i64,
}

/// Bind `addr` and serve the management API until the process exits.
pub async fn serve(addr: SocketAddr, state: ApiState) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "management API listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

/// Build the router. Split out so tests can exercise handlers without a socket.
pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/openapi.yaml", get(openapi_yaml))
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(docs))
        .route("/runs", get(list_runs).post(submit_run))
        .route("/runs/{id}", get(get_run))
        .route("/runs/{id}/cancel", post(cancel_run))
        .route("/runs/{id}/rerun", post(rerun_run))
        .route("/dead-letters", get(list_dead_letters))
        .route("/dead-letters/{id}/redrive", post(redrive_dead_letter))
        .route("/dead-letters/{id}", axum::routing::delete(delete_dead_letter))
        .with_state(state)
}

// ── Handlers ────────────────────────────────────────────────────────────────

async fn healthz() -> &'static str {
    "ok"
}

/// Serve the embedded OpenAPI document as YAML.
async fn openapi_yaml() -> Response {
    ([(header::CONTENT_TYPE, "application/yaml")], OPENAPI_YAML).into_response()
}

/// Serve the same spec as JSON. Parsed once from the embedded YAML (YAML is a
/// JSON superset) and cached, so the YAML file stays the single source of truth.
async fn openapi_json() -> Result<Json<&'static serde_json::Value>, ApiError> {
    static SPEC: OnceLock<serde_json::Value> = OnceLock::new();
    // `get_or_init` can't fail, so parse eagerly here and surface any error as 500
    // (only reachable if the embedded spec is malformed — caught by the unit test).
    if SPEC.get().is_none() {
        let parsed: serde_json::Value = serde_yaml::from_str(OPENAPI_YAML)
            .map_err(|e| anyhow::anyhow!("embedded openapi.yaml is not valid: {e}"))?;
        let _ = SPEC.set(parsed);
    }
    Ok(Json(SPEC.get().expect("spec initialized above")))
}

/// A self-contained Swagger UI page pointing at `/openapi.yaml`. UI assets load
/// from a CDN, so the page needs outbound internet; the raw spec does not.
async fn docs() -> Html<&'static str> {
    Html(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>module-54 scheduler — API docs</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
  <script>
    window.ui = SwaggerUIBundle({ url: "/openapi.yaml", dom_id: "#swagger-ui" });
  </script>
</body>
</html>"##,
    )
}

async fn metrics(State(st): State<ApiState>) -> Result<Response, ApiError> {
    let snap = db::status_counts(&st.pool).await?;
    let pool_stats = crate::metrics::DbPoolStats {
        connections: st.pool.size(),
        idle: st.pool.num_idle() as u32,
        max: st.pool.options().get_max_connections(),
    };
    let body = st.metrics.render(&snap, Some(&pool_stats));
    Ok((
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response())
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    status: Option<String>,
    limit: Option<i64>,
}

async fn list_runs(
    State(st): State<ApiState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Cap the page size so a hostile/typo'd `limit` can't ask for the whole table.
    let limit = q.limit.unwrap_or(50).clamp(1, 1000);
    let runs = db::list_runs(&st.pool, q.status.as_deref(), limit).await?;
    Ok(Json(json!({ "runs": runs })))
}

async fn get_run(
    State(st): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Some(run) = db::get_run(&st.pool, &id).await? else {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    };
    let tasks = db::list_tasks(&st.pool, &id).await?;
    Ok(Json(json!({ "run": run, "tasks": tasks })))
}

async fn submit_run(
    State(st): State<ApiState>,
    body: String,
) -> Result<Response, ApiError> {
    let dag = DagGraph::from_yaml(&body)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("invalid DAG: {e}")))?;

    // Admission control: shed load before it becomes an unbounded backlog. This
    // is the API-path counterpart to the ingest source's MAX_INFLIGHT_RUNS valve
    // — without it, `POST /runs` accepts faster than the engine can drain (the
    // LOADTEST.md finding). A 429 + Retry-After tells clients to back off.
    if st.max_inflight_runs > 0 {
        let active = db::count_active_runs(&st.pool).await?;
        if active >= st.max_inflight_runs {
            info!(active, cap = st.max_inflight_runs, "run rejected — at inflight cap");
            return Ok((
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "1")],
                Json(json!({
                    "error": "too many in-flight runs",
                    "active": active,
                    "max_inflight_runs": st.max_inflight_runs,
                })),
            )
                .into_response());
        }
    }

    let run_id = db::create_run(&st.pool, &dag, &body).await?;
    st.metrics.inc_runs_created();
    info!(%run_id, name = %dag.spec.name, "run submitted via API");
    Ok((StatusCode::CREATED, Json(json!({ "run_id": run_id }))).into_response())
}

async fn cancel_run(
    State(st): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let cancelled = db::cancel_run(&st.pool, &id).await?;
    if !cancelled {
        // Either the run does not exist or it is already terminal — both are
        // "nothing to cancel" from the caller's view.
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!("run '{id}' is not running (missing or already terminal)"),
        ));
    }
    info!(run_id = %id, "run cancelled via API");
    Ok(Json(json!({ "run_id": id, "cancelled": true })))
}

/// Optional body for `POST /runs/{id}/rerun`. `from` selects the rerun mode;
/// only `"failed"` (the default) is supported today — task-anchored rerun
/// (`task:<id>`) is reserved. Parameter override (`params`) is offered by the
/// dagron-api gateway, not this single-binary surface.
#[derive(Debug, Deserialize, Default)]
struct RerunBody {
    #[serde(default)]
    from: Option<String>,
}

/// `POST /runs/{id}/rerun` — cascade rerun a failed/cancelled run from its
/// failure frontier: every failed/cancelled task is reset to pending and the run
/// re-armed, while succeeded tasks are left intact. 404 if the run is unknown,
/// 409 if it is not in a rerunnable state, 400 for an unsupported `from` mode.
async fn rerun_run(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    body: Option<Json<RerunBody>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if let Some(Json(b)) = &body {
        if let Some(from) = &b.from {
            if from != "failed" {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    format!("unsupported rerun mode '{from}'; only 'failed' is supported"),
                ));
            }
        }
    }

    let Some(run) = db::get_run(&st.pool, &id).await? else {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    };
    if !matches!(run.status, RunStatus::Failed | RunStatus::Cancelled) {
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!("run '{id}' is not in a rerunnable state (failed/cancelled)"),
        ));
    }

    // The pre-check above is best-effort; `rerun_from_failed` re-checks atomically.
    // `None` means the run lost its rerunnable state in a concurrent race, so honor
    // the route contract with a 409 rather than reporting a false success.
    let Some(reset) = db::rerun_from_failed(&st.pool, &id).await? else {
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!("run '{id}' is not in a rerunnable state (failed/cancelled)"),
        ));
    };
    info!(run_id = %id, reset, "run reran from failure via API");
    Ok(Json(json!({ "run_id": id, "rerun": reset })))
}

async fn list_dead_letters(
    State(st): State<ApiState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = q.limit.unwrap_or(50).clamp(1, 1000);
    let dead_letters = db::list_dead_letters(&st.pool, limit).await?;
    Ok(Json(json!({ "dead_letters": dead_letters })))
}

/// Re-attempt a dead letter as a fresh run. On success the dead letter is
/// removed; a still-invalid payload returns `400` and the row is kept.
async fn redrive_dead_letter(
    State(st): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Some(dl) = db::get_dead_letter(&st.pool, &id).await? else {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("dead letter '{id}' not found")));
    };
    // Parse before claiming so an invalid payload keeps the row for inspection.
    let dag = DagGraph::from_yaml(&dl.payload).map_err(|e| {
        ApiError(StatusCode::BAD_REQUEST, format!("dead letter still invalid: {e}"))
    })?;
    // Atomic claim gate: the row delete serializes concurrent redrives, so only
    // the caller that wins the delete creates a run; a loser sees `false` and
    // gets 409 instead of a duplicate run.
    if !db::delete_dead_letter(&st.pool, &id).await? {
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!("dead letter '{id}' was already redriven or discarded"),
        ));
    }
    let run_id = match db::create_run(&st.pool, &dag, &dl.payload).await {
        Ok(run_id) => run_id,
        Err(e) => {
            // The row is already claimed (deleted); surface the payload so the
            // operator can recover it rather than losing it silently.
            error!(dead_letter_id = %id, payload = %dl.payload, error = ?e, "redrive create_run failed after claim");
            return Err(e.into());
        }
    };
    st.metrics.inc_runs_created();
    info!(dead_letter_id = %id, %run_id, "dead letter redriven into a run");
    Ok(Json(json!({ "run_id": run_id, "redriven_from": id })))
}

async fn delete_dead_letter(
    State(st): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if !db::delete_dead_letter(&st.pool, &id).await? {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("dead letter '{id}' not found")));
    }
    info!(dead_letter_id = %id, "dead letter discarded");
    Ok(Json(json!({ "id": id, "deleted": true })))
}

// ── Error type ──────────────────────────────────────────────────────────────

/// Handler error carrying an HTTP status + message. `anyhow::Error` (e.g. a DB
/// failure) maps to `500` via `From`, so handlers can `?` their db calls and
/// only spell out the deliberate 4xx cases.
#[derive(Debug)]
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        // Log the real cause server-side; never leak backend/infra details (SQLx,
        // IO, connection strings) to the caller.
        error!(error = ?e, "management API request failed");
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The embedded spec must parse and describe every route the router exposes,
    /// so `/openapi.json` can't 500 and the docs can't silently drift from the API.
    #[test]
    fn embedded_openapi_is_valid_and_covers_all_routes() {
        let spec: serde_json::Value =
            serde_yaml::from_str(OPENAPI_YAML).expect("openapi.yaml parses");
        assert_eq!(spec["openapi"], "3.0.3");
        let paths = spec["paths"].as_object().expect("paths object");
        for route in [
            "/healthz",
            "/metrics",
            "/openapi.yaml",
            "/openapi.json",
            "/docs",
            "/runs",
            "/runs/{id}",
            "/runs/{id}/cancel",
            "/runs/{id}/rerun",
            "/dead-letters",
            "/dead-letters/{id}/redrive",
            "/dead-letters/{id}",
        ] {
            assert!(paths.contains_key(route), "spec missing path {route}");
        }
        // Both run mutations are documented.
        assert!(spec["paths"]["/runs"].get("post").is_some());
        assert!(spec["paths"]["/runs/{id}/cancel"].get("post").is_some());
    }

    /// Per-test SQLite database in a unique temp file.
    async fn temp_state(max_inflight_runs: i64) -> (ApiState, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("module54_api_{}.db", uuid::Uuid::new_v4()));
        let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
        let state = ApiState {
            pool,
            metrics: Arc::new(Metrics::new()),
            max_inflight_runs,
        };
        (state, path)
    }

    const ONE_TASK_DAG: &str = "name: t\ntasks:\n  - name: a\n    command: [\"true\"]\n";

    /// The admission cap sheds load with 429 once the datastore is at the
    /// in-flight ceiling, and accepts again once it drops below.
    #[tokio::test]
    async fn submit_run_sheds_load_at_inflight_cap() {
        let (state, path) = temp_state(1).await;

        // First submit is under the cap (0 active) → 201 Created.
        let first = submit_run(State(state.clone()), ONE_TASK_DAG.to_string())
            .await
            .unwrap()
            .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);

        // That run is now active (1 >= cap of 1) → next submit is rejected 429.
        let second = submit_run(State(state.clone()), ONE_TASK_DAG.to_string())
            .await
            .unwrap()
            .into_response();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(second.headers().get(header::RETRY_AFTER).unwrap(), "1");

        state.pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A cap of 0 disables admission control — submits always pass.
    #[tokio::test]
    async fn submit_run_uncapped_when_zero() {
        let (state, path) = temp_state(0).await;
        for _ in 0..3 {
            let r = submit_run(State(state.clone()), ONE_TASK_DAG.to_string())
                .await
                .unwrap()
                .into_response();
            assert_eq!(r.status(), StatusCode::CREATED);
        }
        state.pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}
