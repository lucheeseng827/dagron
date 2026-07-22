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

/// Swagger UI assets, vendored and embedded so `/docs` renders with no outbound
/// internet (air-gap). Pinned version in `assets/swagger-ui/VERSION`.
const SWAGGER_UI_CSS: &[u8] = include_bytes!("../assets/swagger-ui/swagger-ui.css");
const SWAGGER_UI_JS: &[u8] = include_bytes!("../assets/swagger-ui/swagger-ui-bundle.js");

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
        .route("/docs/swagger-ui.css", get(swagger_ui_css))
        .route("/docs/swagger-ui-bundle.js", get(swagger_ui_js))
        .route("/runs", get(list_runs).post(submit_run))
        .route("/runs/{id}", get(get_run))
        .route("/runs/{id}/wait", get(wait_run))
        .route("/runs/{id}/tasks/{task_id}/logs", get(task_logs))
        .route("/runs/{id}/cancel", post(cancel_run))
        .route("/runs/{id}/rerun", post(rerun_run))
        .route("/runs/{id}/tasks/{task_id}/clear", post(clear_task))
        .route("/runs/{id}/tasks/{task_id}/approve", post(approve_task))
        .route("/runs/{id}/tasks/{task_id}/reject", post(reject_task))
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

/// A self-contained Swagger UI page pointing at `/openapi.yaml`. Assets are
/// served from this binary (`/docs/swagger-ui.*`, vendored), so the page works
/// with no outbound internet — same air-gap posture as the rest of the engine.
async fn docs() -> Html<&'static str> {
    Html(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>module-54 scheduler — API docs</title>
  <link rel="stylesheet" href="/docs/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="/docs/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({ url: "/openapi.yaml", dom_id: "#swagger-ui" });
  </script>
</body>
</html>"##,
    )
}

/// Vendored Swagger UI assets, served locally so `/docs` needs no CDN (air-gap).
async fn swagger_ui_css() -> Response {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], SWAGGER_UI_CSS).into_response()
}

async fn swagger_ui_js() -> Response {
    (
        [(header::CONTENT_TYPE, "application/javascript; charset=utf-8")],
        SWAGGER_UI_JS,
    )
        .into_response()
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

/// Query for `POST /runs`. `wait=true` turns the submit into a **synchronous
/// invocation** (fast-win #15): the handler blocks until the run reaches a
/// terminal state (or `timeout_secs` elapses) and returns its status + result,
/// so dagron is callable as a durable function. Default (`wait` absent) keeps
/// the fire-and-forget `201 {run_id}` behaviour.
#[derive(Debug, Deserialize)]
struct SubmitQuery {
    #[serde(default)]
    wait: bool,
    timeout_secs: Option<u64>,
}

/// Query for `GET /runs/{id}/wait` — long-poll an existing run to terminal.
#[derive(Debug, Deserialize)]
struct WaitQuery {
    timeout_secs: Option<u64>,
}

/// Default / clamp for a wait's budget: 30s default, 1s..=600s allowed.
fn wait_timeout(secs: Option<u64>) -> std::time::Duration {
    std::time::Duration::from_secs(secs.unwrap_or(30).clamp(1, 600))
}

/// Poll a run until it reaches a terminal state or the deadline elapses. Returns
/// `None` if the run doesn't exist, else the last-observed run row. The reconcile
/// loop drives the run concurrently; a short DB poll keeps this backend-agnostic
/// (no `LISTEN` dependency) and correct against both the in-process dev loop and
/// a shared-Postgres engine elsewhere.
async fn wait_for_run(
    pool: &db::Pool,
    run_id: &str,
    timeout: std::time::Duration,
) -> Result<Option<crate::models::WorkflowRun>, ApiError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let Some(run) = db::get_run(pool, run_id).await? else {
            return Ok(None);
        };
        if run.status.is_terminal() || tokio::time::Instant::now() >= deadline {
            return Ok(Some(run));
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// The synchronous-invocation response body: run id, current status, whether it
/// finished, and its result (the `result_from` task's output on success, else
/// null). A timed-out wait returns `finished: false` with the live status so the
/// caller can re-poll — not an error.
fn run_result_json(run: &crate::models::WorkflowRun) -> serde_json::Value {
    // `result` is the run output only on success; a failed/cancelled run's output
    // is an error message, not a result.
    let result = if run.status == RunStatus::Succeeded {
        run.output.clone()
    } else {
        None
    };
    json!({
        "run_id": run.id,
        "status": run.status.to_string(),
        "finished": run.status.is_terminal(),
        "result": result,
    })
}

async fn submit_run(
    State(st): State<ApiState>,
    Query(q): Query<SubmitQuery>,
    body: String,
) -> Result<Response, ApiError> {
    // `{{ env.* }}` variables from the spec's declared environment; an unknown
    // environment is a 400, not a run without its variables.
    let env_params = crate::environments::template_params(&st.pool, &body)
        .await
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("{e}")))?;
    let dag = DagGraph::from_yaml_with_params(&body, &env_params)
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
    info!(%run_id, name = %dag.spec.name, wait = q.wait, "run submitted via API");

    // Synchronous invocation (#15): block until the run finishes (or the wait
    // budget elapses) and return its result inline, instead of just the id.
    if q.wait {
        let Some(run) = wait_for_run(&st.pool, &run_id, wait_timeout(q.timeout_secs)).await? else {
            // The run existed a line ago; only a concurrent GC could remove it.
            return Err(ApiError(StatusCode::NOT_FOUND, format!("run '{run_id}' not found")));
        };
        // 200 (not 201) — the resource was created *and* awaited; the body is the
        // outcome. A timed-out wait still returns 200 with `finished: false`.
        return Ok((StatusCode::OK, Json(run_result_json(&run))).into_response());
    }

    Ok((StatusCode::CREATED, Json(json!({ "run_id": run_id }))).into_response())
}

/// `GET /runs/{id}/wait` — long-poll an existing run until it reaches a terminal
/// state (or `?timeout_secs=` elapses) and return its status + result. 404 if the
/// run is unknown; a timed-out wait is 200 with `finished: false` so the caller
/// re-polls. This is the "await an already-submitted run" half of #15.
async fn wait_run(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Query(q): Query<WaitQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Some(run) = wait_for_run(&st.pool, &id, wait_timeout(q.timeout_secs)).await? else {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    };
    Ok(Json(run_result_json(&run)))
}

/// Query for `GET /runs/{id}/tasks/{task_id}/logs` — resume a tail from a char
/// offset (#17).
#[derive(Debug, Deserialize)]
struct LogQuery {
    offset: Option<usize>,
}

/// `GET /runs/{id}/tasks/{task_id}/logs[?offset=N]` — one task's output for live
/// tailing. With `?offset=` returns only the output past that char offset plus a
/// `next_offset` to resume from and `eof` (the task is terminal); poll until
/// `eof`. 404 if the run or task is unknown.
async fn task_logs(
    State(st): State<ApiState>,
    Path((id, task_id)): Path<(String, String)>,
    Query(q): Query<LogQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if db::get_run(&st.pool, &id).await?.is_none() {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    }
    let tasks = db::list_tasks(&st.pool, &id).await?;
    let Some(task) = tasks.into_iter().find(|t| t.id == task_id) else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("task '{task_id}' not found in run '{id}'"),
        ));
    };

    let full = task.output.unwrap_or_default();
    let total = full.chars().count();
    let eof = matches!(task.status.to_string().as_str(), "succeeded" | "failed" | "skipped" | "cancelled");
    // Char-boundary slice — never splits a multibyte scalar.
    let output = match q.offset {
        Some(off) if off < total => full.chars().skip(off).collect::<String>(),
        Some(_) => String::new(),
        None => full,
    };
    Ok(Json(json!({
        "task_id": task.id,
        "name": task.name,
        "status": task.status.to_string(),
        "attempt": task.attempt,
        "output": output,
        "offset": q.offset.unwrap_or(0).min(total),
        "next_offset": total,
        "eof": eof,
    })))
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

/// `POST /runs/{id}/tasks/{task_id}/clear` — clear a single completed task and
/// re-run it together with its transitive downstream cone ("clear +
/// downstream"). The target and every terminal task that depends on it are reset
/// to pending and the run re-armed; already-succeeded tasks outside the cone are
/// left intact. 404 if the run or task is unknown, 409 if the task is not in a
/// terminal state (a running/pending task can't be cleared).
async fn clear_task(
    State(st): State<ApiState>,
    Path((id, task_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if db::get_run(&st.pool, &id).await?.is_none() {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    }
    // `None` distinguishes "unknown task" from "task not terminal"; the DB op
    // guards both under one query, so re-check the task's existence for the 404.
    match db::clear_task_with_downstream(&st.pool, &id, &task_id).await? {
        Some(reset) => {
            info!(run_id = %id, task_id = %task_id, reset, "task cleared with downstream via API");
            Ok(Json(json!({ "run_id": id, "task_id": task_id, "cleared": reset })))
        }
        None => {
            // Disambiguate: a missing task is 404, an existing-but-active task is 409.
            let known = db::task_exists(&st.pool, &id, &task_id).await?;
            if known {
                Err(ApiError(
                    StatusCode::CONFLICT,
                    format!("task '{task_id}' is not in a clearable (completed) state"),
                ))
            } else {
                Err(ApiError(
                    StatusCode::NOT_FOUND,
                    format!("task '{task_id}' not found in run '{id}'"),
                ))
            }
        }
    }
}

/// `POST /runs/{id}/tasks/{task_id}/approve` — approve a human approval gate
/// (#19): the task succeeds and its dependents advance. 404 if the run/task is
/// unknown, 409 if the task is not awaiting approval.
async fn approve_task(
    State(st): State<ApiState>,
    Path((id, task_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    resolve_approval_gate(&st, &id, &task_id, true).await
}

/// `POST /runs/{id}/tasks/{task_id}/reject` — reject a human approval gate: the
/// task fails and its `all_success` dependents skip. Same status codes as approve.
async fn reject_task(
    State(st): State<ApiState>,
    Path((id, task_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    resolve_approval_gate(&st, &id, &task_id, false).await
}

async fn resolve_approval_gate(
    st: &ApiState,
    id: &str,
    task_id: &str,
    approve: bool,
) -> Result<Json<serde_json::Value>, ApiError> {
    if db::get_run(&st.pool, id).await?.is_none() {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    }
    if db::resolve_approval(&st.pool, id, task_id, approve).await? {
        let resolution = if approve { "approved" } else { "rejected" };
        info!(run_id = %id, task_id = %task_id, resolution, "approval gate resolved via API");
        return Ok(Json(json!({ "run_id": id, "task_id": task_id, "resolution": resolution })));
    }
    // Disambiguate: unknown task → 404, existing-but-not-awaiting → 409.
    if db::task_exists(&st.pool, id, task_id).await? {
        Err(ApiError(
            StatusCode::CONFLICT,
            format!("task '{task_id}' is not awaiting approval"),
        ))
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("task '{task_id}' not found in run '{id}'"),
        ))
    }
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
            "/runs/{id}/wait",
            "/runs/{id}/tasks/{task_id}/logs",
            "/runs/{id}/cancel",
            "/runs/{id}/rerun",
            "/runs/{id}/tasks/{task_id}/clear",
            "/runs/{id}/tasks/{task_id}/approve",
            "/runs/{id}/tasks/{task_id}/reject",
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

    /// Air-gap guard: `/docs` must render with no CDN, so the Swagger UI assets
    /// are served from this binary with the right content types and non-empty
    /// bodies. A regression here would silently re-break the offline docs page.
    #[tokio::test]
    async fn swagger_ui_assets_are_served_locally() {
        for (resp, want_ct) in [
            (swagger_ui_css().await, "text/css"),
            (swagger_ui_js().await, "application/javascript"),
        ] {
            assert_eq!(resp.status(), StatusCode::OK);
            let ct = resp
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            assert!(ct.starts_with(want_ct), "unexpected content-type {ct:?}");
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            assert!(!body.is_empty(), "vendored asset body is empty");
        }
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

    /// The fire-and-forget submit query (no synchronous wait), for tests that
    /// exercise the plain `POST /runs` path.
    fn no_wait() -> Query<SubmitQuery> {
        Query(SubmitQuery { wait: false, timeout_secs: None })
    }

    /// The admission cap sheds load with 429 once the datastore is at the
    /// in-flight ceiling, and accepts again once it drops below.
    #[tokio::test]
    async fn submit_run_sheds_load_at_inflight_cap() {
        let (state, path) = temp_state(1).await;

        // First submit is under the cap (0 active) → 201 Created.
        let first = submit_run(State(state.clone()), no_wait(), ONE_TASK_DAG.to_string())
            .await
            .unwrap()
            .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);

        // That run is now active (1 >= cap of 1) → next submit is rejected 429.
        let second = submit_run(State(state.clone()), no_wait(), ONE_TASK_DAG.to_string())
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
            let r = submit_run(State(state.clone()), no_wait(), ONE_TASK_DAG.to_string())
                .await
                .unwrap()
                .into_response();
            assert_eq!(r.status(), StatusCode::CREATED);
        }
        state.pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// `POST /runs/{id}/tasks/{tid}/clear`: clears a completed task's cone (200),
    /// and disambiguates unknown-run/unknown-task (404) from not-terminal (409).
    #[tokio::test]
    async fn clear_task_handler_paths() {
        let (state, path) = temp_state(0).await;
        let yaml = "name: chain\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [\"a\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = db::create_run(&state.pool, &dag, yaml).await.unwrap();
        let tasks = db::list_tasks(&state.pool, &run_id).await.unwrap();
        let a = tasks.iter().find(|t| t.name == "a").unwrap();

        // Unknown run → 404.
        let e = clear_task(State(state.clone()), Path(("nope".into(), a.id.clone())))
            .await
            .unwrap_err();
        assert_eq!(e.0, StatusCode::NOT_FOUND);

        // Known run, unknown task → 404.
        let e = clear_task(State(state.clone()), Path((run_id.clone(), "nope".into())))
            .await
            .unwrap_err();
        assert_eq!(e.0, StatusCode::NOT_FOUND);

        // Task 'a' is pending (non-terminal) → 409.
        let e = clear_task(State(state.clone()), Path((run_id.clone(), a.id.clone())))
            .await
            .unwrap_err();
        assert_eq!(e.0, StatusCode::CONFLICT);

        // Drive the run to success, then clearing 'a' resets a + downstream b → 200.
        sqlx::query("UPDATE task_runs SET status = 'succeeded' WHERE run_id = ?")
            .bind(&run_id)
            .execute(&state.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded', finished_at = '2026-01-01T00:00:00Z' WHERE id = ?")
            .bind(&run_id)
            .execute(&state.pool)
            .await
            .unwrap();
        let ok = clear_task(State(state.clone()), Path((run_id.clone(), a.id.clone())))
            .await
            .unwrap();
        assert_eq!(ok.0["cleared"], 2, "a and its downstream b are reset");

        state.pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Synchronous invocation (#15): `GET /runs/{id}/wait` returns the run's
    /// result once terminal (result = the `result_from` task's output), 404s on an
    /// unknown run, and returns `finished:false` on a timed-out wait.
    #[tokio::test]
    async fn wait_run_returns_result_and_times_out() {
        let (state, path) = temp_state(0).await;

        // Unknown run → 404.
        let e = wait_run(
            State(state.clone()),
            Path("nope".into()),
            Query(WaitQuery { timeout_secs: Some(1) }),
        )
        .await
        .unwrap_err();
        assert_eq!(e.0, StatusCode::NOT_FOUND);

        // A run whose result_from task succeeded returns that task's output.
        let yaml = "name: fn\nresult_from: a\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = db::create_run(&state.pool, &dag, yaml).await.unwrap();
        // Drive it: task a succeeds with an output, reap finalizes → run output set.
        db::advance_ready_tasks(&state.pool).await.unwrap();
        for t in db::claim_ready(&state.pool, "w", 10).await.unwrap() {
            db::mark_task_succeeded(&state.pool, &t.id, "w", t.version + 1, Some("42".into()))
                .await
                .unwrap();
        }
        db::reap_completed_runs(&state.pool).await.unwrap();

        let done = wait_run(
            State(state.clone()),
            Path(run_id.clone()),
            Query(WaitQuery { timeout_secs: Some(2) }),
        )
        .await
        .unwrap();
        assert_eq!(done.0["status"], "succeeded");
        assert_eq!(done.0["finished"], true);
        assert_eq!(done.0["result"], "42");

        // A still-running run times out with finished:false (not an error).
        let run2 = db::create_run(&state.pool, &dag, yaml).await.unwrap();
        let pending = wait_run(
            State(state.clone()),
            Path(run2.clone()),
            Query(WaitQuery { timeout_secs: Some(1) }),
        )
        .await
        .unwrap();
        assert_eq!(pending.0["finished"], false);
        assert_eq!(pending.0["status"], "running");

        state.pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Live-log tail (#17): `GET /runs/{id}/tasks/{tid}/logs` returns the full
    /// output, an `?offset=` slice with a `next_offset` to resume from, `eof`
    /// tracking terminality, and 404 for unknown run/task.
    #[tokio::test]
    async fn task_logs_tails_from_offset() {
        let (state, path) = temp_state(0).await;
        let yaml = "name: t\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = db::create_run(&state.pool, &dag, yaml).await.unwrap();

        // Unknown run → 404.
        let e = task_logs(
            State(state.clone()),
            Path(("nope".into(), "x".into())),
            Query(LogQuery { offset: None }),
        )
        .await
        .unwrap_err();
        assert_eq!(e.0, StatusCode::NOT_FOUND);

        // Claim task a (→ running) and stream two chunks.
        db::advance_ready_tasks(&state.pool).await.unwrap();
        let claimed = db::claim_ready(&state.pool, "w", 10).await.unwrap();
        let a = &claimed[0];
        let fence = a.version + 1; // claim bumped the row's version
        db::append_task_output(&state.pool, &a.id, fence, "hello\n", true).await.unwrap();
        db::append_task_output(&state.pool, &a.id, fence, "world\n", false).await.unwrap();

        // Unknown task → 404.
        let e = task_logs(
            State(state.clone()),
            Path((run_id.clone(), "nope".into())),
            Query(LogQuery { offset: None }),
        )
        .await
        .unwrap_err();
        assert_eq!(e.0, StatusCode::NOT_FOUND);

        // No offset → full output; running task → eof false.
        let full = task_logs(
            State(state.clone()),
            Path((run_id.clone(), a.id.clone())),
            Query(LogQuery { offset: None }),
        )
        .await
        .unwrap();
        assert_eq!(full.0["output"], "hello\nworld\n");
        assert_eq!(full.0["eof"], false);
        let total = full.0["next_offset"].as_u64().unwrap();
        assert_eq!(total, 12);

        // Resume from the first line's length → only the tail is returned.
        let tail = task_logs(
            State(state.clone()),
            Path((run_id.clone(), a.id.clone())),
            Query(LogQuery { offset: Some(6) }),
        )
        .await
        .unwrap();
        assert_eq!(tail.0["output"], "world\n");
        assert_eq!(tail.0["offset"], 6);

        // Finalize → eof true.
        assert!(db::mark_task_succeeded(&state.pool, &a.id, "w", fence, Some("done".into()))
            .await
            .unwrap());
        let done = task_logs(
            State(state.clone()),
            Path((run_id.clone(), a.id.clone())),
            Query(LogQuery { offset: None }),
        )
        .await
        .unwrap();
        assert_eq!(done.0["eof"], true);

        state.pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Approval gate handlers (#19): approve resolves an `awaiting_approval` gate
    /// (200), and unknown run/task (404) and not-awaiting (409) are disambiguated.
    #[tokio::test]
    async fn approve_reject_handler_paths() {
        let (state, path) = temp_state(0).await;
        let yaml = "name: appr\ntasks:\n  - name: build\n    command: [\"true\"]\n  - name: gate\n    type: approval\n    depends_on: [build]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = db::create_run(&state.pool, &dag, yaml).await.unwrap();
        let gate_id = db::list_tasks(&state.pool, &run_id)
            .await
            .unwrap()
            .into_iter()
            .find(|t| t.name == "gate")
            .unwrap()
            .id;

        // Unknown run → 404.
        assert_eq!(
            approve_task(State(state.clone()), Path(("nope".into(), gate_id.clone())))
                .await
                .unwrap_err()
                .0,
            StatusCode::NOT_FOUND
        );
        // Unknown task → 404.
        assert_eq!(
            approve_task(State(state.clone()), Path((run_id.clone(), "nope".into())))
                .await
                .unwrap_err()
                .0,
            StatusCode::NOT_FOUND
        );
        // The gate is still `pending` (build hasn't run) → not awaiting → 409.
        assert_eq!(
            approve_task(State(state.clone()), Path((run_id.clone(), gate_id.clone())))
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );

        // Drive build to success and advance so the gate parks in awaiting_approval.
        db::advance_ready_tasks(&state.pool).await.unwrap();
        let build = db::claim_ready(&state.pool, "w", 10).await.unwrap();
        db::mark_task_succeeded(&state.pool, &build[0].id, "w", build[0].version + 1, None)
            .await
            .unwrap();
        db::advance_ready_tasks(&state.pool).await.unwrap();

        // Approve → 200 with resolution "approved".
        let ok = approve_task(State(state.clone()), Path((run_id.clone(), gate_id.clone())))
            .await
            .unwrap();
        assert_eq!(ok.0["resolution"], "approved");
        // Re-approving is now 409 (already resolved).
        assert_eq!(
            approve_task(State(state.clone()), Path((run_id.clone(), gate_id.clone())))
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );

        state.pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}
