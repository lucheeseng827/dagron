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
//! | `GET  /healthz`      | liveness probe (static)                            |
//! | `GET  /readyz`       | readiness probe (datastore round trip)             |
//! | `GET  /config`       | effective configuration + fleet fingerprint        |
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
    extract::{Path, Query, RawQuery, State},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
// The log filter grammar, shared with dagron-api so a filter typed against one
// HTTP surface means the same thing on the other.
use dagron_logging::logfilter::{self, LogFilter};
use serde::Deserialize;
use serde_json::json;
use tracing::{error, info};

/// The OpenAPI spec, embedded at compile time so the binary is self-describing.
const OPENAPI_YAML: &str = include_str!("../openapi.yaml");

/// The operator console, embedded so `dagron` stays ONE static binary with a UI.
///
/// This is the deployment the full console cannot reach: it needs `dagron-api`,
/// Postgres, and a Node build. Here there is no second process and no network
/// fetch, so it works air-gapped and against SQLite on a gateway.
///
/// One dependency-free file (~19 KB) rather than the dagron-api console trimmed
/// down, because that app speaks a different dialect — `/api/runs` returns a bare
/// `RunSummary[]` with `definition_id`/`trigger_kind`/triage fields, this API
/// returns `{"runs": [...]}` — and assumes an auth surface the engine has none of.
/// Its Monaco editor alone is 14 MB; authoring is not what an operator needs here.
const CONSOLE_HTML: &str = include_str!("../assets/console/index.html");

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
    /// Admission cap for `POST /runs` on the dimension that costs something: a
    /// 100k-task run and a 4-task run are both one *run*, so the run cap alone
    /// does not bound the work admitted. Counted against the task ROWS the
    /// submission will create, including gang expansion. `0` disables it.
    pub max_inflight_tasks: i64,
}

/// Bind `addr` and serve the management API until the process exits.
///
/// The exposure warning is not about the console. **This API has never had
/// authentication** — `/runs/{id}/cancel`, `/rerun`, the task `clear`/`approve`
/// endpoints and dead-letter redrive are all reachable by anyone who can reach the
/// socket, and always have been. `API_ADDR` is expected to be a loopback or private
/// address with authentication in front of it if it is published.
///
/// What the console changes is *discoverability*: the same capability is now one
/// browser tab away rather than behind reading the OpenAPI. That is a good reason to
/// say so out loud on a non-loopback bind, and a reason for `DAGRON_CONSOLE=off` —
/// which hides the UI, and hides nothing else. An operator who needs the API closed
/// needs a network boundary, not that switch.
pub async fn serve(addr: SocketAddr, state: ApiState) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    if !addr.ip().is_loopback() {
        tracing::warn!(
            %addr,
            "management API bound to a non-loopback address and has NO authentication: anyone who can reach it can cancel, rerun and approve work. Put a network boundary in front of it, or bind 127.0.0.1."
        );
    }
    info!(%addr, console = console_enabled(), "management API listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

/// Whether to serve the console. Opt *out* (`DAGRON_CONSOLE=off|false|0`), because
/// the API it drives is already served here — hiding the UI does not reduce what the
/// socket can do, so defaulting it off would trade a real convenience for the
/// appearance of safety.
fn console_enabled() -> bool {
    !matches!(
        std::env::var("DAGRON_CONSOLE").unwrap_or_default().to_ascii_lowercase().as_str(),
        "off" | "false" | "0" | "no"
    )
}

/// Build the router. Split out so tests can exercise handlers without a socket.
pub fn router(state: ApiState) -> Router {
    let r = Router::new();
    // The console owns `/` and `/console`; every API path below is unchanged, so an
    // existing client sees no difference whether it is mounted or not.
    let r = if console_enabled() {
        r.route("/", get(console)).route("/console", get(console))
    } else {
        r
    };
    r.route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/config", get(config_effective))
        .route("/metrics", get(metrics))
        .route("/openapi.yaml", get(openapi_yaml))
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(docs))
        .route("/docs/swagger-ui.css", get(swagger_ui_css))
        .route("/docs/swagger-ui-bundle.js", get(swagger_ui_js))
        .route("/runs", get(list_runs).post(submit_run))
        .route("/runs/{id}", get(get_run))
        .route("/runs/{id}/wait", get(wait_run))
        // Log views: the whole run's output as one filtered stream, and one
        // task's output (live-tailable) under the same filter grammar.
        .route("/runs/{id}/logs", get(run_logs))
        .route("/runs/{id}/tasks/{task_id}/logs", get(task_logs))
        .route("/runs/{id}/cancel", post(cancel_run))
        .route("/runs/{id}/rerun", post(rerun_run))
        .route("/runs/{id}/tasks/{task_id}/clear", post(clear_task))
        .route("/runs/{id}/tasks/{task_id}/approve", post(approve_task))
        .route("/runs/{id}/tasks/{task_id}/reject", post(reject_task))
        .route("/runs/{id}/tasks/{task_id}/checkpoint", post(checkpoint_task))
        .route("/dead-letters", get(list_dead_letters))
        .route("/dead-letters/{id}/redrive", post(redrive_dead_letter))
        .route("/dead-letters/{id}", axum::routing::delete(delete_dead_letter))
        .route("/datasets", get(list_datasets))
        .route("/datasets/events", get(list_dataset_events).post(post_dataset_event))
        .with_state(state)
}

// ── Handlers ────────────────────────────────────────────────────────────────

async fn healthz() -> &'static str {
    "ok"
}

/// Effective configuration: every registered knob's value (secrets redacted),
/// its source (env / file / profile / default), and the fleet-drift
/// fingerprint — the same view `dagron config` prints, served where a fleet
/// dashboard can diff replicas (LOW_LATENCY S-2/S-4). Cluster-private like the
/// rest of this API.
async fn config_effective() -> Json<serde_json::Value> {
    let knobs: Vec<serde_json::Value> = crate::config::effective()
        .into_iter()
        .map(|(name, value, source)| {
            serde_json::json!({ "name": name, "value": value, "source": source.as_str() })
        })
        .collect();
    let (config_file, profile) = crate::config::layer_info();
    Json(serde_json::json!({
        "fingerprint": crate::config::fingerprint(),
        "config_file": config_file,
        "profile": profile,
        "knobs": knobs,
    }))
}

/// Readiness probe — unlike `/healthz` (pure liveness), this answers 200 only
/// when the datastore actually responds, so an orchestrator stops routing to a
/// process whose pool or database is gone instead of trusting a static "ok"
/// (LOW_LATENCY R-1). The probe carries its own budget
/// (`DAGRON_READY_TIMEOUT_MS`, floor 50 ms) so a wedged pool acquire makes the
/// probe answer 503 inside the kubelet's `timeoutSeconds` instead of hanging
/// past it — an unanswered probe and a truthful 503 are the same verdict, but
/// only one of them shows up in the logs with a reason. Kept unauthenticated
/// like `/healthz`: this API is cluster-private by contract.
async fn readyz(State(st): State<ApiState>) -> Response {
    static BUDGET: OnceLock<std::time::Duration> = OnceLock::new();
    let budget = *BUDGET.get_or_init(|| {
        let ms: u64 = std::env::var("DAGRON_READY_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(500);
        std::time::Duration::from_millis(ms.max(50))
    });
    match tokio::time::timeout(budget, crate::db::ping(&st.pool)).await {
        Ok(Ok(())) => (StatusCode::OK, "ready").into_response(),
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "readiness probe failed");
            (StatusCode::SERVICE_UNAVAILABLE, "datastore unreachable").into_response()
        }
        Err(_) => {
            tracing::warn!(
                budget_ms = budget.as_millis() as u64,
                "readiness probe timed out"
            );
            (StatusCode::SERVICE_UNAVAILABLE, "datastore probe timed out").into_response()
        }
    }
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

/// The operator console (`CONSOLE_HTML`), served at `/` and `/console`.
async fn console() -> Response {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], CONSOLE_HTML).into_response()
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

/// Map an admission refusal from `db::create_run` to the status + body it
/// answers with, or `None` for any other error (which keeps the default 500).
///
/// Two capacity conditions, two codes, one `Retry-After: 1`: `429` for the
/// per-workflow concurrency cap (#21) and `507 Insufficient Storage` for the
/// datastore's free-disk floor (`DAGRON_MIN_FREE_BYTES`). A full flash device
/// is a storage condition, not a rate one — a client that reads 429 as "slow
/// down" would keep offering work to a unit whose disk is what needs relief,
/// while 507 tells it (and any fleet plane behind it) exactly what to wait
/// for. Pure, so the mapping is testable without a full disk.
fn admission_refusal(e: &anyhow::Error) -> Option<(StatusCode, serde_json::Value)> {
    if let Some(m) = e.downcast_ref::<dagron_core::models::MaxActiveRunsReached>() {
        return Some((
            StatusCode::TOO_MANY_REQUESTS,
            json!({
                "error": "max_active_runs reached",
                "workflow": m.name,
                "active": m.active,
                "max_active_runs": m.max,
            }),
        ));
    }
    if let Some(d) = e.downcast_ref::<dagron_core::models::DatastoreLowOnDisk>() {
        return Some((
            StatusCode::INSUFFICIENT_STORAGE,
            json!({
                "error": "datastore low on disk",
                "free_bytes": d.free,
                "min_free_bytes": d.floor,
            }),
        ));
    }
    None
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
    // — without it, `POST /runs` accepts faster than the engine can drain (a
    // load-test finding). A 429 + Retry-After tells clients to back off.
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

    // The same valve on tasks. `MAX_INFLIGHT_TASKS` reached the IngestActor and
    // stopped there, so this path — the engine's own ops API, and the one the
    // load-test fleet actually submits through — was capped on runs only. A run
    // is a poor unit for admission: one submission can expand to 100k tasks and
    // still count as 1.
    //
    // The incoming run's own rows are added before comparing, rather than
    // checking `active >= cap` as the ingest valve does. Otherwise a fleet
    // sitting one task under the cap admits an arbitrarily large run and lands
    // far above it, which is the case the cap exists for.
    if st.max_inflight_tasks > 0 {
        let incoming = dag.task_row_count();
        let active_tasks = db::count_active_tasks(&st.pool).await?;
        if active_tasks + incoming > st.max_inflight_tasks {
            info!(
                active_tasks, incoming, cap = st.max_inflight_tasks,
                "run rejected — would exceed the inflight task cap"
            );
            return Ok((
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "1")],
                Json(json!({
                    "error": "too many in-flight tasks",
                    "active_tasks": active_tasks,
                    "run_tasks": incoming,
                    "max_inflight_tasks": st.max_inflight_tasks,
                })),
            )
                .into_response());
        }
    }

    let run_id = match db::create_run(&st.pool, &dag, &body).await {
        Ok(id) => id,
        Err(e) => {
            // Capacity conditions — the per-workflow cap (#21) and the
            // free-disk floor — are answered with a retryable status +
            // Retry-After (like the inflight valve above), never 500. Any
            // other error keeps the default 500 mapping.
            if let Some((status, refusal)) = admission_refusal(&e) {
                if status == StatusCode::INSUFFICIENT_STORAGE {
                    st.metrics.inc_admission_refused_disk();
                }
                info!(status = status.as_u16(), reason = %e, "run rejected — admission refused");
                return Ok((status, [(header::RETRY_AFTER, "1")], Json(refusal)).into_response());
            }
            return Err(e.into());
        }
    };
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

/// Is this task status terminal (no further output will arrive)?
fn task_is_terminal(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "skipped" | "cancelled")
}

/// Parse the shared log filter out of a raw query string, mapping a parse
/// failure to a 400 that names the reason. A bad regex is the caller's typo, and
/// silently ignoring it would hand them an unfiltered wall of text they'd read
/// as "nothing was filtered out".
///
/// Uses the same parser as `dagron-api` ([`dagron_logging::logfilter`]) so a
/// filter means one thing across both HTTP surfaces.
type ParsedLogQuery = (LogFilter, bool, Vec<(String, String)>);

fn parse_log_filter(raw: Option<&str>) -> Result<ParsedLogQuery, ApiError> {
    let raw = raw.unwrap_or("");
    let pairs = logfilter::parse_pairs(raw);
    let filter = LogFilter::from_pairs(&pairs)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    let filtering = logfilter::pairs_have_filter(&pairs);
    // Hand the split pairs back so the caller reads its own params out of the
    // same pass — parsing the query a second time is both wasted work and a
    // chance for the two passes to disagree.
    Ok((filter, filtering, pairs))
}

/// Read `offset=` (the tail cursor) out of an already-split query.
fn log_offset(pairs: &[(String, String)]) -> Result<Option<usize>, ApiError> {
    // Last wins on a repeated `offset=`, matching dagron-api's `Scope::from_pairs`
    // — one grammar must not give two answers depending on which surface you ask.
    match pairs.iter().rev().find(|(k, _)| k == "offset") {
        None => Ok(None),
        Some((_, v)) => v
            .trim()
            .parse::<usize>()
            .map(Some)
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, format!("offset must be a number, got {v:?}"))),
    }
}

/// Render a [`FilterResult`]'s lines as JSON.
fn filter_lines_json(res: &dagron_logging::logfilter::FilterResult) -> serde_json::Value {
    json!(res
        .lines
        .iter()
        .map(|l| json!({
            "n": l.n, "level": l.level.as_str(), "ts": l.ts,
            "text": l.text, "matched": l.matched,
        }))
        .collect::<Vec<_>>())
}

/// `GET /runs/{id}/tasks/{task_id}/logs[?offset=N][&<filter>]` — one task's
/// output for live tailing. With `?offset=` returns only the output past that
/// char offset plus a `next_offset` to resume from and `eof` (the task is
/// terminal); poll until `eof`. 404 if the run or task is unknown.
///
/// The filter grammar (`q`/`exclude`/`regex`/`level`/`case`/`context`/`limit`/
/// `tail`) applies *within* the returned slice, while `next_offset` keeps
/// counting the raw text — so tailing and filtering compose.
async fn task_logs(
    State(st): State<ApiState>,
    Path((id, task_id)): Path<(String, String)>,
    RawQuery(raw): RawQuery,
) -> Result<Json<serde_json::Value>, ApiError> {
    let (filter, filtering, pairs) = parse_log_filter(raw.as_deref())?;
    let offset = log_offset(&pairs)?;

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
    let eof = task_is_terminal(&task.status.to_string());
    // Char-boundary slice — never splits a multibyte scalar.
    let slice = match offset {
        Some(off) if off < total => full.chars().skip(off).collect::<String>(),
        Some(_) => String::new(),
        None => full,
    };
    let mut body = json!({
        "task_id": task.id,
        "name": task.name,
        "status": task.status.to_string(),
        "attempt": task.attempt,
        "offset": offset.unwrap_or(0).min(total),
        "next_offset": total,
        "eof": eof,
        "filtered": filtering,
        // A line with no stamp of its own was printed somewhere in this window.
        "started_at": task.scheduled_at,
        "finished_at": task.finished_at,
    });
    let obj = body.as_object_mut().expect("json! built an object");
    if filtering {
        let res = filter.apply(&slice);
        obj.insert("output".into(), json!(res.to_text()));
        obj.insert("lines".into(), filter_lines_json(&res));
        obj.insert("total".into(), json!(res.total));
        obj.insert("matched".into(), json!(res.matched));
        obj.insert("truncated".into(), json!(res.truncated));
    } else {
        // No filter asked for: the slice goes back untouched, byte for byte.
        let lines = slice.lines().count();
        obj.insert("output".into(), json!(slice));
        obj.insert("total".into(), json!(lines));
        obj.insert("matched".into(), json!(lines));
        obj.insert("truncated".into(), json!(false));
    }
    Ok(Json(body))
}

/// `GET /runs/{id}/logs[?<filter>][&task=&status=]` — the **whole run's** output
/// as one attributed stream, filtered server-side.
///
/// The view for "this run failed and I don't know which task did it": one call
/// instead of one per task. `task=`/`status=` choose which task output is read;
/// the filter then chooses which of those lines survive.
async fn run_logs(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Result<Json<serde_json::Value>, ApiError> {
    let raw = raw.unwrap_or_default();
    let (filter, filtering, pairs) = parse_log_filter(Some(&raw))?;
    let csv = |key: &str, alt: &str| -> Vec<String> {
        pairs
            .iter()
            .filter(|(k, _)| k == key || k == alt)
            .flat_map(|(_, v)| v.split(','))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };
    let want_tasks = csv("task", "tasks");
    let want_statuses = csv("status", "statuses");

    if db::get_run(&st.pool, &id).await?.is_none() {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    }
    let limit = filter.effective_limit();
    let mut task_rollup = Vec::new();
    let mut lines: Vec<serde_json::Value> = Vec::new();
    let (mut total, mut matched, mut truncated, mut eof) = (0usize, 0usize, false, true);

    for task in db::list_tasks(&st.pool, &id).await? {
        let status = task.status.to_string();
        let selected = (want_tasks.is_empty()
            || want_tasks.iter().any(|t| *t == task.id || t.eq_ignore_ascii_case(&task.name)))
            && (want_statuses.is_empty()
                || want_statuses.iter().any(|s| s.eq_ignore_ascii_case(&status)));
        if !selected {
            task_rollup.push(json!({
                "id": task.id, "name": task.name, "status": status,
                "attempt": task.attempt, "selected": false, "total": 0, "matched": 0,
                // Reported even when unselected: the picker shows when each
                // task ran, which is half of choosing one.
                "started_at": task.scheduled_at, "finished_at": task.finished_at,
            }));
            continue;
        }
        if !task_is_terminal(&status) {
            eof = false;
        }
        let res = filter.apply(task.output.as_deref().unwrap_or_default());
        total += res.total;
        matched += res.matched;
        truncated |= res.truncated;
        task_rollup.push(json!({
            "id": task.id, "name": task.name, "status": status,
            "attempt": task.attempt, "selected": true,
            "total": res.total, "matched": res.matched,
            // Most output carries no time of its own, so this window bounds
            // every line the task printed — as tight as the task was short.
            "started_at": task.scheduled_at, "finished_at": task.finished_at,
        }));
        for l in &res.lines {
            lines.push(json!({
                "task_id": task.id, "task": task.name, "attempt": task.attempt,
                "n": l.n, "level": l.level.as_str(), "ts": l.ts,
                "text": l.text, "matched": l.matched,
            }));
        }
    }

    // Merge-level cap. `tail` keeps the end of the run — where a failure is.
    if lines.len() > limit {
        truncated = true;
        if filter.is_tail() {
            lines.drain(..lines.len() - limit);
        } else {
            lines.truncate(limit);
        }
    }

    Ok(Json(json!({
        "run_id": id,
        "tasks": task_rollup,
        "lines": lines,
        "total": total,
        "matched": matched,
        "truncated": truncated,
        "eof": eof,
        "filtered": filtering,
        "limit": limit,
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

/// Body of `POST /runs/{id}/tasks/{task_id}/checkpoint`.
#[derive(serde::Deserialize)]
struct CheckpointBody {
    /// Where the task just committed its checkpoint (any URI/path the *next*
    /// attempt can read — a `DAGRON_CHECKPOINT_DIR` file, an s3:// object, …).
    uri: String,
    /// Optional progress marker (e.g. `epoch=7`, an offset) surfaced back as
    /// `DAGRON_RESUME_MARKER`.
    #[serde(default)]
    marker: Option<String>,
}

/// `POST /runs/{id}/tasks/{task_id}/checkpoint` — checkpoint-aware resume: a
/// **running** task reports the checkpoint it just committed (typically using
/// its injected `DAGRON_RUN_ID` / `DAGRON_TASK_ID` env). The pointer survives
/// retries; the next attempt is dispatched with `DAGRON_RESUME_FROM[_MARKER]`
/// so it resumes instead of restarting from zero. 404 unknown run/task, 409 if
/// the task is not running (a parked attempt cannot overwrite a newer one's
/// progress), 400 on an empty uri.
async fn checkpoint_task(
    State(st): State<ApiState>,
    Path((id, task_id)): Path<(String, String)>,
    Json(body): Json<CheckpointBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if body.uri.trim().is_empty() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "checkpoint uri must not be empty".into()));
    }
    if db::get_run(&st.pool, &id).await?.is_none() {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    }
    if db::record_task_checkpoint(&st.pool, &id, &task_id, body.uri.trim(), body.marker.as_deref())
        .await?
    {
        return Ok(Json(json!({
            "run_id": id,
            "task_id": task_id,
            "checkpoint_uri": body.uri.trim(),
            "marker": body.marker,
        })));
    }
    if db::task_exists(&st.pool, &id, &task_id).await? {
        Err(ApiError(
            StatusCode::CONFLICT,
            format!("task '{task_id}' is not running — only a live attempt may report a checkpoint"),
        ))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, format!("task '{task_id}' not found in run '{id}'")))
    }
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

/// The dataset registry (data-aware scheduling): every dataset ever produced,
/// with its latest update. Feeds "what data exists and how fresh is it".
async fn list_datasets(
    State(st): State<ApiState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let rows = db::list_datasets(&st.pool, limit).await?;
    let datasets: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(uri, updated_at, last_run_id, last_task, updates)| {
            json!({
                "uri": uri, "updated_at": updated_at, "last_run_id": last_run_id,
                "last_task": last_task, "updates": updates,
            })
        })
        .collect();
    Ok(Json(json!({ "datasets": datasets })))
}

/// Query for the dataset-event lineage read (`?uri=` narrows to one dataset).
#[derive(serde::Deserialize)]
struct DatasetEventsQuery {
    uri: Option<String>,
    limit: Option<i64>,
}

/// The dataset lineage ledger, newest first: which run/task updated which
/// dataset when — the cross-workflow update trail sensors and triggers key off.
async fn list_dataset_events(
    State(st): State<ApiState>,
    Query(q): Query<DatasetEventsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let rows = db::list_dataset_events(&st.pool, q.uri.as_deref(), limit).await?;
    let events: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(id, uri, workflow, run_id, task_name, source, at)| {
            json!({
                "id": id, "uri": uri, "workflow": workflow, "run_id": run_id,
                "task_name": task_name, "source": source, "at": at,
            })
        })
        .collect();
    Ok(Json(json!({ "events": events })))
}

/// Body for the external dataset-event POST (not in this build).
#[derive(serde::Deserialize)]
struct DatasetEventBody {
    uri: String,
}

/// Record an **external** dataset event — a producer outside dagron (CDC, an
/// object-store notification, another orchestrator) declaring "this dataset
/// updated", waking dataset sensors and firing `on_datasets:` triggers.
/// Not in this build, which answers with a signpost
/// (the SOURCE-connector funnel pattern) — its datasets update via `produces:`.
async fn post_dataset_event(
    State(st): State<ApiState>,
    Json(body): Json<DatasetEventBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    dagron_core::dag::validate_dataset_uri(&body.uri)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    if !cfg!(feature = "enterprise") {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "external dataset events are not in this build — \
             https://github.com/lucheeseng827/dagron#what-this-build-does-not-do. This build \
             records dataset updates from `produces:` tasks; to signal external data, \
             run a small task that produces the dataset (see docs/DATASETS.md)."
                .to_string(),
        ));
    }
    db::record_external_dataset_event(&st.pool, &body.uri).await?;
    st.metrics.inc_dataset_updates();
    Ok(Json(json!({ "recorded": body.uri })))
}

/// Re-attempt a dead letter as a fresh run. On success the dead letter is
/// removed; a still-invalid payload returns `400` and the row is kept.
async fn redrive_dead_letter(
    State(st): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
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
            // The free-disk floor is a capacity condition, and the row is
            // already claimed (deleted): re-park the payload as a fresh
            // dead-letter row — same payload, source and failure count, the
            // floor as its error — rather than lose it, and answer 507 so the
            // caller retries once the disk has headroom. If even the re-park
            // fails, fall through to the loud path below.
            if let Some(low) = e.downcast_ref::<dagron_core::models::DatastoreLowOnDisk>() {
                st.metrics.inc_admission_refused_disk();
                match db::record_dead_letter(&st.pool, &dl.payload, &low.to_string(), &dl.source, dl.failures)
                    .await
                {
                    Ok(new_id) => {
                        tracing::warn!(
                            dead_letter_id = %id, reparked_as = %new_id, free = low.free, floor = low.floor,
                            "redrive refused — datastore low on disk; dead letter re-parked"
                        );
                        return Ok((
                            StatusCode::INSUFFICIENT_STORAGE,
                            [(header::RETRY_AFTER, "1")],
                            Json(json!({
                                "error": "datastore low on disk — dead letter re-parked",
                                "dead_letter_id": new_id,
                                "free_bytes": low.free,
                                "min_free_bytes": low.floor,
                            })),
                        )
                            .into_response());
                    }
                    Err(park_err) => {
                        error!(dead_letter_id = %id, error = ?park_err, "could not re-park the dead letter after a disk-floor refusal");
                    }
                }
            } else if let Some(cap) = e.downcast_ref::<dagron_core::models::MaxActiveRunsReached>() {
                // The run cap is the other capacity refusal, and it lands here
                // with the row already claimed. Losing the payload to a
                // condition that clears on its own would be the worse outcome,
                // so it re-parks exactly like the disk floor does — 503 rather
                // than 507, because it is concurrency and not storage.
                match db::record_dead_letter(&st.pool, &dl.payload, &cap.to_string(), &dl.source, dl.failures)
                    .await
                {
                    Ok(new_id) => {
                        tracing::warn!(
                            dead_letter_id = %id, reparked_as = %new_id,
                            workflow = %cap.name, max = cap.max, active = cap.active,
                            "redrive refused — workflow at its active-run cap; dead letter re-parked"
                        );
                        return Ok((
                            StatusCode::SERVICE_UNAVAILABLE,
                            [(header::RETRY_AFTER, "1")],
                            Json(json!({
                                "error": "workflow at its active-run cap — dead letter re-parked",
                                "dead_letter_id": new_id,
                                "workflow": cap.name,
                                "max_active_runs": cap.max,
                                "active_runs": cap.active,
                            })),
                        )
                            .into_response());
                    }
                    Err(park_err) => {
                        error!(dead_letter_id = %id, error = ?park_err, "could not re-park the dead letter after a run-cap refusal");
                    }
                }
            }
            // The row is already claimed (deleted); surface the payload so the
            // operator can recover it rather than losing it silently.
            error!(dead_letter_id = %id, payload = %dl.payload, error = ?e, "redrive create_run failed after claim");
            return Err(e.into());
        }
    };
    st.metrics.inc_runs_created();
    info!(dead_letter_id = %id, %run_id, "dead letter redriven into a run");
    Ok(Json(json!({ "run_id": run_id, "redriven_from": id })).into_response())
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
            "/readyz",
            "/config",
            "/metrics",
            "/openapi.yaml",
            "/openapi.json",
            "/docs",
            "/runs",
            "/runs/{id}",
            "/runs/{id}/wait",
            "/runs/{id}/logs",
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
        temp_state_with(max_inflight_runs, 0).await
    }

    /// As `temp_state`, with the task cap set too.
    async fn temp_state_with(
        max_inflight_runs: i64,
        max_inflight_tasks: i64,
    ) -> (ApiState, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("module54_api_{}.db", uuid::Uuid::new_v4()));
        let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
        let state = ApiState {
            pool,
            metrics: Arc::new(Metrics::new()),
            max_inflight_runs,
            max_inflight_tasks,
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

    /// The task cap bounds the dimension that costs something. `MAX_INFLIGHT_TASKS`
    /// used to reach only the IngestActor, so this path — the engine's own ops
    /// API — was capped on runs alone and a single wide submission sailed past.
    #[tokio::test]
    async fn submit_run_sheds_load_at_inflight_task_cap() {
        // Runs uncapped, tasks capped at 3.
        let (state, path) = temp_state_with(0, 3).await;
        let three = "name: wide\ntasks:\n  - name: a\n    command: [\"true\"]\n  \
                     - name: b\n    command: [\"true\"]\n  - name: c\n    command: [\"true\"]\n";

        // 0 active + 3 incoming == cap → admitted.
        let first = submit_run(State(state.clone()), no_wait(), three.to_string())
            .await
            .unwrap()
            .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);

        // 3 active + 1 incoming > cap → rejected, even though the RUN cap is off
        // and the datastore holds only one run.
        let second = submit_run(State(state.clone()), no_wait(), ONE_TASK_DAG.to_string())
            .await
            .unwrap()
            .into_response();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(second.headers().get(header::RETRY_AFTER).unwrap(), "1");

        state.pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A submission wider than the whole cap is refused on its own, rather than
    /// admitted because the datastore happened to be empty when it arrived.
    #[tokio::test]
    async fn a_single_oversized_run_cannot_slip_past_the_task_cap() {
        let (state, path) = temp_state_with(0, 2).await;
        let three = "name: wide\ntasks:\n  - name: a\n    command: [\"true\"]\n  \
                     - name: b\n    command: [\"true\"]\n  - name: c\n    command: [\"true\"]\n";
        let r = submit_run(State(state.clone()), no_wait(), three.to_string())
            .await
            .unwrap()
            .into_response();
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS, "0 active + 3 > cap of 2");
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
            RawQuery(None),
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
            RawQuery(None),
        )
        .await
        .unwrap_err();
        assert_eq!(e.0, StatusCode::NOT_FOUND);

        // No offset → full output; running task → eof false.
        let full = task_logs(
            State(state.clone()),
            Path((run_id.clone(), a.id.clone())),
            RawQuery(None),
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
            RawQuery(Some("offset=6".into())),
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
            RawQuery(None),
        )
        .await
        .unwrap();
        assert_eq!(done.0["eof"], true);

        state.pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The **workflow** log view: `GET /runs/{id}/logs` merges every task's
    /// output into one attributed stream, and the shared filter grammar narrows
    /// it. This is the end-to-end check that the filter, the per-task rollup and
    /// the merge-level cap agree with each other over real datastore rows.
    #[tokio::test]
    async fn run_logs_merges_and_filters_every_task() {
        let (state, path) = temp_state(0).await;
        let yaml = "name: t\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = db::create_run(&state.pool, &dag, yaml).await.unwrap();

        // Unknown run → 404 (not an empty stream, which would read as "quiet run").
        let e = run_logs(State(state.clone()), Path("nope".into()), RawQuery(None))
            .await
            .unwrap_err();
        assert_eq!(e.0, StatusCode::NOT_FOUND);

        // Give both tasks output: one clean, one with an error line.
        db::advance_ready_tasks(&state.pool).await.unwrap();
        let claimed = db::claim_ready(&state.pool, "w", 10).await.unwrap();
        assert_eq!(claimed.len(), 2, "both tasks are independent and ready");
        for t in &claimed {
            let fence = t.version + 1;
            let body = if t.name == "a" {
                "starting a\nrows=10\n"
            } else {
                "starting b\nERROR upstream timeout\n"
            };
            db::append_task_output(&state.pool, &t.id, fence, body, true).await.unwrap();
        }

        // Unfiltered: every line from every task, attributed, and `filtered` false.
        let all = run_logs(State(state.clone()), Path(run_id.clone()), RawQuery(None))
            .await
            .unwrap();
        assert_eq!(all.0["total"], 4);
        assert_eq!(all.0["lines"].as_array().unwrap().len(), 4);
        assert_eq!(all.0["filtered"], false);
        assert_eq!(all.0["eof"], false, "both tasks are still running");
        assert_eq!(all.0["tasks"].as_array().unwrap().len(), 2);
        let names: Vec<&str> =
            all.0["lines"].as_array().unwrap().iter().map(|l| l["task"].as_str().unwrap()).collect();
        assert!(names.contains(&"a") && names.contains(&"b"), "both tasks contribute lines");

        // Filtered by inferred level: only the error line survives, and `total`
        // still reports every raw line so the view can say what it hid.
        let errs = run_logs(
            State(state.clone()),
            Path(run_id.clone()),
            RawQuery(Some("level=error".into())),
        )
        .await
        .unwrap();
        assert_eq!(errs.0["filtered"], true);
        assert_eq!(errs.0["matched"], 1);
        assert_eq!(errs.0["total"], 4);
        let lines = errs.0["lines"].as_array().unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["task"], "b");
        assert_eq!(lines[0]["level"], "error");

        // Scoping to a task excludes the other entirely — its output isn't even
        // read, which is why its rollup reports zero rather than its real count.
        let only_a =
            run_logs(State(state.clone()), Path(run_id.clone()), RawQuery(Some("task=a".into())))
                .await
                .unwrap();
        assert_eq!(only_a.0["total"], 2);
        let rollup = only_a.0["tasks"].as_array().unwrap();
        let b = rollup.iter().find(|t| t["name"] == "b").unwrap();
        assert_eq!(b["selected"], false);
        assert_eq!(b["total"], 0);

        // A bad regex is the caller's typo → 400 naming the reason, never a
        // silently-unfiltered response.
        let e = run_logs(State(state.clone()), Path(run_id.clone()), RawQuery(Some("regex=%5B".into())))
            .await
            .unwrap_err();
        assert_eq!(e.0, StatusCode::BAD_REQUEST);

        state.pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Tailing and filtering compose: the filter applies *within* the `?offset=`
    /// slice while `next_offset` keeps counting the raw text, so a filtered tail
    /// neither loses nor repeats output.
    #[tokio::test]
    async fn task_logs_filter_applies_within_the_tailed_slice() {
        let (state, path) = temp_state(0).await;
        let yaml = "name: t\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = db::create_run(&state.pool, &dag, yaml).await.unwrap();
        db::advance_ready_tasks(&state.pool).await.unwrap();
        let claimed = db::claim_ready(&state.pool, "w", 10).await.unwrap();
        let a = &claimed[0];
        let fence = a.version + 1;
        db::append_task_output(&state.pool, &a.id, fence, "line one\nERROR first\n", true)
            .await
            .unwrap();

        let first = task_logs(
            State(state.clone()),
            Path((run_id.clone(), a.id.clone())),
            RawQuery(Some("level=error".into())),
        )
        .await
        .unwrap();
        assert_eq!(first.0["output"], "ERROR first\n");
        assert_eq!(first.0["matched"], 1);
        assert_eq!(first.0["total"], 2, "total counts the raw slice, not the matches");
        // The cursor advances over the RAW text — all 21 chars, not the 12 kept.
        let cursor = first.0["next_offset"].as_u64().unwrap();
        assert_eq!(cursor, 21);

        // More output arrives; tailing from the cursor with the filter still set
        // returns only the new matching line.
        db::append_task_output(&state.pool, &a.id, fence, "line three\nERROR second\n", false)
            .await
            .unwrap();
        let tail = task_logs(
            State(state.clone()),
            Path((run_id.clone(), a.id.clone())),
            RawQuery(Some(format!("offset={cursor}&level=error"))),
        )
        .await
        .unwrap();
        assert_eq!(tail.0["output"], "ERROR second\n");
        assert_eq!(tail.0["lines"].as_array().unwrap().len(), 1);
        // Line numbers are positions in the slice, and the cursor keeps climbing.
        assert!(tail.0["next_offset"].as_u64().unwrap() > cursor);

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
