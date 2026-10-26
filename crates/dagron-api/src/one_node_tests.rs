//! A one-node dagron-api, through the router it serves,
//! over a SQLite file the engine's own migrations made: what a person does on
//! a laptop or an edge site, request by request. Signing in, a token, a
//! workflow and its versions, runs submitted (once under an
//! `Idempotency-Key`), listed, read, their logs, cancelled and run again; the
//! routes the Postgres build alone serves answer 501, and an unknown one 404.
//! No engine runs here: `scripts/one-node-e2e.sh` runs the two together.

use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use serde_json::{json, Value};
use tower::ServiceExt;

use crate::state::AppState;

const EMAIL: &str = "admin@example.com";
const PASSWORD: &str = "one-node-password";

/// A fresh SQLite file, migrated as the engine migrates it, and the state a
/// one-node dagron-api serves from, with one admin in it.
pub(crate) async fn one_node() -> (Router, AppState) {
    // What a handler logs as it fails, on the test's output (`RUST_LOG`).
    static LOGS: std::sync::Once = std::sync::Once::new();
    LOGS.call_once(|| dagron_logging::init("one-node-test"));
    let dir = std::env::temp_dir().join(format!("dagron-api-one-node-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("workflow.db");
    let pool = crate::db::connect(&format!("sqlite://{}", path.display())).await.unwrap();
    let hash = crate::pwhash::hash(PASSWORD.to_string()).await.unwrap();
    sqlx::query("INSERT INTO users (id, email, name, pw_hash, groups, created_at) VALUES ($1, $2, $3, $4, $5, $6)")
        .bind("u-admin")
        .bind(EMAIL)
        .bind("Admin")
        .bind(hash)
        .bind(r#"["admin"]"#)
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();
    let state = AppState {
        token_cache: Default::default(),
        read_pool: pool.clone(),
        write_pool: pool.clone(),
        tx: tokio::sync::broadcast::channel(16).0,
        jwt_secret: "one-node-test-secret-of-32-bytes!".to_string(),
        cookie_secure: false,
        identity: Arc::new(crate::identity::LocalIdentityProvider::new(pool)),
        artifact_store: None,
        log_store: None,
        rotation_lock: Arc::new(tokio::sync::Mutex::new(())),
        login_limiter: Arc::new(crate::ratelimit::RateLimiter::from_env()),
        listener_ready: Arc::new(AtomicBool::new(true)),
        oidc: None,
        relay: Arc::new(crate::relay::Relay::new(Default::default())),
    };
    (crate::build_app(state.clone(), "").unwrap(), state)
}

/// One request: `(status, the Set-Cookie session if any, the JSON body or Null)`.
pub(crate) async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    session: Option<&str>,
    body: Option<Value>,
    extra: &[(&str, &str)],
) -> (StatusCode, Option<String>, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(s) = session {
        req = req.header(header::COOKIE, format!("{}={s}", crate::auth::SESSION_COOKIE));
    }
    for (k, v) in extra {
        req = req.header(*k, *v);
    }
    let body = match body {
        Some(v) => {
            req = req.header(header::CONTENT_TYPE, "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let mut req = req.body(body).unwrap();
    // As `into_make_service_with_connect_info` gives each request its peer.
    req.extensions_mut()
        .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 40000))));
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let cookie = resp
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .find_map(|c| {
            c.strip_prefix(&format!("{}=", crate::auth::SESSION_COOKIE))
                .map(|rest| rest.split(';').next().unwrap_or("").to_string())
        });
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    // A handler's plain-text error is kept as text, so a failure says why.
    let json = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, cookie, json)
}

pub(crate) async fn signed_in(app: &Router) -> String {
    let (status, cookie, _) = call(
        app,
        "POST",
        "/api/login",
        None,
        Some(json!({ "email": EMAIL, "password": PASSWORD })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sign-in");
    cookie.expect("a session cookie")
}

const SPEC: &str = "name: one-node\ntasks:\n  - name: a\n    command: [\"sh\", \"-c\", \"echo a\"]\n  - name: b\n    command: [\"sh\", \"-c\", \"echo b\"]\n    depends_on: [a]\n";

#[tokio::test]
async fn a_person_works_a_one_node_install() {
    let (app, state) = one_node().await;
    assert_eq!(call(&app, "GET", "/readyz", None, None, &[]).await.0, StatusCode::OK);

    // Sign-in, who I am, and a token for scripts.
    let (status, _, _) = call(&app, "POST", "/api/login", None,
        Some(json!({ "email": EMAIL, "password": "wrong" })), &[]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let s = signed_in(&app).await;
    let (status, _, me) = call(&app, "GET", "/api/me", Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(me["email"], EMAIL);
    let (status, _, users) = call(&app, "GET", "/api/users", Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{users}");
    assert_eq!(users.as_array().map(Vec::len), Some(1));
    let (status, _, token) = call(&app, "POST", "/api/tokens", Some(&s),
        Some(json!({ "name": "ci" })), &[]).await;
    assert_eq!(status, StatusCode::CREATED, "{token}");
    let bearer = format!("Bearer {}", token["token"].as_str().unwrap());
    let (status, _, me) = call(&app, "GET", "/api/me", None, None, &[("authorization", &bearer)]).await;
    assert_eq!(status, StatusCode::OK, "the token signs in: {me}");
    let (status, _, tokens) = call(&app, "GET", "/api/tokens", Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{tokens}");

    // A workflow, changed once, its versions, paused and active again.
    let (status, _, wf) = call(&app, "POST", "/api/workflows", Some(&s),
        Some(json!({ "spec": SPEC, "description": "on one node" })), &[]).await;
    assert_eq!(status, StatusCode::CREATED, "{wf}");
    let id = wf["id"].as_str().unwrap().to_string();
    let (status, _, listed) = call(&app, "GET", "/api/workflows", Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    assert!(listed.to_string().contains(&id), "{listed}");
    let (status, _, got) = call(&app, "GET", &format!("/api/workflows/{id}"), Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(got["description"], "on one node");
    let changed = SPEC.replace("echo b", "echo b2");
    let (status, _, put) = call(&app, "PUT", &format!("/api/workflows/{id}"), Some(&s),
        Some(json!({ "spec": changed })), &[]).await;
    assert_eq!(status, StatusCode::OK, "{put}");
    let (status, _, versions) = call(&app, "GET", &format!("/api/workflows/{id}/versions"), Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{versions}");
    assert!(!versions.to_string().is_empty());
    for wanted in ["paused", "active"] {
        let (status, _, body) = call(&app, "POST", &format!("/api/workflows/{id}/state"), Some(&s),
            Some(json!({ "state": wanted })), &[]).await;
        assert_eq!(status, StatusCode::OK, "{wanted}: {body}");
    }

    // Runs: of the workflow, and submitted as YAML, twice under one key.
    let (status, _, run) = call(&app, "POST", &format!("/api/workflows/{id}/run"), Some(&s),
        Some(json!({})), &[]).await;
    assert_eq!(status, StatusCode::CREATED, "{run}");
    let run_id = run["run_id"].as_str().unwrap().to_string();
    let submit = json!({ "yaml": SPEC.replace("name: one-node", "name: submitted") });
    let (status, _, first) = call(&app, "POST", "/api/runs", Some(&s), Some(submit.clone()),
        &[("idempotency-key", "k-1")]).await;
    assert_eq!(status, StatusCode::CREATED, "{first}");
    let (status, _, again) = call(&app, "POST", "/api/runs", Some(&s), Some(submit),
        &[("idempotency-key", "k-1")]).await;
    assert_eq!(status, StatusCode::OK, "a repeat of a key answers its run: {again}");
    assert_eq!(first["run_id"], again["run_id"]);

    let (status, _, runs) = call(&app, "GET", "/api/runs?status=running&limit=10", Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{runs}");
    assert_eq!(runs.as_array().map(Vec::len), Some(2), "{runs}");
    let (status, _, runs) = call(&app, "GET", "/api/runs?name=submitted", Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{runs}");
    assert_eq!(runs.as_array().map(Vec::len), Some(1), "{runs}");
    let (status, _, detail) = call(&app, "GET", &format!("/api/runs/{run_id}"), Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    let tasks = detail["tasks"].as_array().expect("tasks");
    assert_eq!(tasks.len(), 2, "{detail}");
    let task_id = tasks[0]["id"].as_str().unwrap().to_string();
    let (status, _, _) = call(&app, "GET", &format!("/api/runs/{run_id}/logs"), Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = call(&app, "GET", &format!("/api/runs/{run_id}/tasks/{task_id}/logs"), Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = call(&app, "GET", &format!("/api/runs/{run_id}/spec"), Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, specs) = call(&app, "GET", &format!("/api/runs/specs?ids={run_id}"), Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{specs}");
    let (status, _, wf_runs) = call(&app, "GET", &format!("/api/workflows/{id}/runs"), Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{wf_runs}");
    let (status, _, refs) = call(&app, "GET", "/api/runs/refs?repo=github.com/acme/app", Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{refs}");

    // Cancelled, then run again from where it broke.
    let (status, _, cancelled) = call(&app, "POST", &format!("/api/runs/{run_id}/cancel"), Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{cancelled}");
    let (status, _, rerun) = call(&app, "POST", &format!("/api/runs/{run_id}/rerun"), Some(&s),
        Some(json!({})), &[]).await;
    assert_eq!(status, StatusCode::OK, "{rerun}");

    // Over, as one that succeeded: none to rerun from its failed tasks, and
    // all of it to run whole again.
    for sql in [
        "UPDATE task_runs SET status = 'succeeded' WHERE run_id = $1",
        "UPDATE workflow_runs SET status = 'succeeded' WHERE id = $1",
    ] {
        sqlx::query(sql)
            .bind(&run_id)
            .execute(&state.write_pool)
            .await
            .unwrap();
    }
    let (status, _, refused) = call(
        &app,
        "POST",
        &format!("/api/runs/{run_id}/rerun"),
        Some(&s),
        Some(json!({})),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    let (status, _, whole) = call(
        &app,
        "POST",
        &format!("/api/runs/{run_id}/rerun"),
        Some(&s),
        Some(json!({ "from": "all" })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{whole}");
    assert_eq!(whole["rerun"], 2, "{whole}");

    let (status, _, health) = call(&app, "GET", "/api/health", Some(&s), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{health}");

    // What the Postgres build alone serves says so; what no build serves is 404.
    for uri in ["/api/schedules", "/api/runners", &format!("/api/runs/{run_id}/graph"), "/runner/v1"] {
        let (status, _, body) = call(&app, "GET", uri, Some(&s), None, &[]).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{uri}: {body}");
        assert!(body["error"].as_str().unwrap_or("").contains("SQLite"), "{uri}: {body}");
    }
    assert_eq!(call(&app, "GET", "/api/no-such-thing", Some(&s), None, &[]).await.0, StatusCode::NOT_FOUND);

    let (status, _, _) = call(&app, "DELETE", &format!("/api/workflows/{id}"), Some(&s), None, &[]).await;
    assert!(status.is_success(), "delete: {status}");
}

/// The runs the file's triggers have named for the engine (migrations/079).
async fn wakes(state: &AppState) -> Vec<String> {
    sqlx::query_scalar("SELECT run_id FROM run_wakes ORDER BY run_id")
        .fetch_all(&state.read_pool)
        .await
        .unwrap()
}

/// What this process writes on the file is heard by the engine's reconcile
/// loop on the other side of it, whichever SQL wrote it: a
/// gate decided through the route, the run's last task, and a run
/// submitted with no task left in it, each wakes its run.
#[tokio::test]
async fn what_this_process_writes_wakes_the_engine_s_run() {
    let (app, state) = one_node().await;
    let s = signed_in(&app).await;

    let gate = json!({ "yaml": "name: gated\ntasks:\n  - { name: gate, type: approval }\n" });
    let (status, _, run) = call(&app, "POST", "/api/runs", Some(&s), Some(gate), &[]).await;
    assert_eq!(status, StatusCode::CREATED, "{run}");
    let gated = run["run_id"].as_str().unwrap().to_string();
    // The engine's part: the gate parks, and it took the run's wake.
    dagron_core::db::advance_ready_tasks(&state.write_pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM run_wakes")
        .execute(&state.write_pool)
        .await
        .unwrap();
    let gate_id: String = sqlx::query_scalar(
        "SELECT id FROM task_runs WHERE run_id = $1 AND status = 'awaiting_approval'",
    )
    .bind(&gated)
    .fetch_one(&state.read_pool)
    .await
    .unwrap();
    let (status, _, decided) = call(
        &app,
        "POST",
        &format!("/api/runs/{gated}/tasks/{gate_id}/approve"),
        Some(&s),
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{decided}");
    assert_eq!(
        wakes(&state).await,
        std::slice::from_ref(&gated),
        "the gate's decision"
    );

    let empty = json!({
        "yaml": "name: tags-only\nparameters: { event: push }\ntasks:\n  - { name: release, command: [\"true\"], when: \"{{ event }} == tag\" }\n"
    });
    let (status, _, run) = call(&app, "POST", "/api/runs", Some(&s), Some(empty), &[]).await;
    assert_eq!(status, StatusCode::CREATED, "{run}");
    let none = run["run_id"].as_str().unwrap().to_string();
    let tasks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE run_id = $1")
        .bind(&none)
        .fetch_one(&state.read_pool)
        .await
        .unwrap();
    assert_eq!(tasks, 0, "the when leaves its task out");
    let mut want = vec![gated, none];
    want.sort();
    assert_eq!(wakes(&state).await, want, "a run of no task");
}

/// The health count reads the partial index of the live runs on this
/// file (migrations/078), not every run.
#[tokio::test]
async fn health_count_plans_on_the_live_index() {
    let (_app, state) = one_node().await;
    let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(&format!(
        "EXPLAIN QUERY PLAN {}",
        dagron_core::db::LIVE_RUNS_COUNT
    ))
    .fetch_all(&state.read_pool)
    .await
    .unwrap();
    let lines: Vec<&str> = plan.iter().map(|row| row.3.as_str()).collect();
    assert!(
        lines.iter().any(
            |l| l.split_whitespace().any(|w| w == "idx_workflow_runs_live")
                || (cfg!(feature = "enterprise")
                    && l.split_whitespace()
                        .any(|w| w == "idx_workflow_runs_status_created_at"))
        ),
        "{lines:?}"
    );
    assert!(lines.iter().all(|l| !l.starts_with("SCAN")), "{lines:?}");
}
