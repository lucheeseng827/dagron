//! The barebone path through the shipped binary: a signed GitHub push in, the
//! repository's workflow run once through dagron-api, the event left `done`;
//! then a dispatch through the run API, run the same way; then a push of a
//! commit that holds a pipeline, which runs in the workflow's place.
//!
//! dagron-api is a stub that records each request and answers as dagron-api
//! does, replaying an `Idempotency-Key` it has seen, and knowing one caller.
//! GitHub's API is a stub too (CI_GITHUB_API_URL), so nothing leaves the
//! machine: it serves `.dagron/ci` at one commit and nothing at the others.

use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

const BIN: &str = env!("CARGO_BIN_EXE_dagron-ci");
const HEAD: &str = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";
const BASE: &str = "7a4235be307170bbd9b16543f6f4ce7d81682e4a";
/// The commit that holds a pipeline, `.dagron/ci/ci.yaml`.
const PIPELINED: &str = "c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00";
const PIPELINE: &str = "name: ci\non: { push: { branches: [main] } }\n\
                        tasks:\n  - { name: test, steps: [{ run: cargo test --locked }] }\n";

/// Kill the child on drop so a failed assertion never leaks a server.
struct Reaper(Child);
impl Drop for Reaper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A run submitted to the stub dagron-api.
#[derive(Clone, Debug)]
struct Submitted {
    /// The stored workflow run, or `None` for `POST /api/runs`.
    workflow: Option<String>,
    /// What `POST /api/runs` was given to run.
    yaml: Option<String>,
    key: String,
    auth: String,
    params: BTreeMap<String, String>,
}

type Seen = Arc<Mutex<Vec<Submitted>>>;
/// The runs the stub created, by `Idempotency-Key`.
type Runs = Arc<Mutex<BTreeMap<String, String>>>;

async fn stub_dagron_api() -> (String, Seen) {
    fn create(
        seen: &Seen,
        runs: &Runs,
        headers: &HeaderMap,
        workflow: Option<String>,
        body: &serde_json::Value,
    ) -> (StatusCode, String) {
        let h = |n: &str| {
            headers
                .get(n)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string()
        };
        let key = h("idempotency-key");
        seen.lock().unwrap().push(Submitted {
            workflow,
            yaml: body["yaml"].as_str().map(str::to_string),
            key: key.clone(),
            auth: h("authorization"),
            params: serde_json::from_value(body["parameters"].clone()).unwrap(),
        });
        let mut runs = runs.lock().unwrap();
        if let Some(run) = runs.get(&key) {
            return (StatusCode::OK, format!(r#"{{"run_id":"{run}"}}"#));
        }
        let run = format!("run-{}", runs.len() + 1);
        runs.insert(key, run.clone());
        (StatusCode::CREATED, format!(r#"{{"run_id":"{run}"}}"#))
    }
    async fn run(
        State((seen, runs)): State<(Seen, Runs)>,
        Path(workflow): Path<String>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> (StatusCode, String) {
        create(&seen, &runs, &headers, Some(workflow), &body)
    }
    async fn submit(
        State((seen, runs)): State<(Seen, Runs)>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> (StatusCode, String) {
        create(&seen, &runs, &headers, None, &body)
    }
    async fn me(headers: HeaderMap) -> Result<Json<serde_json::Value>, StatusCode> {
        match headers.get("authorization").and_then(|v| v.to_str().ok()) {
            Some("Bearer dgr_user") => Ok(Json(serde_json::json!({
                "sub": "u-1", "email": "dev@example.com", "name": "Dev", "groups": [], "exp": 0
            }))),
            _ => Err(StatusCode::UNAUTHORIZED),
        }
    }
    let seen: Seen = Arc::default();
    let app = Router::new()
        .route("/api/workflows/{workflow}/run", post(run))
        .route("/api/runs", post(submit))
        .route("/api/me", get(me))
        .with_state((seen.clone(), Arc::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, seen)
}

/// GitHub's contents API for `acme/widgets`: `.dagron/ci/ci.yaml` at
/// PIPELINED, and no `.dagron/ci` at any other commit.
async fn stub_github() -> String {
    async fn contents(
        Path(path): Path<String>,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> Response {
        if query.get("ref").map(String::as_str) != Some(PIPELINED) {
            return StatusCode::NOT_FOUND.into_response();
        }
        match path.as_str() {
            ".dagron/ci" => Json(serde_json::json!([{
                "type": "file", "name": "ci.yaml", "path": ".dagron/ci/ci.yaml", "size": PIPELINE.len(),
            }]))
            .into_response(),
            ".dagron/ci/ci.yaml" => PIPELINE.into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }
    // The one branch a test dispatches at, and the commit it is at.
    async fn release() -> &'static str {
        BASE
    }
    let app = Router::new()
        .route("/repos/acme/widgets/contents/{*path}", get(contents))
        .route("/repos/acme/widgets/commits/heads/release", get(release));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    base
}

fn push(git_ref: &str, before: &str, after: &str) -> Vec<u8> {
    serde_json::json!({
        "ref": git_ref,
        "before": before,
        "after": after,
        "created": false,
        "deleted": false,
        "repository": { "id": 7, "full_name": "acme/widgets", "html_url": "https://github.com/acme/widgets" },
        "sender": { "login": "octocat" },
    })
    .to_string()
    .into_bytes()
}

async fn deliver(
    http: &reqwest::Client,
    base: &str,
    id: &str,
    body: &[u8],
    secret: &[u8],
) -> (u16, String) {
    let res = http
        .post(format!("{base}/hooks/github"))
        .header("content-type", "application/json")
        .header("x-github-event", "push")
        .header("x-github-delivery", id)
        .header("x-hub-signature-256", dagron_ci::github::sign(secret, body))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    (res.status().as_u16(), res.text().await.unwrap())
}

async fn poll<T, F, Fut>(secs: u64, mut f: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(v) = f().await {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_signed_push_runs_the_bound_workflow_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = format!("sqlite://{}", dir.path().join("ci.db").display());
    let secret = dir.path().join("secret");
    std::fs::write(&secret, "s3cret\n").unwrap();
    let token = dir.path().join("token");
    std::fs::write(&token, "dgr_ci_token\n").unwrap();
    let (api, seen) = stub_dagron_api().await;
    let github = stub_github().await;
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let env = |cmd: &mut Command| {
        cmd.env("CI_DATABASE_URL", &db)
            .env("CI_LISTEN", format!("127.0.0.1:{port}"))
            .env("CI_GITHUB_WEBHOOK_SECRET_FILE", &secret)
            .env("CI_CELLS", format!("default={api}"))
            .env("CI_API_TOKEN_FILE", &token)
            // The repository is real on github.com; a test never polls it,
            // and reads its pipelines from the stub.
            .env("CI_POLL_SECS", "0")
            .env("CI_GITHUB_API_URL", &github)
            .env("RUST_LOG", "warn");
    };

    let mut add = Command::new(BIN);
    add.args([
        "repo",
        "add",
        "github.com/acme/widgets",
        "--workflow",
        "wf-1",
    ]);
    env(&mut add);
    assert!(add.status().unwrap().success());

    let mut serve = Command::new(BIN);
    serve.arg("serve").stdout(Stdio::null());
    env(&mut serve);
    let mut server = Reaper(serve.spawn().unwrap());
    let base = format!("http://127.0.0.1:{port}");
    let http = reqwest::Client::new();
    poll(20, || {
        let http = http.clone();
        let url = format!("{base}/healthz");
        async move {
            http.get(url)
                .send()
                .await
                .ok()
                .filter(|r| r.status().is_success())
        }
    })
    .await
    .expect("dagron-ci serves");

    let body = push("refs/heads/main", BASE, HEAD);
    let (status, text) = deliver(&http, &base, "d-1", &body, b"s3cret").await;
    assert_eq!(status, 202, "{text}");
    poll(20, || async {
        (!seen.lock().unwrap().is_empty()).then_some(())
    })
    .await
    .expect("a worker runs the workflow");
    {
        let seen = seen.lock().unwrap();
        let Submitted {
            workflow,
            key,
            auth,
            params,
            ..
        } = &seen[0];
        assert_eq!(workflow.as_deref(), Some("wf-1"));
        assert!(key.starts_with("ci-") && key.len() == 67, "{key}");
        assert_eq!(auth, "Bearer dgr_ci_token");
        assert_eq!(params["ci_sha"], HEAD);
        assert_eq!(params["ci_ref"], "refs/heads/main");
        assert_eq!(params["ci_event"], "push");
    }

    // A replayed delivery writes nothing; a second delivery of the same commit
    // (what a poll of it will be) is an event, finds the run, and asks nothing.
    let (status, text) = deliver(&http, &base, "d-1", &body, b"s3cret").await;
    assert_eq!((status, text.contains("duplicate")), (202, true), "{text}");
    let (status, _) = deliver(&http, &base, "d-2", &body, b"s3cret").await;
    assert_eq!(status, 202);
    let mut events = Command::new(BIN);
    events.args(["events", "--state", "done"]);
    env(&mut events);
    let listed = poll(20, || {
        let out = events.output().unwrap();
        let text = String::from_utf8(out.stdout).unwrap();
        async move { (text.lines().count() == 2).then_some(text) }
    })
    .await
    .expect("both events end done");
    assert!(
        listed.contains("submitted run run-1 on cell default"),
        "{listed}"
    );
    assert!(
        listed.contains("run run-1 was submitted for this commit already"),
        "{listed}"
    );
    assert_eq!(seen.lock().unwrap().len(), 1, "one run for one commit");
    let metrics = http
        .get(format!("{base}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics.contains("dagron_ci_poll_sweeps_total{outcome=\"queued\"} 0"),
        "{metrics}"
    );

    // The run API: a dagron-api user dispatches the workflow at a commit, with
    // an input, and reads back the run it became.
    let dispatched: serde_json::Value = {
        let res = http
            .post(format!("{base}/ci/v1/runs"))
            .bearer_auth("dgr_user")
            .header("idempotency-key", "release-1")
            .json(&serde_json::json!({
                "repo": "github.com/acme/widgets",
                "ref": "refs/heads/release",
                "sha": BASE,
                "inputs": { "channel": "beta" },
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status().as_u16(), 202);
        res.json().await.unwrap()
    };
    let id = dispatched["id"].as_str().unwrap().to_string();
    let read = poll(20, || {
        let (http, url) = (http.clone(), format!("{base}/ci/v1/runs/{id}"));
        async move {
            let read: serde_json::Value = http
                .get(url)
                .bearer_auth("dgr_user")
                .send()
                .await
                .ok()?
                .json()
                .await
                .ok()?;
            (read["state"] == "done").then_some(read)
        }
    })
    .await
    .expect("the dispatch runs");
    assert_eq!(read["engine_runs"][0]["id"], "run-2");
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        let params = &seen[1].params;
        assert_eq!(
            (
                params["ci_event"].as_str(),
                params["ci_sha"].as_str(),
                params["channel"].as_str()
            ),
            ("dispatch", BASE, "beta")
        );
    }

    // A commit that holds a pipeline runs it, compiled, in the bound
    // workflow's place.
    let body = push("refs/heads/main", HEAD, PIPELINED);
    let (status, text) = deliver(&http, &base, "d-3", &body, b"s3cret").await;
    assert_eq!(status, 202, "{text}");
    let submitted = poll(20, || async { seen.lock().unwrap().get(2).cloned() })
        .await
        .expect("a worker runs the pipeline");
    assert_eq!(submitted.workflow, None, "POST /api/runs, not the workflow");
    let yaml = submitted.yaml.as_deref().unwrap();
    assert!(yaml.starts_with("name: ci\n"), "{yaml}");
    assert!(
        yaml.contains("- name: CI_STEP_1_RUN\n    value: cargo test --locked\n"),
        "{yaml}"
    );
    assert_eq!(
        (
            submitted.params["ci_sha"].as_str(),
            submitted.params["ci_base_sha"].as_str()
        ),
        (PIPELINED, HEAD),
        "measured against where main was"
    );
    assert!(submitted.key.starts_with("ci-") && submitted.key.len() == 67);

    let refused = http
        .post(format!("{base}/ci/v1/runs"))
        .bearer_auth("dgr_stranger")
        .json(&serde_json::json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main" }))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status().as_u16(), 401);

    // SIGTERM: the server and its workers stop cleanly.
    let pid = server.0.id().to_string();
    assert!(Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .unwrap()
        .success());
    let started = Instant::now();
    let code = loop {
        if let Some(status) = server.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "dagron-ci did not stop"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(code.success(), "{code:?}");
}

/// A dagron-api whose runs a test moves along: each submitted run has the
/// pipeline's one job, `test`, in the state the test last set, and finishes
/// when the test says so.
#[derive(Default)]
struct Moving {
    /// The job's state, and the run's once it finished.
    job: String,
    finished: Option<String>,
    submitted: usize,
}

async fn stub_moving_api(st: Arc<Mutex<Moving>>) -> String {
    async fn submit(State(st): State<Arc<Mutex<Moving>>>) -> (StatusCode, String) {
        let mut st = st.lock().unwrap();
        st.submitted += 1;
        (StatusCode::CREATED, r#"{"run_id":"run-1"}"#.into())
    }
    async fn graph(
        State(st): State<Arc<Mutex<Moving>>>,
        Path(id): Path<String>,
        headers: HeaderMap,
    ) -> Response {
        let st = st.lock().unwrap();
        let authorized = headers.get("authorization").and_then(|v| v.to_str().ok())
            == Some("Bearer dgr_ci_token");
        if id != "run-1" || !authorized {
            return StatusCode::NOT_FOUND.into_response();
        }
        Json(serde_json::json!({
            "nodes": [{ "id": "t-1", "name": "test", "status": st.job, "attempt": 1 }],
            "edges": [],
        }))
        .into_response()
    }
    async fn wait(State(st): State<Arc<Mutex<Moving>>>, Path(id): Path<String>) -> Response {
        let st = st.lock().unwrap();
        if id != "run-1" {
            return StatusCode::NOT_FOUND.into_response();
        }
        Json(serde_json::json!({
            "run_id": id,
            "status": st.finished.clone().unwrap_or_else(|| "running".into()),
            "finished": st.finished.is_some(),
        }))
        .into_response()
    }
    let app = Router::new()
        .route("/api/runs", post(submit))
        .route("/api/runs/{id}/graph", get(graph))
        .route("/api/runs/{id}/wait", get(wait))
        .with_state(st);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    base
}

/// GitHub: the pipeline at PIPELINED, and the statuses posted to it.
type Posted = Arc<Mutex<Vec<(String, serde_json::Value, String)>>>;

async fn stub_github_taking_statuses(posted: Posted) -> String {
    async fn contents(Path(path): Path<String>) -> Response {
        match path.as_str() {
            ".dagron/ci" => Json(serde_json::json!([{
                "type": "file", "name": "ci.yaml", "path": ".dagron/ci/ci.yaml", "size": PIPELINE.len(),
            }]))
            .into_response(),
            ".dagron/ci/ci.yaml" => PIPELINE.into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }
    async fn status(
        State(posted): State<Posted>,
        Path(sha): Path<String>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> StatusCode {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        posted.lock().unwrap().push((sha, body, auth));
        StatusCode::CREATED
    }
    let app = Router::new()
        .route("/repos/acme/widgets/contents/{*path}", get(contents))
        .route("/repos/acme/widgets/statuses/{sha}", post(status))
        .with_state(posted);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    base
}

/// Through the shipped binary: a pushed commit's pipeline runs, its job goes
/// up on the commit as pending while it runs, then as its outcome, linked to
/// the run's page.
#[tokio::test]
async fn a_pipeline_s_job_reports_on_its_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = format!("sqlite://{}", dir.path().join("ci.db").display());
    let secret = dir.path().join("secret");
    std::fs::write(&secret, "s3cret\n").unwrap();
    let token = dir.path().join("token");
    std::fs::write(&token, "dgr_ci_token\n").unwrap();
    let github_token = dir.path().join("github-token");
    std::fs::write(&github_token, "ghp_reporter\n").unwrap();
    let moving = Arc::new(Mutex::new(Moving {
        job: "running".into(),
        ..Moving::default()
    }));
    let api = stub_moving_api(moving.clone()).await;
    let posted: Posted = Arc::default();
    let github = stub_github_taking_statuses(posted.clone()).await;
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let env = |cmd: &mut Command| {
        cmd.env("CI_DATABASE_URL", &db)
            .env("CI_LISTEN", format!("127.0.0.1:{port}"))
            .env("CI_GITHUB_WEBHOOK_SECRET_FILE", &secret)
            .env("CI_CELLS", format!("default={api}"))
            .env("CI_API_TOKEN_FILE", &token)
            .env("CI_POLL_SECS", "0")
            .env("CI_GITHUB_API_URL", &github)
            .env("CI_GITHUB_TOKEN_FILE", &github_token)
            .env("CI_RUN_URL", "https://dagron.example.com/runs/{run_id}")
            .env("RUST_LOG", "warn");
    };
    let mut add = Command::new(BIN);
    add.args(["repo", "add", "github.com/acme/widgets"]);
    env(&mut add);
    assert!(add.status().unwrap().success());
    let mut serve = Command::new(BIN);
    serve.arg("serve").stdout(Stdio::null());
    env(&mut serve);
    let _server = Reaper(serve.spawn().unwrap());
    let base = format!("http://127.0.0.1:{port}");
    let http = reqwest::Client::new();
    poll(20, || {
        let (http, url) = (http.clone(), format!("{base}/healthz"));
        async move {
            http.get(url)
                .send()
                .await
                .ok()
                .filter(|r| r.status().is_success())
        }
    })
    .await
    .expect("dagron-ci serves");

    let body = push("refs/heads/main", HEAD, PIPELINED);
    let (status, text) = deliver(&http, &base, "d-1", &body, b"s3cret").await;
    assert_eq!(status, 202, "{text}");
    let first = poll(30, || async { posted.lock().unwrap().first().cloned() })
        .await
        .expect("the job goes up as pending");
    let (sha, body, auth) = first;
    assert_eq!(sha, PIPELINED);
    assert_eq!(auth, "Bearer ghp_reporter");
    assert_eq!(body["context"], "dagron-ci/ci/test (push)");
    assert_eq!(
        (body["state"].as_str(), body["description"].as_str()),
        (Some("pending"), Some("running"))
    );
    assert_eq!(body["target_url"], "https://dagron.example.com/runs/run-1");

    {
        let mut st = moving.lock().unwrap();
        st.job = "succeeded".into();
        st.finished = Some("succeeded".into());
    }
    let last = poll(30, || async {
        let posted = posted.lock().unwrap();
        posted
            .iter()
            .find(|(_, body, _)| body["state"] == "success")
            .cloned()
    })
    .await
    .expect("then its outcome");
    assert_eq!(last.1["context"], "dagron-ci/ci/test (push)");
    assert_eq!(moving.lock().unwrap().submitted, 1);
    let metrics = http
        .get(format!("{base}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics.contains("dagron_ci_commit_statuses_total{outcome=\"posted\"} 2"),
        "{metrics}"
    );
}
