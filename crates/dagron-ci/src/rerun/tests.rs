//! A run that runs again, against a stub of its cell's dagron-api: when a
//! record re-opens and when it does not, through the datastore, the three
//! looks and the router.

use std::collections::BTreeMap;
use std::sync::Mutex;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::*;
use crate::config::Config;
use crate::db::tests::{drop_schema, postgres, sqlite};
use crate::db::{Claimed, Repo, ReportWrite, SubmittedRun};
use crate::dispatch::tests::claimed;
use crate::event::{Kind, Trust};
use crate::server::{router, AppState};
use crate::Seams;

const SECRET: &str = "engine-s3cret";
const CI_TOKEN: &str = "dgr_dagron_ci";
const REPOSITORY: &str = "https://github.com/acme/widgets.git";
const HOOK_SECRET: &str = "hook-s3cret";
/// The App whose check runs a rig posts, where it posts them.
const APP: u64 = 4242;

/// A cell's dagron-api, as much of it as following a run takes.
#[derive(Default)]
struct Cell {
    /// Each run it has, and how it reads.
    runs: BTreeMap<String, String>,
    /// What a rerun is answered in place of doing it.
    refuse_rerun: Option<u16>,
    /// The one run whose rerun is answered 403, whoever asks.
    forbidden: Option<String>,
    /// The runs whose rerun is not answered for longer than dagron-ci waits.
    stalled: Vec<String>,
    /// Each rerun asked for: the run, and the credential it was asked with.
    reruns: Vec<(String, String)>,
    /// The body each rerun was asked with, as it came: empty for none.
    bodies: Vec<String>,
    /// A dagron-api from before the rerun of every job: a rerun that names
    /// its `from` is answered 400, as it answered one.
    no_rerun_of_every_job: bool,
    /// Answers every read with a 500.
    down: bool,
}

type StubCell = Arc<Mutex<Cell>>;

fn auth(headers: &HeaderMap) -> String {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

async fn stub_cell() -> (String, StubCell) {
    async fn me(headers: HeaderMap) -> Response {
        let (sub, groups) = match auth(&headers).as_str() {
            "Bearer dgr_operator" => ("u-1", vec![]),
            "Bearer dgr_viewer" => ("u-3", vec!["viewer"]),
            "Bearer dgr_agent" => ("agent:fixer", vec![]),
            _ => return StatusCode::UNAUTHORIZED.into_response(),
        };
        Json(json!({ "sub": sub, "email": "", "groups": groups })).into_response()
    }
    async fn run(State(cell): State<StubCell>, Path(id): Path<String>) -> Response {
        let cell = cell.lock().unwrap();
        if cell.down {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        match cell.runs.get(&id) {
            Some(status) => {
                Json(json!({ "id": id, "status": status, "tasks": [] })).into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }
    async fn rerun(
        State(cell): State<StubCell>,
        Path(id): Path<String>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> Response {
        let stalls = {
            let mut cell = cell.lock().unwrap();
            cell.reruns.push((id.clone(), auth(&headers)));
            cell.bodies
                .push(String::from_utf8_lossy(&body).into_owned());
            cell.stalled.contains(&id)
        };
        let from = serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|body| body["from"].as_str().map(String::from));
        if stalls {
            tokio::time::sleep(Duration::from_secs(30)).await;
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        let mut cell = cell.lock().unwrap();
        if let Some(code) = cell.refuse_rerun {
            return StatusCode::from_u16(code).unwrap().into_response();
        }
        if cell.forbidden.as_deref() == Some(id.as_str()) {
            return StatusCode::FORBIDDEN.into_response();
        }
        let whole = match from.as_deref() {
            Some(from) if cell.no_rerun_of_every_job || from != "all" => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("unsupported rerun mode '{from}'; only 'failed' is supported"),
                )
                    .into_response();
            }
            Some(_) => true,
            None => false,
        };
        match cell.runs.get(&id).map(String::as_str) {
            Some("failed" | "cancelled") if !whole => {
                cell.runs.insert(id.clone(), "running".into());
                Json(json!({ "run_id": id, "rerun": 1 })).into_response()
            }
            Some("succeeded" | "failed" | "cancelled") if whole => {
                cell.runs.insert(id.clone(), "running".into());
                Json(json!({ "run_id": id, "rerun": 3 })).into_response()
            }
            Some(_) => (
                StatusCode::CONFLICT,
                format!("run '{id}' is not in a rerunnable state (failed/cancelled)"),
            )
                .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }
    async fn list(
        State(cell): State<StubCell>,
        Query(q): Query<BTreeMap<String, String>>,
        headers: HeaderMap,
    ) -> Response {
        let cell = cell.lock().unwrap();
        if cell.down || auth(&headers) != format!("Bearer {CI_TOKEN}") {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        assert_eq!(q.get("status").map(String::as_str), Some("running"));
        // Every running run: one dagron-ci could not mark as CI's is no
        // less its own.
        assert_eq!(q.get("trigger"), None);
        let offset: usize = q["offset"].parse().unwrap();
        let limit: usize = q["limit"].parse().unwrap();
        let running: Vec<Value> = cell
            .runs
            .iter()
            .filter(|(_, status)| *status == "running")
            .skip(offset)
            .take(limit)
            .map(|(id, status)| json!({ "id": id, "status": status }))
            .collect();
        Json(running).into_response()
    }
    let cell: StubCell = Arc::default();
    let app = Router::new()
        .route("/api/me", get(me))
        .route("/api/runs", get(list))
        .route("/api/runs/{id}", get(run))
        .route("/api/runs/{id}/rerun", post(rerun))
        .with_state(cell.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, cell)
}

/// dagron-ci over `db`, with one cell it holds a token for and a checkout
/// secret; and the event every run here is of.
struct Rig {
    app: Router,
    reruns: Arc<Reruns>,
    cell: StubCell,
    db: Db,
    repo: Repo,
    event: Claimed,
    _dir: tempfile::TempDir,
}

async fn rig(db: Db, seams: Seams) -> Rig {
    rig_checking(db, seams, None).await
}

/// A rig whose dagron-ci posts check runs as `checks_app`, where there is
/// one, and so takes a check's "Re-run".
async fn rig_checking(db: Db, seams: Seams, checks_app: Option<u64>) -> Rig {
    rig_with(db, seams, checks_app, false).await
}

/// A rig as [`rig_checking`]'s, whose dagron-ci runs an agent's work where
/// `agents` says so (CI_AGENT_RUNS=untrusted).
async fn rig_with(db: Db, seams: Seams, checks_app: Option<u64>, agents: bool) -> Rig {
    let (base, cell) = stub_cell().await;
    let dir = tempfile::tempdir().unwrap();
    let token = dir.path().join("token");
    std::fs::write(&token, format!("{CI_TOKEN}\n")).unwrap();
    let secret = dir.path().join("checkout-secret");
    std::fs::write(&secret, format!("{SECRET}\n")).unwrap();
    let hook = dir.path().join("hook-secret");
    std::fs::write(&hook, format!("{HOOK_SECRET}\n")).unwrap();
    let path = |p: &std::path::Path| p.to_str().unwrap().to_string();
    let config = Config::from_lookup(|name| match name {
        "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
        "CI_CELLS" => Some(format!("default={base}")),
        "CI_API_TOKEN_FILE" => Some(path(&token)),
        "CI_CHECKOUT_TOKEN_SECRET_FILE" => Some(path(&secret)),
        "CI_GITHUB_WEBHOOK_SECRET_FILE" => Some(path(&hook)),
        "CI_AGENT_RUNS" => agents.then(|| "untrusted".into()),
        _ => None,
    })
    .unwrap();
    let state = AppState::new(db.clone(), &config, seams).with_checks_app(checks_app);
    let (repo, event) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
    Rig {
        reruns: state
            .reruns
            .clone()
            .expect("a cell and a token: reruns are followed"),
        app: router(state),
        cell,
        db,
        repo,
        event,
        _dir: dir,
    }
}

impl Rig {
    /// A run of the event that dagron-ci submitted, running in its cell.
    async fn submitted(&self, run_id: &str) {
        record(&self.db, &self.repo, &self.event, run_id).await;
        self.cell
            .lock()
            .unwrap()
            .runs
            .insert(run_id.into(), "running".into());
    }

    /// The run ends as `status`, in its cell and on dagron-ci's record, a
    /// moment ago: a sight re-opens a record that ended before it, and
    /// SQLite tells the two apart to the millisecond.
    async fn ended(&self, run_id: &str, status: &str) {
        self.cell
            .lock()
            .unwrap()
            .runs
            .insert(run_id.into(), status.into());
        end(&self.db, run_id, status).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    fn reads(&self, run_id: &str, status: &str) {
        self.cell
            .lock()
            .unwrap()
            .runs
            .insert(run_id.into(), status.into());
    }

    async fn call(
        &self,
        method: Method,
        uri: &str,
        bearer: Option<&str>,
        body: Value,
    ) -> (StatusCode, Value) {
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(bearer) = bearer {
            req = req.header("authorization", format!("Bearer {bearer}"));
        }
        let body = if body.is_null() {
            Body::empty()
        } else {
            Body::from(body.to_string())
        };
        let res = self
            .app
            .clone()
            .oneshot(req.body(body).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, body)
    }

    /// Ask for a checkout credential for `run_id`, as its engine does.
    async fn credential(&self, run_id: &str) -> (StatusCode, Value) {
        let body = json!({
            "repository": REPOSITORY,
            "commit": self.event.sha,
            "run_id": run_id,
            "task_id": "build",
            "attempt": 1,
        });
        self.call(Method::POST, crate::checkout::PATH, Some(SECRET), body)
            .await
    }

    /// A `check_run` delivery, signed as GitHub signs one.
    async fn deliver(&self, delivery: &str, body: &Value) -> (StatusCode, Value) {
        self.deliver_as("check_run", delivery, body).await
    }

    /// A delivery of `event`, signed as GitHub signs one.
    async fn deliver_as(&self, event: &str, delivery: &str, body: &Value) -> (StatusCode, Value) {
        let body = body.to_string();
        let req = Request::post("/hooks/github")
            .header("content-type", "application/json")
            .header(crate::github::EVENT_HEADER, event)
            .header(crate::github::DELIVERY_HEADER, delivery)
            .header(
                crate::github::SIGNATURE_HEADER,
                crate::github::sign(HOOK_SECRET.as_bytes(), body.as_bytes()),
            )
            .body(Body::from(body))
            .unwrap();
        let res = self.app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn rerun(&self, bearer: Option<&str>, query: &str) -> (StatusCode, Value) {
        let uri = format!("/ci/v1/runs/{}/rerun{query}", self.event.id);
        self.call(Method::POST, &uri, bearer, Value::Null).await
    }

    /// `dagron_ci_runs_reopened_total{by}`, as `/metrics` renders it.
    async fn reopened(&self, by: &str) -> u64 {
        let res = self
            .app
            .clone()
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let text = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        let series = format!("dagron_ci_runs_reopened_total{{by=\"{by}\"}} ");
        String::from_utf8(text.to_vec())
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix(&series)?.parse().ok())
            .unwrap_or_else(|| panic!("no {series}"))
    }
}

/// A dispatch that the agent `agent:fixer` asked for, queued: its event.
async fn agents_dispatch(rig: &Rig) -> String {
    let dispatch = crate::event::NewEvent {
        kind: Kind::Dispatch,
        trust: Trust::Agent,
        actor: Some("agent:fixer".into()),
        ..crate::db::tests::event()
    };
    let crate::db::Dispatched::Event(id) = rig
        .db
        .record_dispatch(&rig.repo, &dispatch, &BTreeMap::new(), None, None, None)
        .await
        .unwrap()
    else {
        panic!("a new event");
    };
    id
}

/// Where this dagron-ci runs an agent's work (CI_AGENT_RUNS=untrusted), an
/// agent reruns an event it asked for, and no other; where it does not, it
/// reruns none, its own included. The cell is asked for nothing it refuses.
#[tokio::test]
async fn an_agent_reruns_the_events_it_asked_for_and_no_other() {
    let (db, _dir) = sqlite().await;
    let rig = rig_with(db, Seams::default(), None, true).await;
    rig.submitted("run-1").await;
    rig.ended("run-1", "failed").await;
    // The rig's event is a person's push.
    let (code, problem) = rig.rerun(Some("dgr_agent"), "").await;
    assert_eq!(code, StatusCode::FORBIDDEN, "{problem}");
    assert_eq!(problem["title"], "not the agent's");
    // Its own passes, to what there is of it: no run.
    let id = agents_dispatch(&rig).await;
    let uri = format!("/ci/v1/runs/{id}/rerun");
    let (code, problem) = rig
        .call(Method::POST, &uri, Some("dgr_agent"), Value::Null)
        .await;
    assert_eq!(code, StatusCode::CONFLICT, "{problem}");
    assert_eq!(problem["title"], "nothing to rerun");
    assert!(rig.cell.lock().unwrap().reruns.is_empty());

    let (db, _dir) = sqlite().await;
    let rig = rig_with(db, Seams::default(), None, false).await;
    let id = agents_dispatch(&rig).await;
    let uri = format!("/ci/v1/runs/{id}/rerun");
    let (code, problem) = rig
        .call(Method::POST, &uri, Some("dgr_agent"), Value::Null)
        .await;
    assert_eq!(code, StatusCode::FORBIDDEN, "{problem}");
    assert_eq!(problem["title"], "agent caller");
}

async fn record(db: &Db, repo: &Repo, event: &Claimed, run_id: &str) {
    assert!(db
        .record_run(&SubmittedRun {
            run_id: run_id.into(),
            event_id: event.id.clone(),
            repo_id: repo.id.clone(),
            cell: "default".into(),
            pipeline: format!(".dagron/ci/{run_id}.yaml"),
            idempotency_key: format!("ci-{run_id}"),
            on_failed: false,
        })
        .await
        .unwrap());
}

/// The reporter's write that ends `run_id` as `status`, with a meter set.
async fn end(db: &Db, run_id: &str, status: &str) {
    // Nothing else is due: each run here is ended before the next is made.
    let (due, _) = db
        .claim_report("reporter", Duration::from_secs(60))
        .await
        .unwrap()
        .expect("the run is due for a look");
    assert_eq!(due, run_id);
    let write = ReportWrite {
        status: Some(status.into()),
        meter: true,
        ..ReportWrite::default()
    };
    assert!(db.finish_report(run_id, "reporter", &write).await.unwrap());
}

async fn status(db: &Db, run_id: &str) -> String {
    db.run_commit(run_id)
        .await
        .unwrap()
        .expect("the run is on record")
        .status
}

/// The end due to the meter, taken: the run and how it ended.
async fn told(db: &Db) -> Option<(String, String)> {
    let ended = db.claim_meter_end(Duration::from_secs(60)).await.unwrap()?;
    db.meter_told(&ended.run.run_id).await.unwrap();
    Some((ended.run.run_id, ended.status))
}

/// How many runs count against the build's limit, as an admission sees it.
async fn active(db: &Db) -> u64 {
    let mut seen = 0;
    db.admit("ci-someone-else", crate::admission::HOLD, |active| {
        seen = active;
        Err("only looking".into())
    })
    .await
    .unwrap()
    .unwrap_err();
    seen
}

/// What re-opening a record is, in the datastore. An ended record alone, on
/// a sight later than its end; then it is a run that is followed, holds a
/// slot, and whose next end is told to the meter again.
async fn the_reopen_contract(db: &Db) {
    let (repo, event) = claimed(db, None, Kind::Push, Trust::Trusted).await;
    record(db, &repo, &event, "run-1").await;
    let ids = ["run-1".to_string(), "run-never".to_string()];

    // A run that has not ended has nothing to re-open.
    assert!(!db.reopen_run("run-1", Duration::ZERO, None).await.unwrap());
    assert!(db.ended_among(&ids).await.unwrap().is_empty());

    end(db, "run-1", "failed").await;
    assert_eq!(status(db, "run-1").await, "failed");
    assert_eq!(told(db).await, Some(("run-1".into(), "failed".into())));
    assert_eq!(db.ended_among(&ids).await.unwrap(), ["run-1"]);
    assert_eq!(active(db).await, 0, "over: it holds no slot");
    assert!(db.ended_among(&[]).await.unwrap().is_empty());

    // A sight from before its end says nothing of after it.
    assert!(!db
        .reopen_run("run-1", Duration::from_secs(3600), None)
        .await
        .unwrap());
    assert_eq!(status(db, "run-1").await, "failed");
    assert!(!db
        .reopen_run("run-never", Duration::ZERO, None)
        .await
        .unwrap());

    // Seen running since, by a few milliseconds, which both datastores tell
    // apart: given a slot to run again under, it re-opens, and is counted
    // once, in the slot's place.
    assert!(db
        .admit("ci-run-1", crate::admission::HOLD, |_| Ok(()))
        .await
        .unwrap()
        .is_ok());
    assert_eq!(active(db).await, 1);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(db
        .reopen_run("run-1", Duration::from_millis(5), Some("ci-run-1"))
        .await
        .unwrap());
    assert_eq!(status(db, "run-1").await, "submitted");
    assert_eq!(active(db).await, 1, "the run, and no slot beside it");
    assert!(db.ended_among(&ids).await.unwrap().is_empty());
    assert!(
        !db.reopen_run("run-1", Duration::ZERO, None).await.unwrap(),
        "once"
    );
    assert_eq!(told(db).await, None, "re-opening is no end");

    // It is due for a look at once, and its next end is an end like the
    // first: recorded, and told to the meter again, which takes a repeat.
    end(db, "run-1", "succeeded").await;
    assert_eq!(status(db, "run-1").await, "succeeded");
    assert_eq!(told(db).await, Some(("run-1".into(), "succeeded".into())));
    assert_eq!(active(db).await, 0);
}

#[tokio::test]
async fn a_record_reopens_on_a_sight_later_than_its_end() {
    let (db, _dir) = sqlite().await;
    the_reopen_contract(&db).await;
}

#[tokio::test]
async fn a_record_reopens_on_a_sight_later_than_its_end_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    the_reopen_contract(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

/// Only its cell saying the run is running re-opens a record: not a run that
/// reads over, not one the cell no longer has, and not a cell that cannot be
/// asked.
#[tokio::test]
async fn only_a_sight_of_the_run_running_reopens_its_record() {
    let (db, _dir) = sqlite().await;
    let rig = rig(db, Seams::default()).await;
    rig.submitted("run-1").await;
    rig.ended("run-1", "failed").await;
    let follow = || {
        rig.reruns
            .follow(&rig.db, "default", "run-1", Reopened::Sweep)
    };

    assert_eq!(follow().await, Ok(false), "it reads failed");
    rig.cell.lock().unwrap().runs.remove("run-1");
    assert_eq!(follow().await, Ok(false), "the cell has no such run");
    rig.cell.lock().unwrap().down = true;
    assert!(
        follow().await.is_err(),
        "a cell that cannot be asked has said nothing"
    );
    assert!(rig
        .reruns
        .follow(&rig.db, "another-cell", "run-1", Reopened::Sweep)
        .await
        .is_err());
    assert_eq!(status(&rig.db, "run-1").await, "failed");

    rig.cell.lock().unwrap().down = false;
    rig.reads("run-1", "running");
    assert_eq!(follow().await, Ok(true));
    assert_eq!(status(&rig.db, "run-1").await, "submitted");
    assert_eq!(
        follow().await,
        Ok(true),
        "running still, and followed already"
    );
}

/// A run moved through dagron-api, whose jobs fetch nothing, is found by the
/// sweep of its cell's running runs: on record as over, and running.
#[tokio::test]
async fn the_sweep_reopens_a_run_that_is_running_and_on_record_as_over() {
    let (db, _dir) = sqlite().await;
    let rig = rig(db, Seams::default()).await;
    for run in ["run-1", "run-2", "run-3"] {
        rig.submitted(run).await;
        if run != "run-3" {
            rig.ended(run, "cancelled").await;
        }
    }
    assert_eq!(
        rig.reruns.sweep(&rig.db).await,
        0,
        "nothing is running that is on record as over"
    );

    // Rerun through dagron-api: nothing told dagron-ci.
    rig.reads("run-1", "running");
    assert_eq!(rig.reruns.sweep(&rig.db).await, 1);
    assert_eq!(status(&rig.db, "run-1").await, "submitted");
    assert_eq!(
        status(&rig.db, "run-2").await,
        "cancelled",
        "over, and not running"
    );
    assert_eq!(
        status(&rig.db, "run-3").await,
        "submitted",
        "running, and followed all along"
    );
    assert_eq!(rig.reruns.sweep(&rig.db).await, 0, "once");
    assert_eq!(rig.reopened("sweep").await, 1);

    // A cell that cannot be asked is passed over, and nothing re-opens.
    rig.reads("run-2", "running");
    rig.cell.lock().unwrap().down = true;
    assert_eq!(rig.reruns.sweep(&rig.db).await, 0);
    assert_eq!(status(&rig.db, "run-2").await, "cancelled");
}

/// A rerun's job asks for its checkout credential while the record still
/// says the run is over. dagron-ci asks the cell itself, and mints for a run
/// the cell says is running. The request alone gets nothing.
#[tokio::test]
async fn a_credential_asked_for_a_run_that_is_running_again_is_given() {
    let (db, _dir) = sqlite().await;
    let rig = rig(db, Seams::default()).await;
    rig.submitted("run-1").await;
    rig.ended("run-1", "failed").await;

    let (code, problem) = rig.credential("run-1").await;
    assert_eq!(code, StatusCode::CONFLICT, "{problem}");
    assert!(
        problem["detail"]
            .as_str()
            .unwrap()
            .contains("is over (failed)"),
        "{problem}"
    );

    // The cell cannot be asked: no credential, and no word on the run.
    rig.cell.lock().unwrap().down = true;
    let (code, problem) = rig.credential("run-1").await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{problem}");
    assert!(
        problem["detail"]
            .as_str()
            .unwrap()
            .contains("could not be asked whether"),
        "{problem}"
    );
    assert_eq!(status(&rig.db, "run-1").await, "failed");

    // Rerun through dagron-api, and its job starts.
    rig.cell.lock().unwrap().down = false;
    rig.reads("run-1", "running");
    let (code, body) = rig.credential("run-1").await;
    assert_eq!(
        code,
        StatusCode::NO_CONTENT,
        "no App here: fetched without one; {body}"
    );
    assert_eq!(
        status(&rig.db, "run-1").await,
        "submitted",
        "and the run is followed again"
    );
    assert_eq!(rig.reopened("credential").await, 1);
    assert_eq!(
        rig.credential("run-1").await.0,
        StatusCode::NO_CONTENT,
        "as for any live run"
    );
    assert_eq!(rig.reopened("credential").await, 1);
}

/// The door: an event's failed and cancelled runs are rerun by their cell,
/// asked with the caller's own credential, and their records re-open with
/// the answer. The rest are left, each with why.
#[tokio::test]
async fn the_rerun_door_reruns_what_failed_and_follows_it() {
    let (db, _dir) = sqlite().await;
    let rig = rig(db, Seams::default()).await;
    rig.submitted("run-1").await;
    rig.ended("run-1", "failed").await;
    rig.submitted("run-2").await;
    rig.ended("run-2", "succeeded").await;
    rig.submitted("run-3").await;

    // Who may: an identified caller who may run things, and no agent.
    assert_eq!(rig.rerun(None, "").await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        rig.rerun(Some("dgr_stranger"), "").await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        rig.rerun(Some("dgr_viewer"), "").await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        rig.rerun(Some("dgr_agent"), "").await.0,
        StatusCode::FORBIDDEN
    );
    let (code, _) = rig
        .call(
            Method::POST,
            "/ci/v1/runs/no-such-event/rerun",
            Some("dgr_operator"),
            Value::Null,
        )
        .await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    assert!(
        rig.cell.lock().unwrap().reruns.is_empty(),
        "the cell was asked for nothing"
    );

    let (code, body) = rig.rerun(Some("dgr_operator"), "?failed=true").await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["id"], rig.event.id);
    assert_eq!(
        body["rerun"],
        json!([{ "id": "run-1", "cell": "default", "pipeline": ".dagron/ci/run-1.yaml", "jobs": 1 }])
    );
    let left: Vec<(&str, &str)> = body["left"]
        .as_array()
        .unwrap()
        .iter()
        .map(|run| (run["id"].as_str().unwrap(), run["why"].as_str().unwrap()))
        .collect();
    assert_eq!(
        left,
        [
            ("run-2", "it succeeded: none of its jobs failed"),
            ("run-3", "it is running"),
        ]
    );
    assert_eq!(
        rig.cell.lock().unwrap().reruns,
        [("run-1".to_string(), "Bearer dgr_operator".to_string())],
        "asked as the caller, for the one run that failed"
    );
    assert_eq!(
        rig.cell.lock().unwrap().bodies,
        [""],
        "with no body: from its failed jobs, as it always was"
    );
    assert_eq!(status(&rig.db, "run-1").await, "submitted");
    assert_eq!(rig.reopened("rerun").await, 1);

    // What the event's read says of each run now.
    let (_, seen) = rig
        .call(
            Method::GET,
            &format!("/ci/v1/runs/{}", rig.event.id),
            Some("dgr_viewer"),
            Value::Null,
        )
        .await;
    let statuses: Vec<&str> = seen["engine_runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|run| run["status"].as_str().unwrap())
        .collect();
    assert_eq!(statuses, ["submitted", "succeeded", "submitted"]);

    // Again, with nothing failed: nothing to rerun, and each run says why.
    let (code, problem) = rig.rerun(Some("dgr_operator"), "").await;
    assert_eq!(code, StatusCode::CONFLICT, "{problem}");
    assert!(problem["detail"]
        .as_str()
        .unwrap()
        .contains("run run-1 (.dagron/ci/run-1.yaml): it is running"));
    assert_eq!(rig.cell.lock().unwrap().reruns.len(), 1);
}

/// `?failed=false`: each of the event's runs that is over is run again
/// whole, one that succeeded too, asked of its cell as `{"from": "all"}`
/// with the caller's own credential, and each record re-opens. A run that is
/// running is left, and an agent is refused as before. A `failed` that is
/// neither `true` nor `false` is refused with a problem document.
async fn every_job(db: Db) {
    let rig = rig(db, Seams::default()).await;
    rig.submitted("run-1").await;
    rig.ended("run-1", "failed").await;
    rig.submitted("run-2").await;
    rig.ended("run-2", "succeeded").await;
    rig.submitted("run-3").await;

    assert_eq!(
        rig.rerun(Some("dgr_agent"), "?failed=false").await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        rig.rerun(Some("dgr_viewer"), "?failed=false").await.0,
        StatusCode::FORBIDDEN
    );
    for query in ["?failed=no", "?failed="] {
        let (code, problem) = rig.rerun(Some("dgr_operator"), query).await;
        assert_eq!(code, StatusCode::BAD_REQUEST, "{query}: {problem}");
        assert_eq!(problem["title"], "invalid rerun", "{query}: {problem}");
    }
    assert!(rig.cell.lock().unwrap().reruns.is_empty());

    let (code, body) = rig.rerun(Some("dgr_operator"), "?failed=false").await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(
        body["rerun"],
        json!([
            { "id": "run-1", "cell": "default", "pipeline": ".dagron/ci/run-1.yaml", "jobs": 3 },
            { "id": "run-2", "cell": "default", "pipeline": ".dagron/ci/run-2.yaml", "jobs": 3 },
        ])
    );
    assert_eq!(
        body["left"],
        json!([{ "id": "run-3", "pipeline": ".dagron/ci/run-3.yaml", "why": "it is running" }])
    );
    {
        let cell = rig.cell.lock().unwrap();
        let operator = "Bearer dgr_operator".to_string();
        assert_eq!(
            cell.reruns,
            [
                ("run-1".to_string(), operator.clone()),
                ("run-2".to_string(), operator)
            ]
        );
        for body in &cell.bodies {
            assert_eq!(
                serde_json::from_str::<Value>(body).unwrap(),
                json!({ "from": "all" })
            );
        }
    }
    for run in ["run-1", "run-2", "run-3"] {
        assert_eq!(status(&rig.db, run).await, "submitted", "{run}");
    }
    assert_eq!(rig.reopened("rerun").await, 2);

    // Nothing is over now: nothing to rerun, and each run says why.
    let (code, problem) = rig.rerun(Some("dgr_operator"), "?failed=false").await;
    assert_eq!(code, StatusCode::CONFLICT, "{problem}");
    assert_eq!(rig.cell.lock().unwrap().reruns.len(), 2);
}

#[tokio::test]
async fn the_rerun_door_reruns_every_job_with_failed_false() {
    let (db, _dir) = sqlite().await;
    every_job(db).await;
}

#[tokio::test]
async fn the_rerun_door_reruns_every_job_with_failed_false_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    every_job(db.clone()).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

/// A cell whose dagron-api is older than the rerun of every job answers
/// `{"from": "all"}` with a 400. The run is left with why, its slot under
/// the build's limit is let go, and the door answers 409, not 503: there is
/// nothing to try again. Once the cell runs it, the rerun holds the slot.
#[tokio::test]
async fn a_cell_that_runs_no_rerun_of_every_job_leaves_the_run_with_why() {
    let (db, _dir) = sqlite().await;
    let seams = Seams {
        admission: Some(Arc::new(Cap(1))),
        ..Seams::default()
    };
    let rig = rig(db, seams).await;
    rig.submitted("run-1").await;
    rig.ended("run-1", "succeeded").await;
    rig.cell.lock().unwrap().no_rerun_of_every_job = true;

    let (code, problem) = rig.rerun(Some("dgr_operator"), "?failed=false").await;
    assert_eq!(code, StatusCode::CONFLICT, "{problem}");
    let detail = problem["detail"].as_str().unwrap();
    assert!(
        detail.contains(
            "run run-1 (.dagron/ci/run-1.yaml): its cell's dagron-api runs no rerun of every \
             job: unsupported rerun mode 'all'"
        ),
        "{problem}"
    );
    assert_eq!(status(&rig.db, "run-1").await, "succeeded");
    assert_eq!(active(&rig.db).await, 0, "the slot was let go");

    rig.cell.lock().unwrap().no_rerun_of_every_job = false;
    let (code, body) = rig.rerun(Some("dgr_operator"), "?failed=false").await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(status(&rig.db, "run-1").await, "submitted");
    assert_eq!(active(&rig.db).await, 1, "the rerun, counted once");
}

/// What the cell answers is the caller's answer: a caller it does not let
/// rerun is refused as it refused them; a run it will not rerun because it
/// runs already is followed; and a cell that cannot be asked is a 503.
#[tokio::test]
async fn the_rerun_door_answers_as_the_cell_does() {
    let (db, _dir) = sqlite().await;
    let rig = rig(db, Seams::default()).await;
    rig.submitted("run-1").await;
    rig.ended("run-1", "failed").await;

    rig.cell.lock().unwrap().refuse_rerun = Some(403);
    assert_eq!(
        rig.rerun(Some("dgr_operator"), "").await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(status(&rig.db, "run-1").await, "failed");

    rig.cell.lock().unwrap().refuse_rerun = Some(500);
    let (code, problem) = rig.rerun(Some("dgr_operator"), "").await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{problem}");
    assert_eq!(status(&rig.db, "run-1").await, "failed");

    // Its concurrency key is full: the cell says 429, and the run waits. That
    // is nothing to rerun now, and nothing went wrong.
    rig.cell.lock().unwrap().refuse_rerun = Some(429);
    let (code, problem) = rig.rerun(Some("dgr_operator"), "").await;
    assert_eq!(code, StatusCode::CONFLICT, "{problem}");
    assert!(
        problem["detail"]
            .as_str()
            .unwrap()
            .contains("run run-1 (.dagron/ci/run-1.yaml): waiting:"),
        "{problem}"
    );
    assert_eq!(status(&rig.db, "run-1").await, "failed");

    // Rerun through dagron-api a moment ago: the cell refuses a second, and
    // the run it is already running is followed.
    rig.cell.lock().unwrap().refuse_rerun = None;
    rig.reads("run-1", "running");
    let (code, problem) = rig.rerun(Some("dgr_operator"), "").await;
    assert_eq!(code, StatusCode::CONFLICT, "{problem}");
    assert!(
        problem["detail"]
            .as_str()
            .unwrap()
            .contains("it is running again already, and is followed"),
        "{problem}"
    );
    assert_eq!(status(&rig.db, "run-1").await, "submitted");
}

/// A cell that lets the caller rerun one of an event's runs and not the next:
/// the first is running again by then, and the answer says so. Refused for
/// the first, the caller gets the cell's refusal and nothing more is asked.
#[tokio::test]
async fn the_rerun_door_says_what_it_reran_when_a_later_run_is_refused() {
    let (db, _dir) = sqlite().await;
    let rig = rig(db, Seams::default()).await;
    for run in ["run-1", "run-2"] {
        rig.submitted(run).await;
        rig.ended(run, "failed").await;
    }

    rig.cell.lock().unwrap().forbidden = Some("run-1".into());
    assert_eq!(
        rig.rerun(Some("dgr_operator"), "").await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        rig.cell.lock().unwrap().reruns.len(),
        1,
        "run-2 was not asked for"
    );
    assert_eq!(status(&rig.db, "run-2").await, "failed");

    rig.cell.lock().unwrap().forbidden = Some("run-2".into());
    let (code, body) = rig.rerun(Some("dgr_operator"), "").await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["rerun"][0]["id"], "run-1");
    assert_eq!(
        body["left"],
        json!([{
            "id": "run-2",
            "pipeline": ".dagron/ci/run-2.yaml",
            "why": "dagron-api did not let this caller rerun it",
        }])
    );
    assert_eq!(status(&rig.db, "run-1").await, "submitted");
    assert_eq!(status(&rig.db, "run-2").await, "failed");
}

/// A door asked with an engine run's id, the one a commit status links to,
/// is asked about that run of its event and no other.
#[tokio::test]
async fn a_run_s_own_id_names_that_run_at_a_door() {
    let (db, _dir) = sqlite().await;
    let rig = rig(db, Seams::default()).await;
    for run in ["run-1", "run-2"] {
        rig.submitted(run).await;
        rig.ended(run, "failed").await;
    }
    let operator = Some("dgr_operator");

    let (code, body) = rig
        .call(Method::GET, "/ci/v1/runs/run-2", operator, Value::Null)
        .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["id"], json!(rig.event.id), "its event");
    assert_eq!(body["engine_runs"].as_array().unwrap().len(), 1);
    assert_eq!(body["engine_runs"][0]["id"], "run-2");

    let (code, body) = rig
        .call(
            Method::POST,
            "/ci/v1/runs/run-2/rerun",
            operator,
            Value::Null,
        )
        .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["id"], json!(rig.event.id));
    assert_eq!(body["rerun"].as_array().unwrap().len(), 1);
    assert_eq!(body["rerun"][0]["id"], "run-2");
    assert_eq!(body["left"], json!([]));
    assert_eq!(
        rig.cell.lock().unwrap().reruns.len(),
        1,
        "run-1 was not asked for"
    );
    assert_eq!(status(&rig.db, "run-1").await, "failed");
    assert_eq!(status(&rig.db, "run-2").await, "submitted");

    // An id that is neither names nothing, and says so to someone
    // identified alone.
    let (code, _) = rig
        .call(Method::GET, "/ci/v1/runs/run-9", operator, Value::Null)
        .await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    for id in ["run-2", "run-9"] {
        let (code, _) = rig
            .call(
                Method::GET,
                &format!("/ci/v1/runs/{id}"),
                Some("dgr_nobody"),
                Value::Null,
            )
            .await;
        assert_eq!(code, StatusCode::UNAUTHORIZED, "{id}");
    }
}

/// The build's limit on runs in flight.
struct Cap(u64);

impl crate::Admission for Cap {
    fn admit(&self, active: u64) -> Result<(), String> {
        if active < self.0 {
            Ok(())
        } else {
            Err(format!("the limit is {} and {active} are active", self.0))
        }
    }
}

/// A rerun passes the build's limit like a new run: it waits while the limit
/// is taken, without the cell being asked, and holds a slot once it runs.
#[tokio::test]
async fn a_rerun_passes_the_build_s_limit_like_a_new_run() {
    let (db, _dir) = sqlite().await;
    let seams = Seams {
        admission: Some(Arc::new(Cap(1))),
        ..Seams::default()
    };
    let rig = rig(db, seams).await;
    rig.submitted("run-1").await;
    rig.ended("run-1", "failed").await;
    rig.submitted("run-2").await;

    let (code, problem) = rig.rerun(Some("dgr_operator"), "").await;
    assert_eq!(code, StatusCode::CONFLICT, "{problem}");
    assert!(
        problem["detail"]
            .as_str()
            .unwrap()
            .contains("waiting: the limit is 1 and 1 are active"),
        "{problem}"
    );
    assert!(
        rig.cell.lock().unwrap().reruns.is_empty(),
        "not asked for while it waits"
    );
    assert_eq!(
        active(&rig.db).await,
        1,
        "and it holds no slot of its own meanwhile"
    );

    rig.ended("run-2", "succeeded").await;
    let (code, body) = rig.rerun(Some("dgr_operator"), "").await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(active(&rig.db).await, 1, "the rerun, counted once");
}

/// A check run's "Re-run", as GitHub delivers one: of `run`'s check run, an
/// App's, asked by a sender of a kind, in a repository.
fn rerequested(run: &str, app: u64, sender: &str, repo: &str) -> Value {
    json!({
        "action": "rerequested",
        "check_run": {
            "id": 9,
            "name": "dagron-ci/ci/test (push)",
            "external_id": run,
            "app": { "id": app },
        },
        "repository": { "id": 7, "full_name": repo, "html_url": format!("https://github.com/{repo}") },
        "sender": { "login": "octocat", "type": sender },
    })
}

/// A check's "Re-run": the run the check run is of is run again from its
/// failed jobs, with dagron-ci's own token, and its record re-opens: asked
/// of the cell with no body, so not of every job. No other run is, and none
/// is for a delivery that is not such a check run's.
#[tokio::test]
async fn a_check_s_re_run_reruns_the_run_it_is_of() {
    let (db, _dir) = sqlite().await;
    let rig = rig_checking(db, Seams::default(), Some(APP)).await;
    rig.submitted("run-1").await;
    rig.ended("run-1", "failed").await;
    rig.submitted("run-2").await;
    rig.ended("run-2", "failed").await;
    // A second repository, with no run of its own.
    rig.db
        .add_repo("github", "github.com", "acme/gadgets", None, "default")
        .await
        .unwrap()
        .unwrap();

    for (what, delivery) in [
        (
            "another App's check run",
            rerequested("run-1", 7, "User", "acme/widgets"),
        ),
        (
            "a bot's asking",
            rerequested("run-1", APP, "Bot", "acme/widgets"),
        ),
        (
            "a run dagron-ci does not have",
            rerequested("run-9", APP, "User", "acme/widgets"),
        ),
        (
            "what is no run's id",
            rerequested("../run-1", APP, "User", "acme/widgets"),
        ),
        (
            "another repository's run",
            rerequested("run-1", APP, "User", "acme/gadgets"),
        ),
    ] {
        let (code, answer) = rig.deliver("d-0", &delivery).await;
        assert_eq!(code, StatusCode::ACCEPTED, "{what}: {answer}");
        assert!(answer["ignored"].is_string(), "{what}: {answer}");
    }
    let elsewhere = rerequested("run-1", APP, "User", "acme/elsewhere");
    let (code, _) = rig.deliver("d-0", &elsewhere).await;
    assert_eq!(code, StatusCode::NOT_FOUND, "not a connected repository");
    assert!(
        rig.cell.lock().unwrap().reruns.is_empty(),
        "the cell was asked for nothing"
    );

    // GitHub names a repository in the case it was made in.
    let asked = rerequested("run-1", APP, "User", "Acme/Widgets");
    let (code, answer) = rig.deliver("d-1", &asked).await;
    assert_eq!(code, StatusCode::ACCEPTED, "{answer}");
    assert_eq!(answer["rerun"][0]["id"], "run-1", "{answer}");
    assert_eq!(
        answer["rerun"][0]["jobs"], 1,
        "its failed jobs, not every job: {answer}"
    );
    assert_eq!(
        rig.cell.lock().unwrap().reruns,
        [("run-1".to_string(), format!("Bearer {CI_TOKEN}"))],
        "that run alone, asked for as dagron-ci"
    );
    assert_eq!(rig.cell.lock().unwrap().bodies, [""], "asked with no body");
    assert_eq!(status(&rig.db, "run-1").await, "submitted");
    assert_eq!(status(&rig.db, "run-2").await, "failed");

    // Asked again while it runs: nothing is rerun, and the answer says why.
    let (code, answer) = rig.deliver("d-2", &asked).await;
    assert_eq!(code, StatusCode::ACCEPTED, "{answer}");
    let why = answer["ignored"].as_str().unwrap_or_default();
    assert!(why.contains("it is running"), "{answer}");
    assert_eq!(rig.cell.lock().unwrap().reruns.len(), 1);

    // A delivery is taken once. The run fails again, and the delivery that
    // reran it is sent again, as a redelivery or a replayed capture is: it
    // reruns nothing. So with one that was answered and ran nothing.
    rig.ended("run-1", "failed").await;
    for seen in ["d-1", "d-2"] {
        let (code, answer) = rig.deliver(seen, &asked).await;
        assert_eq!(
            (code, &answer["duplicate"]),
            (StatusCode::ACCEPTED, &json!(true)),
            "{seen}: {answer}"
        );
    }
    assert_eq!(rig.cell.lock().unwrap().reruns.len(), 1);

    // A cell that does not answer is not a re-run taken: GitHub is told, and
    // the delivery can be sent again, and is then taken.
    rig.cell.lock().unwrap().refuse_rerun = Some(500);
    let (code, _) = rig.deliver("d-3", &asked).await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
    rig.cell.lock().unwrap().refuse_rerun = None;
    let (code, answer) = rig.deliver("d-3", &asked).await;
    assert_eq!(code, StatusCode::ACCEPTED, "{answer}");
    assert_eq!(answer["rerun"][0]["id"], "run-1", "{answer}");
    rig.ended("run-1", "failed").await;
    // And a dagron-api that does not let dagron-ci's token rerun says so.
    rig.cell.lock().unwrap().refuse_rerun = Some(403);
    let (code, _) = rig.deliver("d-4", &asked).await;
    assert_eq!(code, StatusCode::BAD_GATEWAY);
}

/// "Re-run all checks", as GitHub delivers one: of an App's suite at a
/// commit, asked by a sender of a kind, in a repository.
fn suite_rerequested(sha: &str, app: u64, sender: &str, repo: &str) -> Value {
    json!({
        "action": "rerequested",
        "check_suite": { "id": 3, "head_sha": sha, "app": { "id": app } },
        "repository": { "id": 7, "full_name": repo, "html_url": format!("https://github.com/{repo}") },
        "sender": { "login": "octocat", "type": sender },
    })
}

/// "Re-run all checks": each run of the commit that was reported as check
/// runs is run again from its failed jobs, with dagron-ci's own token, and
/// one that was reported as statuses is of no suite. A run its cell refuses
/// that token for is left with why while the others run again, and is run
/// again when it is asked for again. One that succeeded is left: GitHub's
/// buttons run no rerun of every job.
#[tokio::test]
async fn a_re_run_of_all_checks_reruns_the_commit_s_runs_that_were_check_runs() {
    let (db, _dir) = sqlite().await;
    let rig = rig_checking(db, Seams::default(), Some(APP)).await;
    for run in ["run-1", "run-2", "run-3"] {
        rig.submitted(run).await;
        rig.ended(run, "failed").await;
    }
    rig.submitted("run-4").await;
    rig.ended("run-4", "succeeded").await;
    // The first two and the fourth were reported as check runs, and the third
    // as statuses.
    crate::db::tests::exec(
        &rig.db,
        r#"UPDATE ci_runs SET reported = '{"dagron-ci/ci/test (push)":"check:1:completed/failure:failed"}' WHERE run_id IN ('run-1', 'run-2', 'run-4')"#,
    )
    .await;
    let sha = rig.event.sha.clone();
    let nowhere = "0".repeat(40);

    for (what, delivery) in [
        (
            "another App's suite",
            suite_rerequested(&sha, 7, "User", "acme/widgets"),
        ),
        (
            "a bot's asking",
            suite_rerequested(&sha, APP, "Bot", "acme/widgets"),
        ),
        (
            "a commit dagron-ci posted no check run for",
            suite_rerequested(&nowhere, APP, "User", "acme/widgets"),
        ),
    ] {
        let (code, answer) = rig.deliver_as("check_suite", "s-0", &delivery).await;
        assert_eq!(code, StatusCode::ACCEPTED, "{what}: {answer}");
        assert!(answer["ignored"].is_string(), "{what}: {answer}");
    }
    assert!(
        rig.cell.lock().unwrap().reruns.is_empty(),
        "the cell was asked for nothing"
    );

    // A cell that refuses dagron-ci's token for every run reran nothing:
    // that is said as it is for a check's own re-run, and the delivery is
    // not kept.
    let asked = suite_rerequested(&sha, APP, "User", "acme/widgets");
    rig.cell.lock().unwrap().refuse_rerun = Some(403);
    let (code, _) = rig.deliver_as("check_suite", "s-1", &asked).await;
    assert_eq!(code, StatusCode::BAD_GATEWAY);
    {
        let mut cell = rig.cell.lock().unwrap();
        assert_eq!(cell.reruns.len(), 2, "each of the two was asked for");
        cell.reruns.clear();
        cell.refuse_rerun = None;
        // It lets dagron-ci rerun the first from here, and not the second.
        cell.forbidden = Some("run-2".into());
    }
    // Sent again, it is taken. The first is running again by the time the
    // second is refused, and the answer says both.
    let (code, answer) = rig.deliver_as("check_suite", "s-1", &asked).await;
    assert_eq!(code, StatusCode::ACCEPTED, "{answer}");
    assert_eq!(
        answer["rerun"].as_array().map(Vec::len),
        Some(1),
        "{answer}"
    );
    assert_eq!(answer["rerun"][0]["id"], "run-1", "{answer}");
    assert_eq!(answer["rerun"][0]["jobs"], 1, "its failed jobs: {answer}");
    assert_eq!(
        answer["left"],
        json!([
            {
                "id": "run-2",
                "pipeline": ".dagron/ci/run-2.yaml",
                "why": "dagron-api did not let dagron-ci's own token rerun it",
            },
            {
                "id": "run-4",
                "pipeline": ".dagron/ci/run-4.yaml",
                "why": "it succeeded: none of its jobs failed",
            },
        ])
    );
    let as_ci = format!("Bearer {CI_TOKEN}");
    assert_eq!(
        rig.cell.lock().unwrap().reruns,
        [
            ("run-1".to_string(), as_ci.clone()),
            ("run-2".to_string(), as_ci.clone())
        ],
        "the two that were check runs, as dagron-ci; the third is of no suite"
    );
    assert_eq!(status(&rig.db, "run-1").await, "submitted");
    assert_eq!(status(&rig.db, "run-2").await, "failed");
    assert_eq!(status(&rig.db, "run-3").await, "failed");

    // Asked again, which is a new delivery: what was left is run again, and
    // what is running is left.
    rig.cell.lock().unwrap().forbidden = None;
    let (code, answer) = rig.deliver_as("check_suite", "s-2", &asked).await;
    assert_eq!(code, StatusCode::ACCEPTED, "{answer}");
    assert_eq!(answer["rerun"][0]["id"], "run-2", "{answer}");
    assert_eq!(answer["left"][0]["id"], "run-1", "{answer}");
    assert_eq!(answer["left"][0]["why"], "it is running", "{answer}");
    assert_eq!(status(&rig.db, "run-2").await, "submitted");
    assert_eq!(status(&rig.db, "run-3").await, "failed");

    // A delivery is taken once.
    let (code, answer) = rig.deliver_as("check_suite", "s-2", &asked).await;
    assert_eq!(
        (code, &answer["duplicate"]),
        (StatusCode::ACCEPTED, &json!(true)),
        "{answer}"
    );
    let cell = rig.cell.lock().unwrap();
    assert_eq!(cell.reruns.len(), 3);
    assert!(
        cell.bodies.iter().all(String::is_empty),
        "each from its failed jobs, asked with no body: {:?}",
        cell.bodies
    );
    assert_eq!(
        status_of(&cell, "run-4"),
        "succeeded",
        "never asked of the cell"
    );
}

/// How the stand-in cell has `run`.
fn status_of(cell: &Cell, run: &str) -> String {
    cell.runs.get(run).cloned().unwrap_or_default()
}

/// "Re-run all checks" that is cut short keeps what it did. The second
/// run's cell does not answer and neither does the third's, the time GitHub
/// waits for an answer runs out, and the first run is running again by
/// then. The answer says so, with the others left and why, and the delivery
/// stays taken: sent again it reruns nothing, whatever the first run has
/// done since.
#[tokio::test]
async fn a_re_run_of_all_checks_that_is_cut_short_keeps_what_it_did() {
    let (db, _dir) = sqlite().await;
    let rig = rig_checking(db, Seams::default(), Some(APP)).await;
    for run in ["run-1", "run-2", "run-3"] {
        rig.submitted(run).await;
        rig.ended(run, "failed").await;
    }
    crate::db::tests::exec(
        &rig.db,
        r#"UPDATE ci_runs SET reported = '{"dagron-ci/ci/test (push)":"check:1:completed/failure:failed"}'"#,
    )
    .await;
    rig.cell.lock().unwrap().stalled = vec!["run-2".into(), "run-3".into()];
    let asked = suite_rerequested(&rig.event.sha, APP, "User", "acme/widgets");

    let (code, answer) = rig.deliver_as("check_suite", "s-1", &asked).await;
    assert_eq!(code, StatusCode::ACCEPTED, "{answer}");
    assert_eq!(
        answer["rerun"].as_array().map(Vec::len),
        Some(1),
        "{answer}"
    );
    assert_eq!(answer["rerun"][0]["id"], "run-1", "{answer}");
    let left: BTreeMap<&str, &str> = answer["left"]
        .as_array()
        .unwrap()
        .iter()
        .map(|run| (run["id"].as_str().unwrap(), run["why"].as_str().unwrap()))
        .collect();
    assert_eq!(
        left,
        BTreeMap::from([
            ("run-2", "dagron-api could not be asked for it"),
            (
                "run-3",
                "its cell had not answered in time, or was not asked"
            ),
        ]),
        "{answer}"
    );
    assert_eq!(status(&rig.db, "run-1").await, "submitted");

    // The first run fails again, and the delivery is sent again.
    rig.cell.lock().unwrap().stalled.clear();
    rig.ended("run-1", "failed").await;
    let (code, answer) = rig.deliver_as("check_suite", "s-1", &asked).await;
    assert_eq!(
        (code, &answer["duplicate"]),
        (StatusCode::ACCEPTED, &json!(true)),
        "{answer}"
    );
    let asked_for = |run: &str| {
        let cell = rig.cell.lock().unwrap();
        cell.reruns.iter().filter(|(id, _)| id == run).count()
    };
    assert_eq!(asked_for("run-1"), 1, "it was not run again a second time");
    assert_eq!(status(&rig.db, "run-1").await, "failed");
}

/// Without check runs, a "Re-run" is nobody's: it is dropped.
#[tokio::test]
async fn a_check_s_re_run_is_dropped_where_check_runs_are_off() {
    let (db, _dir) = sqlite().await;
    let rig = rig(db, Seams::default()).await;
    rig.submitted("run-1").await;
    rig.ended("run-1", "failed").await;
    let asked = rerequested("run-1", APP, "User", "acme/widgets");
    let (code, answer) = rig.deliver("d-1", &asked).await;
    assert_eq!(code, StatusCode::ACCEPTED, "{answer}");
    assert!(
        answer["ignored"]
            .as_str()
            .is_some_and(|why| why.contains("CI_GITHUB_CHECKS")),
        "{answer}"
    );
    assert!(rig.cell.lock().unwrap().reruns.is_empty());
}
