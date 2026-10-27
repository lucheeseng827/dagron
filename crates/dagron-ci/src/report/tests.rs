use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Router;
use serde_json::{json, Value};
use sha2::Digest;

use super::*;
use crate::db::tests::{drop_schema, postgres, sqlite};
use crate::db::{Claimed, SubmittedRun};
use crate::dispatch::tests::claimed;
use crate::event::{Kind, Trust};

fn job(name: &str, status: &str) -> Job {
    Job {
        id: format!("t-{name}"),
        name: name.into(),
        status: status.into(),
        attempt: 1,
    }
}

fn seen(jobs: &[(&str, &str)], finished: Option<&str>) -> Seen {
    Seen {
        jobs: jobs.iter().map(|(n, s)| job(n, s)).collect(),
        finished: finished.map(str::to_string),
        whole_reruns: None,
    }
}

fn status_of(
    statuses: &BTreeMap<String, (&'static str, String)>,
    job: &str,
) -> (&'static str, String) {
    statuses
        .get(&format!("dagron-ci/ci/{job} (push)"))
        .cloned()
        .unwrap_or_else(|| panic!("no status for {job} in {statuses:?}"))
}

#[test]
fn each_job_s_state_is_a_status() {
    let running = statuses(
        "ci",
        "push",
        &seen(
            &[
                ("a", "pending"),
                ("b", "ready"),
                ("c", "running"),
                ("d", "awaiting_approval"),
                ("e", "succeeded"),
                ("f", "failed"),
                ("g", "cancelled"),
            ],
            None,
        ),
    );
    assert_eq!(status_of(&running, "a"), ("pending", "queued".into()));
    assert_eq!(status_of(&running, "b"), ("pending", "queued".into()));
    assert_eq!(status_of(&running, "c"), ("pending", "running".into()));
    assert_eq!(
        status_of(&running, "d"),
        ("pending", "awaiting approval".into())
    );
    assert_eq!(status_of(&running, "e"), ("success", "succeeded".into()));
    assert_eq!(status_of(&running, "f"), ("failure", "failed".into()));
    assert_eq!(status_of(&running, "g"), ("error", "cancelled".into()));

    // What a run that ended left unfinished did not finish.
    let ended = statuses("ci", "push", &seen(&[("a", "running")], Some("failed")));
    assert_eq!(status_of(&ended, "a"), ("error", "did not finish".into()));
}

/// dagron skips a job whose upstream failed as it skips one whose `when:`
/// is false, so a skipped job passes only once the run has.
#[test]
fn a_skipped_job_waits_for_the_run() {
    let skipped = [("deploy", "skipped")];
    let (state, _) = status_of(&statuses("ci", "push", &seen(&skipped, None)), "deploy");
    assert_eq!(state, "pending");
    assert_eq!(
        status_of(
            &statuses("ci", "push", &seen(&skipped, Some("succeeded"))),
            "deploy"
        ),
        ("success", "skipped".into())
    );
    for run in ["failed", "cancelled"] {
        let (state, why) = status_of(
            &statuses("ci", "push", &seen(&skipped, Some(run))),
            "deploy",
        );
        assert_eq!(state, "error", "{run}");
        assert!(why.contains("did not succeed"), "{why}");
    }
}

/// A check is named for the pipeline, the job and the kind of event, never
/// the run: a later run of the same pipeline on the same kind of event
/// updates the checks the first one posted.
#[test]
fn checks_are_named_for_the_pipeline_job_and_event_never_the_run() {
    let first = statuses("ci", "push", &seen(&[("build", "running")], None));
    let rerun = statuses(
        "ci",
        "push",
        &seen(&[("build", "succeeded")], Some("succeeded")),
    );
    assert_eq!(
        first.keys().collect::<Vec<_>>(),
        rerun.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        first.keys().next().map(String::as_str),
        Some("dagron-ci/ci/build (push)")
    );
    let pr = statuses("ci", "pull_request", &seen(&[("build", "running")], None));
    assert_eq!(
        pr.keys().next().map(String::as_str),
        Some("dagron-ci/ci/build (pull_request)"),
        "a pull request's checks are its own"
    );
    // A merge queue waits, on the group's commit, for the checks branch
    // protection requires of a pull request.
    let group = statuses("ci", "merge_group", &seen(&[("build", "running")], None));
    assert_eq!(
        group.keys().collect::<Vec<_>>(),
        pr.keys().collect::<Vec<_>>()
    );
    assert_eq!(pipeline_name(".dagron/ci/ci.yaml"), "ci");
    assert_eq!(pipeline_name(".dagron/ci/release.yml"), "release");
    assert_eq!(pipeline_name("workflow:nightly"), "nightly");
}

// ── Against a dagron-api and a GitHub ────────────────────────────────────────

/// What GitHub answers once it has taken a number of statuses: a code, and
/// headers such as `retry-after`.
type Refusal = (usize, u16, Vec<(&'static str, &'static str)>);

/// A dagron-api that serves runs, and a GitHub that takes statuses, at one
/// address.
#[derive(Default)]
struct Stub {
    /// Each run's jobs, and its status once it finished.
    runs: BTreeMap<String, (Vec<Job>, Option<String>)>,
    /// Each run's failure summary, as `GET /api/runs/{id}` gives it.
    failures: BTreeMap<String, Value>,
    /// After this many statuses taken, GitHub answers with this.
    refuse_after: Option<Refusal>,
    /// A context GitHub takes no more statuses under: each is answered 422,
    /// in GitHub's words for a context that is full.
    refuse_context: Option<String>,
    /// How often it said so.
    context_refusals: usize,
    /// The statuses GitHub took: path, body, authorization.
    posts: Vec<(String, Value, String)>,
    /// The reads dagron-api answered: path and query, authorization.
    reads: Vec<(String, String)>,
    /// The App was not given `checks: write`: a mint that asks for it is
    /// refused.
    no_checks: bool,
    /// The permissions of each installation token minted. The n-th is
    /// `ghs_{n}`.
    minted: Vec<Value>,
    /// The check runs GitHub took: `POST` for a new one or `PATCH` for
    /// one brought up to date, its id, its body, and authorization.
    checks: Vec<(String, u64, Value, String)>,
    /// Each task's log, by its id: dagron-api answers a run's log only
    /// where one is set, as one from before there was a log to ask of.
    logs: BTreeMap<String, Vec<String>>,
    /// dagron-api does not answer for a log.
    logs_fail: bool,
    /// The App was not given `pull requests: write`: a mint that asks for it
    /// is refused.
    no_comments: bool,
    /// The comments GitHub took: `POST` for a new one or `PATCH` for one
    /// written again, its id, what it says, and authorization.
    comments: Vec<(String, u64, String, String)>,
    /// Comments someone deleted: a `PATCH` of one is answered 404.
    deleted: Vec<u64>,
    /// What GitHub answers a comment with and does not take it: a code, and
    /// headers such as `retry-after`.
    refuse_comments: Option<(u16, Vec<(&'static str, &'static str)>)>,
    /// The statuses GitLab took, for lab/gadgets: each body.
    gl_statuses: Vec<Value>,
    /// How many times each run was rerun whole, as its wait says it: a run
    /// with none set is answered as a dagron-api from before the count.
    whole_reruns: BTreeMap<String, i64>,
    /// A run dagron-api is slow to answer for, and how slow.
    slow: Option<(String, Duration)>,
    /// Each task's attempts, by its id, as dagron-api answers them.
    attempts: BTreeMap<String, Value>,
    /// The issues GitHub took: `POST` for a new one or `PATCH` for one
    /// written again, its number, its title, what it says, authorization.
    issues: Vec<(String, u64, String, String, String)>,
    /// Issues a person closed, and when: a `PATCH` of one says so.
    closed_issues: BTreeMap<u64, String>,
    /// The repository has its issues turned off: a new one is a `410`.
    issues_off: bool,
}

type Shared = Arc<Mutex<Stub>>;

async fn stub() -> (String, Shared) {
    async fn handle(
        State(st): State<Shared>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let path = uri.path().to_string();
        let slow = st.lock().unwrap().slow.clone();
        if let Some((run, wait)) = slow {
            if path.starts_with(&format!("/api/runs/{run}/")) {
                tokio::time::sleep(wait).await;
            }
        }
        let mut st = st.lock().unwrap();
        // GitLab's lab/gadgets: its statuses, and merge request 7's notes,
        // kept with the comments as GitHub's are.
        if let Some(rest) = path.strip_prefix("/projects/lab%2Fgadgets/") {
            let said: Value = serde_json::from_slice(&body).unwrap();
            if method == Method::POST && rest.starts_with("statuses/") {
                st.gl_statuses.push(said);
                return (StatusCode::CREATED, axum::Json(json!({}))).into_response();
            }
            let id = match (method.as_str(), rest.strip_prefix("merge_requests/7/notes")) {
                ("POST", Some("")) => {
                    st.comments.iter().filter(|(how, ..)| how == "POST").count() as u64 + 1
                }
                ("PUT", Some(id)) => id.trim_start_matches('/').parse().unwrap(),
                _ => return StatusCode::NOT_FOUND.into_response(),
            };
            if st.deleted.contains(&id) {
                return (
                    StatusCode::NOT_FOUND,
                    axum::Json(json!({ "message": "404 Not found" })),
                )
                    .into_response();
            }
            st.comments.push((
                method.to_string(),
                id,
                said["body"].as_str().unwrap().to_string(),
                auth,
            ));
            return (StatusCode::CREATED, axum::Json(json!({ "id": id }))).into_response();
        }
        if path == "/repos/acme/widgets/installation" {
            return axum::Json(json!({ "id": 77 })).into_response();
        }
        if path == "/app/installations/77/access_tokens" {
            let asked: Value = serde_json::from_slice(&body).unwrap();
            let comments = asked["permissions"]["pull_requests"] == "write";
            if (st.no_checks && asked["permissions"].get("checks").is_some())
                || (st.no_comments && comments)
            {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    axum::Json(json!({ "message": "The permissions requested are not granted to this installation." })),
                )
                    .into_response();
            }
            st.minted.push(asked["permissions"].clone());
            let token = format!("ghs_{}", st.minted.len());
            return (
                StatusCode::CREATED,
                axum::Json(json!({ "token": token, "expires_at": "2099-01-01T00:00:00Z" })),
            )
                .into_response();
        }
        if let Some(rest) = path.strip_prefix("/repos/acme/widgets/check-runs") {
            let check: Value = serde_json::from_slice(&body).unwrap();
            let id = match rest.strip_prefix('/') {
                Some(id) => id.parse().unwrap(),
                None => st.checks.iter().filter(|(how, ..)| how == "POST").count() as u64 + 1,
            };
            st.checks.push((method.to_string(), id, check, auth));
            return (StatusCode::CREATED, axum::Json(json!({ "id": id }))).into_response();
        }
        // An issue of a job that flakes: a new one, or one written again.
        if st.issues_off && method == Method::POST && path == "/repos/acme/widgets/issues" {
            return StatusCode::GONE.into_response();
        }
        let number = match path.strip_prefix("/repos/acme/widgets/issues") {
            Some("") if method == Method::POST => {
                Some(st.issues.iter().filter(|(how, ..)| how == "POST").count() as u64 + 1)
            }
            Some(rest) if method == Method::PATCH => rest
                .strip_prefix('/')
                .filter(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
                .map(|n| n.parse().unwrap()),
            _ => None,
        };
        if let Some(number) = number {
            let said: Value = serde_json::from_slice(&body).unwrap();
            st.issues.push((
                method.to_string(),
                number,
                said["title"].as_str().unwrap_or("").to_string(),
                said["body"].as_str().unwrap().to_string(),
                auth,
            ));
            let closed = st.closed_issues.get(&number).cloned();
            return axum::Json(json!({
                "number": number,
                "state": if closed.is_some() { "closed" } else { "open" },
                "closed_at": closed,
            }))
            .into_response();
        }
        // A pull request's conversation is an issue's.
        if let Some(rest) = path.strip_prefix("/repos/acme/widgets/issues/") {
            let said: Value = serde_json::from_slice(&body).unwrap();
            if let Some((code, extra)) = &st.refuse_comments {
                let mut res = StatusCode::from_u16(*code).unwrap().into_response();
                for (name, value) in extra {
                    res.headers_mut().insert(*name, value.parse().unwrap());
                }
                return res;
            }
            let id = match (method.as_str(), rest.strip_prefix("comments/")) {
                ("PATCH", Some(id)) => id.parse().unwrap(),
                ("POST", None) if rest == "7/comments" => {
                    st.comments.iter().filter(|(how, ..)| how == "POST").count() as u64 + 1
                }
                _ => return StatusCode::NOT_FOUND.into_response(),
            };
            if st.deleted.contains(&id) {
                return (
                    StatusCode::NOT_FOUND,
                    axum::Json(json!({ "message": "Not Found" })),
                )
                    .into_response();
            }
            let text = said["body"].as_str().unwrap().to_string();
            st.comments.push((method.to_string(), id, text, auth));
            return axum::Json(json!({ "id": id })).into_response();
        }
        if let Some(failure) = path
            .strip_prefix("/api/runs/")
            .filter(|id| !id.contains('/'))
            .and_then(|id| st.failures.get(id))
        {
            return axum::Json(json!({ "status": "failed", "failure": failure })).into_response();
        }
        if method == Method::GET {
            let Some((id, what)) = path
                .strip_prefix("/api/runs/")
                .and_then(|rest| rest.split_once('/'))
            else {
                return StatusCode::NOT_FOUND.into_response();
            };
            st.reads.push((uri.to_string(), auth));
            return match (st.runs.get(id), what) {
                (Some((jobs, _)), "graph") => axum::Json(json!({
                    "nodes": jobs.iter().map(|j| json!({"id": format!("t-{}", j.name), "name": j.name, "status": j.status, "attempt": j.attempt})).collect::<Vec<_>>(),
                    "edges": [],
                }))
                .into_response(),
                // As dagron-api answers for a run it does not have.
                (None, "graph") => axum::Json(json!({"nodes": [], "edges": []})).into_response(),
                (Some(_), what) if what.starts_with("tasks/") && what.ends_with("/attempts") => {
                    let task = what.trim_start_matches("tasks/").trim_end_matches("/attempts");
                    match st.attempts.get(task) {
                        Some(attempts) => axum::Json(attempts.clone()).into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                }
                (Some((_, finished)), "wait") => {
                    let mut wait = json!({
                        "run_id": id,
                        "status": finished.clone().unwrap_or_else(|| "running".into()),
                        "finished": finished.is_some(),
                    });
                    if let Some(n) = st.whole_reruns.get(id) {
                        wait["whole_reruns"] = json!(n);
                    }
                    axum::Json(wait).into_response()
                }
                (Some(_), "logs") if st.logs_fail => {
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
                // As dagron-api answers: the lines of the task asked for
                // that hold what was asked for, the last of them.
                (Some(_), "logs") if !st.logs.is_empty() => {
                    let asked: BTreeMap<String, String> =
                        reqwest::Url::parse(&format!("http://cell{uri}"))
                            .unwrap()
                            .query_pairs()
                            .into_owned()
                            .collect();
                    let limit: usize = asked["limit"].parse().unwrap();
                    assert_eq!((asked["tail"].as_str(), asked["case"].as_str()), ("1", "1"));
                    let lines: Vec<Value> = st
                        .logs
                        .get(&asked["task"])
                        .into_iter()
                        .flatten()
                        .filter(|line| line.contains(asked["q"].as_str()))
                        .map(|line| json!({ "task_id": asked["task"], "text": line, "matched": true }))
                        .collect();
                    let from = lines.len().saturating_sub(limit);
                    axum::Json(json!({ "run_id": id, "lines": lines[from..] })).into_response()
                }
                _ => StatusCode::NOT_FOUND.into_response(),
            };
        }
        if method == Method::POST && path.starts_with("/repos/acme/widgets/statuses/") {
            let status: Value = serde_json::from_slice(&body).unwrap();
            if st.refuse_context.is_some()
                && st.refuse_context.as_deref() == status["context"].as_str()
            {
                st.context_refusals += 1;
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    axum::Json(json!({
                        "message": "Validation Failed",
                        "errors": [{ "resource": "Status", "code": "custom",
                            "message": "This SHA and context has reached the maximum number of statuses." }],
                    })),
                )
                    .into_response();
            }
            if let Some((after, code, extra)) = &st.refuse_after {
                if st.posts.len() >= *after {
                    let mut res = StatusCode::from_u16(*code).unwrap().into_response();
                    for (name, value) in extra {
                        res.headers_mut().insert(*name, value.parse().unwrap());
                    }
                    return res;
                }
            }
            st.posts
                .push((path, serde_json::from_slice(&body).unwrap(), auth));
            return (StatusCode::CREATED, axum::Json(json!({}))).into_response();
        }
        StatusCode::NOT_FOUND.into_response()
    }
    let st: Shared = Arc::default();
    let app = Router::new().fallback(handle).with_state(st.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (origin, st)
}

fn reporter(db: &Db, origin: &str, token: Option<&str>) -> (Reporter, Arc<Metrics>) {
    let metrics = Arc::new(Metrics::default());
    let reporter = Reporter::new(
        db.clone(),
        BTreeMap::from([("default".to_string(), origin.to_string())]),
        "dgr_token".into(),
        Arc::new(Forge::new(token.map(str::to_string)).with_origin(origin)),
        None,
        metrics.clone(),
    );
    (reporter, metrics)
}

/// A push to acme/widgets, and `run_id` recorded for it, as a worker records
/// the run it submits.
async fn recorded(db: &Db, run_id: &str) -> Claimed {
    let (_, ev) = claimed(db, None, Kind::Push, Trust::Trusted).await;
    record(db, &ev, run_id).await;
    ev
}

async fn record(db: &Db, ev: &Claimed, run_id: &str) {
    let run = SubmittedRun {
        run_id: run_id.into(),
        event_id: ev.id.clone(),
        repo_id: ev.repo.id.clone(),
        cell: "default".into(),
        pipeline: ".dagron/ci/ci.yaml".into(),
        idempotency_key: format!("key-{run_id}"),
        on_failed: false,
    };
    assert!(db.record_run(&run).await.unwrap());
}

fn set_run(st: &Shared, run_id: &str, jobs: &[(&str, &str)], finished: Option<&str>) {
    st.lock().unwrap().runs.insert(
        run_id.into(),
        (
            jobs.iter().map(|(n, s)| job(n, s)).collect(),
            finished.map(str::to_string),
        ),
    );
}

/// The reporter's columns of one run: whether it is due again, its status,
/// what was posted, and why not.
#[derive(Debug)]
struct Row {
    due: bool,
    status: String,
    reported: BTreeMap<String, String>,
    error: Option<String>,
}

async fn row(db: &Db, run_id: &str) -> Row {
    let (due, status, reported, error): (Option<String>, String, String, Option<String>) =
        match db {
            Db::Sqlite(p) => sqlx::query_as(
                "SELECT report_due_at, status, reported, report_error FROM ci_runs WHERE run_id = ?1",
            )
            .bind(run_id)
            .fetch_one(p)
            .await
            .unwrap(),
            Db::Postgres(p) => sqlx::query_as(
                "SELECT report_due_at::TEXT, status, reported, report_error FROM ci_runs WHERE run_id = $1",
            )
            .bind(run_id)
            .fetch_one(p)
            .await
            .unwrap(),
        };
    Row {
        due: due.is_some(),
        status,
        reported: serde_json::from_str(&reported).unwrap(),
        error,
    }
}

/// Make `run_id` due now, as the passing of its interval would.
async fn make_due(db: &Db, run_id: &str) {
    let rows = match db {
        Db::Sqlite(p) => sqlx::query(
            "UPDATE ci_runs SET report_due_at = '2000-01-01T00:00:00.000Z' WHERE run_id = ?1",
        )
        .bind(run_id)
        .execute(p)
        .await
        .unwrap()
        .rows_affected(),
        Db::Postgres(p) => sqlx::query(
            "UPDATE ci_runs SET report_due_at = now() - interval '1 second' WHERE run_id = $1",
        )
        .bind(run_id)
        .execute(p)
        .await
        .unwrap()
        .rows_affected(),
    };
    assert_eq!(rows, 1);
}

fn posted(st: &Shared) -> Vec<(String, String, String)> {
    st.lock()
        .unwrap()
        .posts
        .iter()
        .map(|(_, body, _)| {
            (
                body["context"].as_str().unwrap().to_string(),
                body["state"].as_str().unwrap().to_string(),
                body["description"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

/// A run's jobs go up as pending at its first look, each changes only when
/// it does, and the run is left once its outcomes are posted.
async fn the_status_contract(db: &Db) {
    let (origin, st) = stub().await;
    let (reporter, metrics) = reporter(db, &origin, Some("ghp_test"));
    let ev = recorded(db, "run-1").await;
    set_run(
        &st,
        "run-1",
        &[("build", "running"), ("test", "pending")],
        None,
    );

    assert!(
        reporter.report_one("a").await.unwrap(),
        "a recorded run is due at once"
    );
    assert_eq!(
        posted(&st),
        vec![
            (
                "dagron-ci/ci/build (push)".into(),
                "pending".into(),
                "running".into()
            ),
            (
                "dagron-ci/ci/test (push)".into(),
                "pending".into(),
                "queued".into()
            ),
        ]
    );
    {
        let st = st.lock().unwrap();
        let (path, _, auth) = &st.posts[0];
        assert_eq!(path, &format!("/repos/acme/widgets/statuses/{}", ev.sha));
        assert_eq!(auth, "Bearer ghp_test");
        assert!(
            st.reads.iter().all(|(_, auth)| auth == "Bearer dgr_token"),
            "dagron-api is read with dagron-ci's token: {:?}",
            st.reads
        );
        assert!(
            st.reads.iter().all(|(path, _)| path.ends_with("/graph")),
            "a running run's status is not read: {:?}",
            st.reads
        );
    }
    let r = row(db, "run-1").await;
    assert!(r.due && r.error.is_none(), "{r:?}");
    assert_eq!(r.reported.len(), 2);
    assert!(
        !reporter.report_one("a").await.unwrap(),
        "not due again until its interval passes"
    );

    // Nothing changed: nothing is posted.
    make_due(db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(posted(&st).len(), 2);

    set_run(
        &st,
        "run-1",
        &[("build", "succeeded"), ("test", "failed")],
        Some("failed"),
    );
    make_due(db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        posted(&st)[2..],
        [
            (
                "dagron-ci/ci/build (push)".into(),
                "success".into(),
                "succeeded".into()
            ),
            (
                "dagron-ci/ci/test (push)".into(),
                "failure".into(),
                "failed".into()
            ),
        ]
    );
    let r = row(db, "run-1").await;
    assert!(!r.due, "its statuses are final: {r:?}");
    assert_eq!(r.status, "failed");
    assert_eq!(r.reported["dagron-ci/ci/test (push)"], "failure:failed");
    assert!(!reporter.report_one("a").await.unwrap(), "and it is left");
    assert!(metrics
        .render(None, &[])
        .contains("dagron_ci_commit_statuses_total{outcome=\"posted\"} 4"));
}

#[tokio::test]
async fn a_run_s_jobs_are_pending_then_their_outcomes() {
    let (db, _dir) = sqlite().await;
    the_status_contract(&db).await;
}

#[tokio::test]
async fn a_run_s_jobs_are_pending_then_their_outcomes_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    the_status_contract(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;

    // Each contract connects its repository: a schema of its own.
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    the_claim_contract(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

#[tokio::test]
async fn a_cancelled_run_turns_its_checks_to_cancelled() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(&db, &origin, Some("ghp_test"));
    recorded(&db, "run-1").await;
    set_run(
        &st,
        "run-1",
        &[
            ("lint", "succeeded"),
            ("test", "cancelled"),
            ("deploy", "skipped"),
        ],
        Some("cancelled"),
    );
    assert!(reporter.report_one("a").await.unwrap());
    let posted = posted(&st);
    assert_eq!(posted.len(), 3);
    assert!(posted.contains(&(
        "dagron-ci/ci/lint (push)".into(),
        "success".into(),
        "succeeded".into()
    )));
    assert!(posted.contains(&(
        "dagron-ci/ci/test (push)".into(),
        "error".into(),
        "cancelled".into()
    )));
    assert!(posted.contains(&(
        "dagron-ci/ci/deploy (push)".into(),
        "error".into(),
        "skipped: an earlier job did not succeed".into()
    )));
    let r = row(&db, "run-1").await;
    assert_eq!((r.due, r.status.as_str()), (false, "cancelled"));
}

/// A run that runs again is followed again. Its record re-opened, the job that
/// is run again reads `pending` and then how it ends, under the context it
/// had; a job that was not run again is left as it was posted; and the run's
/// second end is recorded as its first was.
#[tokio::test]
async fn a_reopened_run_s_checks_read_pending_then_how_it_ends_under_the_same_contexts() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(&db, &origin, Some("ghp_test"));
    recorded(&db, "run-1").await;
    set_run(
        &st,
        "run-1",
        &[("lint", "succeeded"), ("test", "failed")],
        Some("failed"),
    );
    assert!(reporter.report_one("a").await.unwrap());
    let r = row(&db, "run-1").await;
    assert_eq!((r.due, r.status.as_str()), (false, "failed"));
    assert!(!reporter.report_one("a").await.unwrap(), "over: it is left");

    // Rerun from its failed job, and seen running.
    set_run(
        &st,
        "run-1",
        &[("lint", "succeeded"), ("test", "running")],
        None,
    );
    assert!(db.reopen_run("run-1", Duration::ZERO, None).await.unwrap());
    let r = row(&db, "run-1").await;
    assert_eq!((r.due, r.status.as_str()), (true, "submitted"));
    let before = posted(&st).len();
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        posted(&st)[before..],
        [(
            "dagron-ci/ci/test (push)".to_string(),
            "pending".to_string(),
            "running".to_string()
        )],
        "the job run again, under its own context; the other is as it was posted"
    );

    set_run(
        &st,
        "run-1",
        &[("lint", "succeeded"), ("test", "succeeded")],
        Some("succeeded"),
    );
    make_due(&db, "run-1").await;
    let before = posted(&st).len();
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        posted(&st)[before..],
        [(
            "dagron-ci/ci/test (push)".to_string(),
            "success".to_string(),
            "succeeded".to_string()
        )]
    );
    let r = row(&db, "run-1").await;
    assert_eq!(
        (r.due, r.status.as_str()),
        (false, "succeeded"),
        "its second end, recorded"
    );
}

/// A rerun can be over by the reporter's next look, having failed as it did
/// before. Its job then reads as it was last posted, and used to be left: the
/// commit kept the first attempt's status, with nothing to say it was run
/// again. It is a new result, and is posted; the job that had succeeded, which
/// a rerun keeps, is not.
#[tokio::test]
async fn a_rerun_that_fails_the_same_way_before_the_next_look_is_posted_again() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(&db, &origin, Some("ghp_test"));
    recorded(&db, "run-1").await;
    set_run(
        &st,
        "run-1",
        &[("lint", "succeeded"), ("test", "failed")],
        Some("failed"),
    );
    assert!(reporter.report_one("a").await.unwrap());
    let failed = (
        "dagron-ci/ci/test (push)".to_string(),
        "failure".to_string(),
        "failed".to_string(),
    );

    // Seen running again, and failed again before it is looked at.
    assert!(db.reopen_run("run-1", Duration::ZERO, None).await.unwrap());
    let before = posted(&st).len();
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        posted(&st)[before..],
        [failed.clone()],
        "its failure, again"
    );
    let r = row(&db, "run-1").await;
    assert_eq!((r.due, r.status.as_str()), (false, "failed"));

    // Once for each time it ran again, and not once a look.
    assert!(db.reopen_run("run-1", Duration::ZERO, None).await.unwrap());
    set_run(
        &st,
        "run-1",
        &[("lint", "succeeded"), ("test", "failed")],
        None,
    );
    let before = posted(&st).len();
    assert!(reporter.report_one("a").await.unwrap());
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        posted(&st)[before..],
        [failed],
        "once, however often it is looked at"
    );
}

/// Refused once, not asked again: nothing more is posted for the run, which
/// is still followed to its end, so that its end is recorded.
#[tokio::test]
async fn a_refused_status_is_not_asked_again_and_the_run_still_ends() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, metrics) = reporter(&db, &origin, Some("ghp_test"));
    recorded(&db, "run-1").await;
    set_run(&st, "run-1", &[("build", "running")], None);
    st.lock().unwrap().refuse_after = Some((0, 403, vec![]));
    let refused_once = || {
        metrics
            .render(None, &[])
            .contains("dagron_ci_commit_statuses_total{outcome=\"refused\"} 1\n")
    };
    assert!(reporter.report_one("a").await.unwrap());
    let r = row(&db, "run-1").await;
    assert!(r.due, "still followed: {r:?}");
    assert_eq!(r.status, "submitted");
    assert!(
        r.error.as_deref().unwrap().contains("statuses: write"),
        "{r:?}"
    );
    assert!(refused_once());

    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    let r = row(&db, "run-1").await;
    assert!(r.due, "{r:?}");
    assert!(
        r.error.as_deref().unwrap().contains("statuses: write"),
        "why stays said: {r:?}"
    );
    assert!(refused_once(), "not asked again");

    set_run(&st, "run-1", &[("build", "succeeded")], Some("succeeded"));
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    let r = row(&db, "run-1").await;
    assert_eq!((r.due, r.status.as_str()), (false, "succeeded"), "{r:?}");
    assert!(refused_once(), "nor at its end");
    assert!(posted(&st).is_empty());
}

/// A status GitHub refuses for its context alone (it keeps a thousand under
/// one context on a commit, and then no more) stops that context, and not the
/// run's others. It used to be taken for a refusal of the commit, and the
/// run's other jobs then said nothing either.
#[tokio::test]
async fn a_status_refused_for_its_context_alone_does_not_stop_the_run_s_others() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, metrics) = reporter(&db, &origin, Some("ghp_test"));
    recorded(&db, "run-1").await;
    const FULL: &str = "dagron-ci/ci/build (push)";
    const OTHER: &str = "dagron-ci/ci/ship (push)";
    st.lock().unwrap().refuse_context = Some(FULL.into());
    let asked = || st.lock().unwrap().context_refusals;
    let said = |state: &str, description: &str| {
        (
            OTHER.to_string(),
            state.to_string(),
            description.to_string(),
        )
    };

    set_run(
        &st,
        "run-1",
        &[("build", "running"), ("ship", "pending")],
        None,
    );
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        posted(&st),
        vec![said("pending", "queued")],
        "the other job's goes on"
    );
    assert_eq!(asked(), 1);
    let r = row(&db, "run-1").await;
    assert!(r.due, "{r:?}");
    assert!(
        r.error
            .as_deref()
            .is_some_and(|e| e.contains(FULL) && e.contains("HTTP 422")),
        "the record says which context, and why: {r:?}"
    );

    // The full context is left alone from here, whatever its job does next.
    set_run(
        &st,
        "run-1",
        &[("build", "failed"), ("ship", "skipped")],
        Some("failed"),
    );
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        posted(&st),
        vec![
            said("pending", "queued"),
            said("error", "skipped: an earlier job did not succeed"),
        ]
    );
    assert_eq!(asked(), 1, "not asked again under the full context");
    let r = row(&db, "run-1").await;
    assert_eq!((r.due, r.status.as_str()), (false, "failed"), "{r:?}");
    assert!(
        r.error.as_deref().is_some_and(|e| e.contains(FULL)),
        "why stays said: {r:?}"
    );
    assert!(metrics
        .render(None, &[])
        .contains("dagron_ci_commit_statuses_total{outcome=\"refused\"} 1\n"));
}

/// GitHub's rate limit is waited out as GitHub asks, and what was posted
/// before it is kept, so the next look posts only the rest.
#[tokio::test]
async fn a_rate_limit_is_waited_out_and_what_was_posted_is_kept() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(&db, &origin, Some("ghp_test"));
    recorded(&db, "run-1").await;
    set_run(&st, "run-1", &[("a", "running"), ("b", "running")], None);
    st.lock().unwrap().refuse_after = Some((1, 403, vec![("retry-after", "120")]));
    assert!(reporter.report_one("a").await.unwrap());
    let r = row(&db, "run-1").await;
    assert!(r.due, "{r:?}");
    assert!(
        r.error.as_deref().unwrap().contains("retry after 120"),
        "{r:?}"
    );
    assert_eq!(r.reported.len(), 1, "the one posted is kept");
    assert!(
        !reporter.report_one("a").await.unwrap(),
        "not looked at again before GitHub allows"
    );

    st.lock().unwrap().refuse_after = None;
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(posted(&st).len(), 2, "the rest, and only the rest");
    assert!(row(&db, "run-1").await.error.is_none());
}

/// A job's context that another event is saying skipped under a lease
/// (`Db::lease_context`) is left for the run's next look, soon though the
/// run is over, so that what the run says goes up after and stands. Its
/// other contexts go up at once. Once the run owns the context, no lease is
/// had of it.
async fn the_lease_contract(db: &Db) {
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(db, &origin, Some("ghp_test"));
    let ev = recorded(db, "run-1").await;
    set_run(
        &st,
        "run-1",
        &[("build", "failed"), ("test", "succeeded")],
        Some("failed"),
    );
    let (build, test) = ("dagron-ci/ci/build (push)", "dagron-ci/ci/test (push)");
    let minute = Duration::from_secs(60);
    assert!(db
        .lease_context(&ev.repo.id, &ev.sha, build, "ev-other", minute)
        .await
        .unwrap());
    let said = |context: &str, state: &str, description: &str| {
        (
            context.to_string(),
            state.to_string(),
            description.to_string(),
        )
    };

    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(posted(&st), [said(test, "success", "succeeded")]);
    let r = row(db, "run-1").await;
    assert!(r.due, "looked at again for the one it left: {r:?}");
    assert_eq!(r.status, "failed");
    assert!(!r.reported.contains_key(build), "{r:?}");
    // While the lease is in force, the run waits on.
    make_due(db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(posted(&st).len(), 1);

    // Given back: the run owns the context and posts under it, and it is
    // the run's from then on.
    db.release_context(&ev.repo.id, &ev.sha, build, "ev-other")
        .await
        .unwrap();
    make_due(db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        posted(&st),
        [
            said(test, "success", "succeeded"),
            said(build, "failure", "failed")
        ]
    );
    let r = row(db, "run-1").await;
    assert!(!r.due, "over, with all it has posted: {r:?}");
    assert!(!db
        .lease_context(&ev.repo.id, &ev.sha, build, "ev-other", minute)
        .await
        .unwrap());
    assert!(!db
        .lease_context(&ev.repo.id, &ev.sha, test, "ev-other", minute)
        .await
        .unwrap());
}

/// Whether `run_id` is next due more than `secs` from now.
async fn due_after(db: &Db, run_id: &str, secs: i64) -> bool {
    match db {
        Db::Sqlite(p) => sqlx::query_scalar(
            "SELECT report_due_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds') \
             FROM ci_runs WHERE run_id = ?1",
        )
        .bind(run_id)
        .bind(secs)
        .fetch_one(p)
        .await
        .unwrap(),
        Db::Postgres(p) => sqlx::query_scalar(
            "SELECT report_due_at > now() + make_interval(secs => $2) FROM ci_runs WHERE run_id = $1",
        )
        .bind(run_id)
        .bind(secs as f64)
        .fetch_one(p)
        .await
        .unwrap(),
    }
}

/// A context left for a lease is looked at again soon, but not sooner than
/// a rate limit met at the same look allows: the forge is not asked again
/// before it said to.
#[tokio::test]
async fn a_context_left_for_a_lease_keeps_a_rate_limit_s_wait() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(&db, &origin, Some("ghp_test"));
    let ev = recorded(&db, "run-1").await;
    set_run(
        &st,
        "run-1",
        &[("build", "failed"), ("test", "succeeded")],
        Some("failed"),
    );
    let (build, minute) = ("dagron-ci/ci/build (push)", Duration::from_secs(60));
    assert!(db
        .lease_context(&ev.repo.id, &ev.sha, build, "ev-other", minute)
        .await
        .unwrap());
    st.lock().unwrap().refuse_after = Some((0, 403, vec![("retry-after", "120")]));

    assert!(reporter.report_one("a").await.unwrap());
    let r = row(&db, "run-1").await;
    assert!(r.due, "{r:?}");
    assert!(
        r.error.as_deref().unwrap().contains("retry after 120"),
        "{r:?}"
    );
    assert!(
        due_after(&db, "run-1", 60).await,
        "the rate limit's wait is kept"
    );
}

#[tokio::test]
async fn a_context_another_event_is_saying_skipped_is_posted_after_it() {
    let (db, _dir) = sqlite().await;
    the_lease_contract(&db).await;
}

#[tokio::test]
async fn a_context_another_event_is_saying_skipped_is_posted_after_it_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    the_lease_contract(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

#[tokio::test]
async fn a_failing_github_is_tried_again_later() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, metrics) = reporter(&db, &origin, Some("ghp_test"));
    recorded(&db, "run-1").await;
    set_run(&st, "run-1", &[("build", "succeeded")], Some("succeeded"));
    st.lock().unwrap().refuse_after = Some((0, 502, vec![]));
    assert!(reporter.report_one("a").await.unwrap());
    let r = row(&db, "run-1").await;
    assert!(
        r.due,
        "a finished run whose statuses did not go is looked at again: {r:?}"
    );
    assert!(r.error.as_deref().unwrap().contains("HTTP 502"), "{r:?}");
    assert!(metrics
        .render(None, &[])
        .contains("dagron_ci_commit_statuses_total{outcome=\"failed\"} 1"));
}

/// Nothing will finish a run dagron-api no longer has: it is over, and left.
#[tokio::test]
async fn a_run_dagron_api_does_not_have_is_over() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(&db, &origin, Some("ghp_test"));
    recorded(&db, "run-gone").await;
    assert!(reporter.report_one("a").await.unwrap());
    let r = row(&db, "run-gone").await;
    assert_eq!((r.due, r.status.as_str()), (false, GONE), "{r:?}");
    assert!(
        r.error.as_deref().unwrap().contains("has no run run-gone"),
        "{r:?}"
    );
    assert!(posted(&st).is_empty());
}

/// A run's status moves once: one whose last statuses were still being
/// retried when dagron-api stopped having it keeps how it ended.
async fn how_a_run_ended_is_kept(db: &Db) {
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(db, &origin, Some("ghp_test"));
    recorded(db, "run-1").await;
    set_run(&st, "run-1", &[("build", "succeeded")], Some("succeeded"));
    st.lock().unwrap().refuse_after = Some((0, 502, vec![]));
    assert!(reporter.report_one("a").await.unwrap());
    let r = row(db, "run-1").await;
    assert_eq!((r.due, r.status.as_str()), (true, "succeeded"), "{r:?}");

    st.lock().unwrap().runs.remove("run-1");
    make_due(db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    let r = row(db, "run-1").await;
    assert_eq!((r.due, r.status.as_str()), (false, "succeeded"), "{r:?}");
}

#[tokio::test]
async fn how_a_run_ended_is_not_rewritten() {
    let (db, _dir) = sqlite().await;
    how_a_run_ended_is_kept(&db).await;
}

#[tokio::test]
async fn how_a_run_ended_is_not_rewritten_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    how_a_run_ended_is_kept(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

/// A repository that takes no statuses gets none, and is told why; its run is
/// still followed to its end, which the checkout-token endpoint reads.
#[tokio::test]
async fn a_repository_that_takes_no_statuses_is_followed_to_its_end_with_why() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(&db, &origin, None);
    recorded(&db, "run-1").await;
    set_run(&st, "run-1", &[("build", "running")], None);
    assert!(reporter.report_one("a").await.unwrap());
    let r = row(&db, "run-1").await;
    assert!(r.due, "{r:?}");
    assert_eq!(r.status, "submitted");
    assert!(
        r.error.as_deref().unwrap().contains("CI_GITHUB_TOKEN_FILE"),
        "{r:?}"
    );

    set_run(&st, "run-1", &[("build", "failed")], Some("failed"));
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    let r = row(&db, "run-1").await;
    assert_eq!((r.due, r.status.as_str()), (false, "failed"), "{r:?}");
    let st = st.lock().unwrap();
    assert!(!st.reads.is_empty(), "the run is read");
    assert!(st.posts.is_empty(), "and nothing is posted for it");
}

#[tokio::test]
async fn a_status_links_to_the_run_s_page() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let reporter = Reporter::new(
        db.clone(),
        BTreeMap::from([("default".to_string(), origin.clone())]),
        "dgr_token".into(),
        Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
        Some("https://ci.example.com/{cell}/runs/{run_id}".into()),
        Arc::new(Metrics::default()),
    );
    recorded(&db, "run-1").await;
    set_run(&st, "run-1", &[("build", "running")], None);
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        st.lock().unwrap().posts[0].1["target_url"],
        "https://ci.example.com/default/runs/run-1"
    );
}

/// A claim holds its run for the lease, and only the reporter holding it
/// writes; a run recorded before statuses existed is never claimed.
async fn the_claim_contract(db: &Db) {
    let (_, ev) = claimed(db, None, Kind::PullRequest, Trust::Trusted).await;
    record(db, &ev, "run-claim").await;
    let (run_id, due) = db
        .claim_report("a", Duration::from_secs(60))
        .await
        .unwrap()
        .expect("due at once");
    assert_eq!(run_id, "run-claim");
    let due = due.unwrap();
    assert_eq!(
        (due.kind.as_str(), due.cell.as_str()),
        ("pull_request", "default")
    );
    assert_eq!(due.repo.full_name, "acme/widgets");
    assert!(
        db.claim_report("b", Duration::from_secs(60))
            .await
            .unwrap()
            .is_none(),
        "held for the lease"
    );
    let done = ReportWrite::default();
    assert!(
        !db.finish_report("run-claim", "b", &done).await.unwrap(),
        "fenced"
    );
    assert!(db.finish_report("run-claim", "a", &done).await.unwrap());
    assert!(
        db.claim_report("a", Duration::from_secs(60))
            .await
            .unwrap()
            .is_none(),
        "finished with no next look: never due again"
    );
}

#[tokio::test]
async fn a_claim_is_held_and_fenced() {
    let (db, _dir) = sqlite().await;
    the_claim_contract(&db).await;
}

#[tokio::test]
async fn a_run_recorded_before_statuses_is_never_claimed() {
    let (db, _dir) = sqlite().await;
    recorded(&db, "run-old").await;
    let Db::Sqlite(p) = &db else { unreachable!() };
    sqlx::query("UPDATE ci_runs SET report_due_at = NULL")
        .execute(p)
        .await
        .unwrap();
    let (origin, _st) = stub().await;
    let (reporter, _) = reporter(&db, &origin, Some("ghp_test"));
    assert!(!reporter.report_one("a").await.unwrap());
}

/// A meter that keeps what it is told, and refuses an end while `refuse` is
/// above 0, counting it down.
#[derive(Default)]
pub(crate) struct StubMeter {
    pub(crate) submitted: Mutex<Vec<crate::meter::MeteredRun>>,
    pub(crate) ended: Mutex<Vec<crate::meter::EndedRun>>,
    pub(crate) refuse: Mutex<usize>,
}

impl crate::meter::RunMeter for StubMeter {
    fn submitted<'a>(&'a self, run: &'a crate::meter::MeteredRun) -> crate::meter::MeterFuture<'a> {
        Box::pin(async move {
            self.submitted.lock().unwrap().push(run.clone());
            Ok(())
        })
    }

    fn ended<'a>(&'a self, run: &'a crate::meter::EndedRun) -> crate::meter::MeterFuture<'a> {
        Box::pin(async move {
            let mut refuse = self.refuse.lock().unwrap();
            if *refuse > 0 {
                *refuse -= 1;
                return Err("the meter is down".into());
            }
            self.ended.lock().unwrap().push(run.clone());
            Ok(())
        })
    }
}

/// A pass of the meter, as [`Reporter::meter_ends`] says it.
fn pass(taken: usize, refused: bool) -> super::MeterPass {
    super::MeterPass { taken, refused }
}

/// Whether `run_id`'s end is due to the meter.
async fn meter_due(db: &Db, run_id: &str) -> bool {
    let due: Option<String> = match db {
        Db::Sqlite(p) => sqlx::query_scalar("SELECT meter_due_at FROM ci_runs WHERE run_id = ?1")
            .bind(run_id)
            .fetch_one(p)
            .await
            .unwrap(),
        Db::Postgres(p) => {
            sqlx::query_scalar("SELECT meter_due_at::TEXT FROM ci_runs WHERE run_id = $1")
                .bind(run_id)
                .fetch_one(p)
                .await
                .unwrap()
        }
    };
    due.is_some()
}

/// Make every end due to the meter due now, as the lease running out would.
async fn meter_due_now(db: &Db) {
    crate::db::tests::exec(
        db,
        "UPDATE ci_runs SET meter_due_at = '2000-01-01T00:00:00Z' WHERE meter_due_at IS NOT NULL",
    )
    .await;
}

fn set_run_at(st: &Shared, run_id: &str, jobs: &[(&str, &str, i64)], finished: Option<&str>) {
    st.lock().unwrap().runs.insert(
        run_id.into(),
        (
            jobs.iter()
                .map(|(n, s, a)| Job {
                    id: format!("t-{n}"),
                    name: n.to_string(),
                    status: s.to_string(),
                    attempt: *a,
                })
                .collect(),
            finished.map(str::to_string),
        ),
    );
}

/// Each run's end is told to the meter once it is recorded, with how it
/// ended and whether any of its jobs started; told again while the meter
/// refuses it, and not once it took it.
async fn the_meter_contract(db: &Db) {
    let (origin, st) = stub().await;
    let meter = Arc::new(StubMeter::default());
    let (reporter, _) = reporter(db, &origin, Some("ghp_test"));
    let reporter = reporter.with_meter(meter.clone());

    // Running: nothing is due yet.
    recorded(db, "run-1").await;
    set_run_at(
        &st,
        "run-1",
        &[("build", "running", 1), ("test", "pending", 0)],
        None,
    );
    assert!(reporter.report_one("a").await.unwrap());
    assert!(!meter_due(db, "run-1").await);
    assert_eq!(reporter.meter_ends().await.unwrap(), pass(0, false));

    // Over: due in the same write, and told once.
    set_run_at(
        &st,
        "run-1",
        &[("build", "succeeded", 1), ("test", "succeeded", 1)],
        Some("succeeded"),
    );
    make_due(db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert!(meter_due(db, "run-1").await);
    assert_eq!(reporter.meter_ends().await.unwrap(), pass(1, false));
    assert!(!meter_due(db, "run-1").await);
    let told = meter.ended.lock().unwrap().clone();
    assert_eq!(told.len(), 1);
    assert_eq!(
        told[0],
        crate::meter::EndedRun {
            run: crate::meter::MeteredRun {
                run_id: "run-1".into(),
                cell: "default".into(),
                repo: "github.com/acme/widgets".into(),
                pipeline: ".dagron/ci/ci.yaml".into(),
            },
            kind: "push".into(),
            status: "succeeded".into(),
            started: true,
            whole_reruns: 0,
        }
    );
    meter_due_now(db).await;
    assert_eq!(
        reporter.meter_ends().await.unwrap(),
        pass(0, false),
        "told once"
    );

    // Cancelled before any job started: told so. The meter refuses it once,
    // and it is told again once the lease runs out.
    *meter.refuse.lock().unwrap() = 1;
    recorded_again(db, "run-2").await;
    set_run_at(
        &st,
        "run-2",
        &[("build", "cancelled", 0)],
        Some("cancelled"),
    );
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(reporter.meter_ends().await.unwrap(), pass(0, true));
    assert!(meter_due(db, "run-2").await, "refused, so still due");
    assert_eq!(
        reporter.meter_ends().await.unwrap(),
        pass(0, false),
        "not before its lease is out"
    );
    meter_due_now(db).await;
    assert_eq!(reporter.meter_ends().await.unwrap(), pass(1, false));
    assert!(!meter_due(db, "run-2").await);
    let told = meter.ended.lock().unwrap().clone();
    assert_eq!(
        (
            told[1].run.run_id.as_str(),
            told[1].status.as_str(),
            told[1].started
        ),
        ("run-2", "cancelled", false)
    );

    // Seen started, then gone from dagron-api: started stays.
    recorded_again(db, "run-3").await;
    set_run_at(&st, "run-3", &[("build", "running", 1)], None);
    assert!(reporter.report_one("a").await.unwrap());
    st.lock().unwrap().runs.remove("run-3");
    make_due(db, "run-3").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(reporter.meter_ends().await.unwrap(), pass(1, false));
    let told = meter.ended.lock().unwrap().clone();
    assert_eq!(
        (
            told[2].run.run_id.as_str(),
            told[2].status.as_str(),
            told[2].started
        ),
        ("run-3", GONE, true)
    );

    // Run whole again (`from: all`), on the same run: each round's end is
    // told with the count its cell gives, and with whether a job of that
    // round started, not of the round before. A rerun of the round's failed
    // jobs keeps the count.
    let round = |status: &'static str, jobs: &'static [(&'static str, &'static str, i64)]| {
        let st = st.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(db.reopen_run("run-1", Duration::ZERO, None).await.unwrap());
            set_run_at(&st, "run-1", jobs, Some(status));
        }
    };
    let last_told = || {
        let e = meter.ended.lock().unwrap().last().cloned().unwrap();
        (e.run.run_id, e.status, e.started, e.whole_reruns)
    };
    st.lock().unwrap().whole_reruns.insert("run-1".into(), 1);
    round(
        "cancelled",
        &[("build", "cancelled", 0), ("test", "cancelled", 0)],
    )
    .await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(reporter.meter_ends().await.unwrap(), pass(1, false));
    assert_eq!(
        last_told(),
        ("run-1".to_string(), "cancelled".to_string(), false, 1),
        "the first round's jobs started; this round's did not"
    );
    round(
        "failed",
        &[("build", "failed", 1), ("test", "cancelled", 0)],
    )
    .await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(reporter.meter_ends().await.unwrap(), pass(1, false));
    assert_eq!(
        last_told(),
        ("run-1".to_string(), "failed".to_string(), true, 1)
    );
}

/// A pass stops at the first end the meter refuses: the ends behind it are
/// not claimed, so a meter that is down costs one wait, not one per end, and
/// they are told on the next pass.
async fn a_refused_end_stops_the_pass(db: &Db) {
    let (origin, st) = stub().await;
    let meter = Arc::new(StubMeter::default());
    let (reporter, _) = reporter(db, &origin, Some("ghp_test"));
    let reporter = reporter.with_meter(meter.clone());
    for run_id in ["run-1", "run-2", "run-3"] {
        if run_id == "run-1" {
            recorded(db, run_id).await;
        } else {
            recorded_again(db, run_id).await;
        }
        set_run_at(&st, run_id, &[("build", "succeeded", 1)], Some("succeeded"));
        assert!(reporter.report_one("a").await.unwrap());
    }
    *meter.refuse.lock().unwrap() = 1;
    assert_eq!(reporter.meter_ends().await.unwrap(), pass(0, true));
    assert!(meter.ended.lock().unwrap().is_empty());
    // The two behind it were never claimed: they are due, and taken now.
    assert_eq!(reporter.meter_ends().await.unwrap(), pass(2, false));
    // The refused one, once its lease is out.
    meter_due_now(db).await;
    assert_eq!(reporter.meter_ends().await.unwrap(), pass(1, false));
    let mut told: Vec<String> = meter
        .ended
        .lock()
        .unwrap()
        .iter()
        .map(|e| e.run.run_id.clone())
        .collect();
    told.sort();
    assert_eq!(told, ["run-1", "run-2", "run-3"]);
}

#[tokio::test]
async fn a_refused_end_stops_the_meter_pass() {
    let (db, _dir) = sqlite().await;
    a_refused_end_stops_the_pass(&db).await;
}

#[tokio::test]
async fn a_refused_end_stops_the_meter_pass_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    a_refused_end_stops_the_pass(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

/// Another run of acme/widgets, for an event of its own.
async fn recorded_again(db: &Db, run_id: &str) {
    let repo = db
        .find_repo("github.com", "acme/widgets")
        .await
        .unwrap()
        .unwrap();
    let mut ev = crate::db::tests::event();
    // A commit of its own: its event is a key of its own.
    ev.sha = hex::encode(sha2::Sha256::digest(run_id.as_bytes()))[..40].to_string();
    db.record_delivery(&repo, &uuid::Uuid::now_v7().to_string(), &ev, b"{}")
        .await
        .unwrap();
    let ev = db
        .claim_event("w", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    record(db, &ev, run_id).await;
}

#[tokio::test]
async fn each_run_s_end_is_told_to_the_meter_once() {
    let (db, _dir) = sqlite().await;
    the_meter_contract(&db).await;
}

#[tokio::test]
async fn each_run_s_end_is_told_to_the_meter_once_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    the_meter_contract(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

/// Without a meter, no end is ever due.
#[tokio::test]
async fn without_a_meter_no_end_is_due() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(&db, &origin, Some("ghp_test"));
    recorded(&db, "run-1").await;
    set_run(&st, "run-1", &[("build", "succeeded")], Some("succeeded"));
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(row(&db, "run-1").await.status, "succeeded");
    assert!(!meter_due(&db, "run-1").await);
    assert_eq!(reporter.meter_ends().await.unwrap(), pass(0, false));
}

/// A reporter whose forge is an App that posts check runs
/// (CI_GITHUB_CHECKS=on), and links each to its run's page. `None`, and the
/// test skipped, where there is no `openssl` to make the App's key with.
fn reporter_with_checks(db: &Db, origin: &str) -> Option<Reporter> {
    let dir = tempfile::tempdir().unwrap();
    let (private, _) = crate::checkout::tests::rsa_key(dir.path())?;
    let app = crate::checkout::GitHubApp::new(4242, &private).unwrap();
    let forge = Forge::new(Some("ghp_test".into()))
        .with_origin(origin)
        .with_app(Some(app))
        .with_checks(true);
    Some(Reporter::new(
        db.clone(),
        BTreeMap::from([("default".to_string(), origin.to_string())]),
        "dgr_token".into(),
        Arc::new(forge),
        Some("https://ci.example/runs/{run_id}".into()),
        Arc::new(Metrics::default()),
    ))
}

/// Each check run GitHub took from the `from`-th on: how it came, its id,
/// its name, and how it read.
fn checked(st: &Shared, from: usize) -> Vec<String> {
    st.lock().unwrap().checks[from..]
        .iter()
        .map(|(how, id, check, _)| {
            format!(
                "{how} {id} {}: {}/{} {}",
                check["name"].as_str().unwrap(),
                check["status"].as_str().unwrap(),
                check["conclusion"].as_str().unwrap_or("-"),
                check["output"]["title"].as_str().unwrap(),
            )
        })
        .collect()
}

fn check_of(jobs: &[(&str, &str)], finished: Option<&str>, name: &str) -> String {
    let all = checks("ci", "push", &seen(jobs, finished));
    let (status, conclusion, title) = &all[&format!("dagron-ci/ci/{name} (push)")];
    format!("{status}/{} {title}", conclusion.unwrap_or("-"))
}

#[test]
fn each_job_s_state_is_a_check_run() {
    let going = |status: &str| check_of(&[("test", status)], None, "test");
    assert_eq!(going("pending"), "queued/- queued");
    assert_eq!(going("ready"), "queued/- queued");
    assert_eq!(going("awaiting_approval"), "queued/- awaiting approval");
    assert_eq!(going("running"), "in_progress/- running");
    assert_eq!(going("succeeded"), "completed/success succeeded");
    assert_eq!(going("failed"), "completed/failure failed");
    assert_eq!(going("cancelled"), "completed/cancelled cancelled");
    // A skipped job waits for the run, as its status does: skipped where the
    // run succeeded, and nothing a required check is met by where it did not.
    assert_eq!(
        going("skipped"),
        "queued/- skipped; settled when the run ends"
    );
    let ended = |status: &str, run: &str| check_of(&[("test", status)], Some(run), "test");
    assert_eq!(ended("skipped", "succeeded"), "completed/skipped skipped");
    assert_eq!(
        ended("skipped", "failed"),
        "completed/cancelled skipped: an earlier job did not succeed"
    );
    assert_eq!(
        ended("running", "failed"),
        "completed/failure did not finish"
    );
    // Named as its status would be, a merge group's as its pull request's.
    let named: Vec<String> = checks("ci", "merge_group", &seen(&[("test", "running")], None))
        .into_keys()
        .collect();
    assert_eq!(named, ["dagron-ci/ci/test (pull_request)"]);
}

/// With check runs on and the App to post them, a run's jobs are check runs
/// and no statuses: one each, brought up to date as the job moves, linked to
/// the run and carrying its id. A job run again is a new check run of the
/// same name, and one that was not run again is left as it ended.
#[tokio::test]
async fn a_run_s_jobs_are_check_runs_where_the_app_posts_them() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_checks(&db, &origin) else {
        return;
    };
    let ev = recorded(&db, "run-1").await;
    set_run(
        &st,
        "run-1",
        &[("lint", "running"), ("test", "pending")],
        None,
    );
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        checked(&st, 0),
        [
            "POST 1 dagron-ci/ci/lint (push): in_progress/- running",
            "POST 2 dagron-ci/ci/test (push): queued/- queued",
        ]
    );
    {
        let st = st.lock().unwrap();
        assert!(st.posts.is_empty(), "no job is a status as well");
        let (_, _, check, carried) = &st.checks[0];
        assert_eq!(check["head_sha"], json!(ev.sha));
        assert_eq!(check["external_id"], "run-1");
        assert_eq!(check["details_url"], "https://ci.example/runs/run-1");
        let summary = check["output"]["summary"].as_str().unwrap();
        assert!(summary.contains("`run-1`"), "{summary}");
        // With the token that is for check runs, and nothing else.
        let for_checks = st
            .minted
            .iter()
            .position(|permissions| *permissions == json!({ "checks": "write" }))
            .expect("a token for check runs was minted");
        assert_eq!(carried, &format!("Bearer ghs_{}", for_checks + 1));
    }

    // Looked at again with nothing moved: nothing is posted.
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(checked(&st, 2), Vec::<String>::new());

    set_run(
        &st,
        "run-1",
        &[("lint", "succeeded"), ("test", "failed")],
        Some("failed"),
    );
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        checked(&st, 2),
        [
            "PATCH 1 dagron-ci/ci/lint (push): completed/success succeeded",
            "PATCH 2 dagron-ci/ci/test (push): completed/failure failed",
        ],
        "each brought up to date, by its id"
    );
    let r = row(&db, "run-1").await;
    assert_eq!((r.due, r.status.as_str()), (false, "failed"));

    // Rerun from its failed job, and seen running.
    set_run(
        &st,
        "run-1",
        &[("lint", "succeeded"), ("test", "running")],
        None,
    );
    assert!(db.reopen_run("run-1", Duration::ZERO, None).await.unwrap());
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        checked(&st, 4),
        ["POST 3 dagron-ci/ci/test (push): in_progress/- running"],
        "a new check run of the same name; the one that is over stays as it ended"
    );
    set_run(
        &st,
        "run-1",
        &[("lint", "succeeded"), ("test", "succeeded")],
        Some("succeeded"),
    );
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        checked(&st, 5),
        ["PATCH 3 dagron-ci/ci/test (push): completed/success succeeded"]
    );
    let r = row(&db, "run-1").await;
    assert_eq!((r.due, r.status.as_str()), (false, "succeeded"));
    assert!(st.lock().unwrap().posts.is_empty());
}

/// A rerun that is over by the next look, having failed as it did: a new
/// result, and a new check run, as it is a status posted again.
#[tokio::test]
async fn a_rerun_that_fails_the_same_way_is_a_new_check_run() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_checks(&db, &origin) else {
        return;
    };
    recorded(&db, "run-1").await;
    let failed: &[(&str, &str)] = &[("lint", "succeeded"), ("test", "failed")];
    set_run(&st, "run-1", failed, Some("failed"));
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(checked(&st, 0).len(), 2);
    assert!(db.reopen_run("run-1", Duration::ZERO, None).await.unwrap());
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        checked(&st, 2),
        ["POST 3 dagron-ci/ci/test (push): completed/failure failed"]
    );
}

/// The acceptance line: a cancelled run turns its checks to cancelled, and
/// a job that was skipped for it meets no required check.
#[tokio::test]
async fn a_cancelled_run_s_check_runs_read_cancelled() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_checks(&db, &origin) else {
        return;
    };
    recorded(&db, "run-1").await;
    set_run(
        &st,
        "run-1",
        &[("build", "running"), ("test", "pending")],
        None,
    );
    assert!(reporter.report_one("a").await.unwrap());
    set_run(
        &st,
        "run-1",
        &[("build", "cancelled"), ("test", "skipped")],
        Some("cancelled"),
    );
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        checked(&st, 2),
        [
            "PATCH 1 dagron-ci/ci/build (push): completed/cancelled cancelled",
            "PATCH 2 dagron-ci/ci/test (push): completed/cancelled skipped: an earlier job did not succeed",
        ]
    );
}

/// The last check run GitHub took under `job`'s name.
fn last_check(st: &Shared, job: &str) -> Value {
    let name = format!("dagron-ci/ci/{job} (push)");
    st.lock()
        .unwrap()
        .checks
        .iter()
        .rev()
        .find(|(_, _, check, _)| check["name"] == name.as_str())
        .map(|(_, _, check, _)| check.clone())
        .unwrap_or_else(|| panic!("no check run for {job}"))
}

/// The logs dagron-api was asked for, each by the task it was asked of.
fn logs_read(st: &Shared) -> Vec<String> {
    st.lock()
        .unwrap()
        .reads
        .iter()
        .filter(|(path, _)| path.contains("/logs?"))
        .map(|(path, auth)| {
            assert_eq!(auth, "Bearer dgr_token");
            let (_, rest) = path.split_once("task=").unwrap();
            rest.split('&').next().unwrap().to_string()
        })
        .collect()
}

/// What a job said of itself as it ended is read from its log, and goes with
/// the writing that ends its check run: where its tests failed as
/// annotations, its report as the check's text, and what its steps wrote to
/// their summaries under the check's own. A check run that is not over says
/// none of it, and a job that was never started is asked nothing.
#[tokio::test]
async fn what_a_job_said_goes_with_the_end_of_its_check_run() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_checks(&db, &origin) else {
        return;
    };
    recorded(&db, "run-1").await;
    st.lock().unwrap().logs.insert(
        "t-test".into(),
        [
            "::dagron-ci::step::1::start::cargo test",
            "test t::bad ... FAILED",
            "::dagron-ci::step::1::end::101",
            r#"::dagron-ci::report::{"tests":12,"failures":1,"errors":0,"skipped":2,"findings":0,"more_annotations":0,"more_failed":0,"problems":[]}"#,
            r#"::dagron-ci::annotation::{"path":"src/lib.rs","start_line":7,"end_line":7,"level":"failure","title":"t::bad","message":"left != right"}"#,
            r#"::dagron-ci::annotation::{"path":"../../etc/passwd","start_line":1,"end_line":1,"level":"failure","title":"x","message":"y"}"#,
            r#"::dagron-ci::failed::{"name":"t::bad","message":"left != right"}"#,
            r###"::dagron-ci::summary::"## Built\n\n""###,
            r#"::dagron-ci::summary::"3 crates.""#,
            r#"::dagron-ci::result::{"exit_code":101}"#,
        ]
        .map(str::to_string)
        .to_vec(),
    );
    set_run_at(
        &st,
        "run-1",
        &[
            ("lint", "running", 1),
            ("test", "running", 1),
            ("deploy", "pending", 0),
        ],
        None,
    );
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(logs_read(&st), Vec::<String>::new(), "no job is over");
    let going = last_check(&st, "test");
    assert!(
        going["output"].get("text").is_none() && going["output"].get("annotations").is_none(),
        "{going}"
    );

    set_run_at(
        &st,
        "run-1",
        &[
            ("lint", "succeeded", 1),
            ("test", "failed", 1),
            ("deploy", "skipped", 0),
        ],
        Some("failed"),
    );
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        checked(&st, 3),
        [
            "PATCH 1 dagron-ci/ci/deploy (push): completed/cancelled skipped: an earlier job did not succeed",
            "PATCH 2 dagron-ci/ci/lint (push): completed/success succeeded",
            "PATCH 3 dagron-ci/ci/test (push): completed/failure failed",
        ],
        "how each ended is the engine's word, whatever a job said"
    );
    // Each job that ran is asked once, by its task; one that never started
    // is not asked.
    assert_eq!(logs_read(&st), ["t-lint", "t-test"]);
    let test = last_check(&st, "test");
    assert_eq!(
        test["output"]["annotations"],
        json!([{
            "path": "src/lib.rs", "start_line": 7, "end_line": 7,
            "annotation_level": "failure", "title": "t::bad", "message": "left != right",
        }]),
        "and none for a place outside the repository"
    );
    assert_eq!(
        test["output"]["text"],
        "**12 tests**: 1 failed, 0 errors, 2 skipped.\n\n- `t::bad`: `left != right`"
    );
    let summary = test["output"]["summary"].as_str().unwrap();
    assert!(
        summary.starts_with("`dagron-ci/ci/test (push)`"),
        "{summary}"
    );
    assert!(
        summary.ends_with("\n\n---\n\n## Built\n\n3 crates."),
        "{summary}"
    );
    // A job that said nothing has a check run as it was.
    for quiet in ["lint", "deploy"] {
        let check = last_check(&st, quiet);
        let output = &check["output"];
        assert!(
            output.get("text").is_none() && output.get("annotations").is_none(),
            "{check}"
        );
        assert!(
            !output["summary"].as_str().unwrap().contains("---"),
            "{check}"
        );
    }
    let r = row(&db, "run-1").await;
    assert_eq!((r.due, r.status.as_str(), r.error), (false, "failed", None));

    // Looked at again, it is over: nothing is posted, and no log is read.
    assert!(db.reopen_run("run-1", Duration::ZERO, None).await.unwrap());
    set_run_at(
        &st,
        "run-1",
        &[
            ("lint", "succeeded", 1),
            ("test", "running", 2),
            ("deploy", "pending", 0),
        ],
        None,
    );
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        logs_read(&st).len(),
        2,
        "a job that is running again is not asked"
    );
    // Run again and ended, it is asked again: the report is of this attempt.
    st.lock().unwrap().logs.insert(
        "t-test".into(),
        vec![
            r#"::dagron-ci::report::{"tests":12,"failures":0,"errors":0,"skipped":2}"#.to_string(),
        ],
    );
    set_run_at(
        &st,
        "run-1",
        &[
            ("lint", "succeeded", 1),
            ("test", "succeeded", 2),
            ("deploy", "succeeded", 1),
        ],
        Some("succeeded"),
    );
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(logs_read(&st)[2..], ["t-deploy", "t-test"]);
    let again = last_check(&st, "test");
    assert_eq!(
        again["output"]["text"],
        "**12 tests**: 0 failed, 0 errors, 2 skipped."
    );
    assert!(again["output"].get("annotations").is_none(), "{again}");
}

/// A log dagron-api does not answer for holds up no check run: it ends as
/// its job did, and says what it is without. Why is the operator's to read,
/// and names the cell. The run is held while its logs are read, so the
/// first that is not answered is the last asked for at that look.
#[tokio::test]
async fn a_log_that_is_not_read_holds_up_no_check_run() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_checks(&db, &origin) else {
        return;
    };
    recorded(&db, "run-1").await;
    st.lock().unwrap().logs_fail = true;
    set_run(
        &st,
        "run-1",
        &[("lint", "failed"), ("test", "failed")],
        Some("failed"),
    );
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        checked(&st, 0),
        [
            "POST 1 dagron-ci/ci/lint (push): completed/failure failed",
            "POST 2 dagron-ci/ci/test (push): completed/failure failed",
        ]
    );
    assert_eq!(logs_read(&st), ["t-lint"], "and no other after it");
    for job in ["lint", "test"] {
        let check = last_check(&st, job);
        let text = check["output"]["text"].as_str().unwrap();
        assert!(text.contains("could not be read from its log"), "{text}");
        assert!(
            !text.contains("127.0.0.1") && !text.contains("http"),
            "{text}"
        );
    }
    let r = row(&db, "run-1").await;
    assert_eq!((r.due, r.status.as_str(), r.error), (false, "failed", None));
}

/// An App that was not given check runs: the run's jobs are statuses, as
/// they were, and a run is one or the other to its end whatever changes.
#[tokio::test]
async fn a_run_s_jobs_are_statuses_where_the_app_posts_no_check_runs() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_checks(&db, &origin) else {
        return;
    };
    st.lock().unwrap().no_checks = true;
    recorded(&db, "run-1").await;
    set_run(&st, "run-1", &[("test", "running")], None);
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        posted(&st),
        [(
            "dagron-ci/ci/test (push)".to_string(),
            "pending".to_string(),
            "running".to_string()
        )]
    );
    assert!(st.lock().unwrap().checks.is_empty());

    // The App is given the permission while the run goes. A reporter that
    // finds it (another replica, or this one once it asks again) goes on as
    // the run began: a job that was a status does not become a check run too.
    st.lock().unwrap().no_checks = false;
    let Some(other) = reporter_with_checks(&db, &origin) else {
        return;
    };
    set_run(&st, "run-1", &[("test", "succeeded")], Some("succeeded"));
    make_due(&db, "run-1").await;
    assert!(other.report_one("b").await.unwrap());
    assert_eq!(posted(&st).len(), 2);
    assert_eq!(posted(&st)[1].1, "success");
    assert!(st.lock().unwrap().checks.is_empty());

    // The next run is check runs.
    recorded_again(&db, "run-2").await;
    set_run(&st, "run-2", &[("test", "running")], None);
    assert!(other.report_one("b").await.unwrap());
    assert_eq!(
        checked(&st, 0),
        ["POST 1 dagron-ci/ci/test (push): in_progress/- running"]
    );
}

/// The runs that are due are looked at together, and what each look found
/// is written with what the others found by then, its posts in the report
/// log; and one statement takes many runs' writes, each fenced on its own
/// claim.
#[tokio::test]
async fn due_runs_are_looked_at_together_and_written_in_batches() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, metrics) = reporter(&db, &origin, Some("ghp_test"));
    let reporter = Arc::new(reporter);
    let ev = recorded(&db, "run-1").await;
    record_as(&db, &ev, "run-2", ".dagron/ci/lint.yaml").await;
    set_run(&st, "run-1", &[("build", "succeeded")], Some("succeeded"));
    set_run(&st, "run-2", &[("lint", "running")], None);

    // Both are looked at at once; each is written once its look ends, with
    // any other ended by then.
    let mut looks = Looking::new();
    let mut written = 0;
    while written < 2 {
        written += reporter.report_round("a", &mut looks).await.unwrap();
    }
    assert!(looks.is_empty());
    let text = metrics.render(None, &[]);
    assert!(
        text.contains("dagron_ci_report_looks_written_total 2\n"),
        "{text}"
    );
    assert!(
        text.contains("dagron_ci_report_batches_total 1\n")
            || text.contains("dagron_ci_report_batches_total 2\n"),
        "{text}"
    );
    assert!(
        text.contains("dagron_ci_report_writes_waiting 0\n"),
        "{text}"
    );
    let (one, two) = (row(&db, "run-1").await, row(&db, "run-2").await);
    assert_eq!(
        (one.status.as_str(), one.due),
        ("succeeded", false),
        "{one:?}"
    );
    assert_eq!(
        (two.status.as_str(), two.due),
        ("submitted", true),
        "{two:?}"
    );
    assert_eq!(
        reporter.report_round("a", &mut looks).await.unwrap(),
        0,
        "nothing is due now"
    );

    // What was posted, and what came of it, is a row of the log each.
    let mut log: Vec<(String, String, String)> = db
        .report_log_of(&ev.id)
        .await
        .unwrap()
        .iter()
        .map(|row| {
            let row: Value = serde_json::from_str(row).unwrap();
            let at = |k: &str| row[k].as_str().unwrap_or("-").to_string();
            (at("context"), at("state"), at("outcome"))
        })
        .collect();
    log.sort();
    assert_eq!(
        log,
        [
            (
                "dagron-ci/ci/build (push)".to_string(),
                "success".to_string(),
                "posted".to_string()
            ),
            (
                "dagron-ci/lint/lint (push)".to_string(),
                "pending".to_string(),
                "posted".to_string()
            ),
        ]
    );
    assert_eq!(posted(&st).len(), 2);

    // One statement takes many runs' writes, each fenced on its own claim:
    // a run another reporter holds is not written.
    make_due(&db, "run-1").await;
    make_due(&db, "run-2").await;
    let (first, _) = db
        .claim_report("b", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    let (second, _) = db
        .claim_report("b", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    let write = ReportWrite {
        next: Some(Duration::from_secs(30)),
        ..ReportWrite::default()
    };
    let finished = db
        .finish_reports(&[
            (first.as_str(), "b", &write),
            (second.as_str(), "not-b", &write),
            ("run-none", "b", &write),
        ])
        .await
        .unwrap();
    assert_eq!(finished, std::slice::from_ref(&first));
    assert!(row(&db, &first).await.due);
}

/// A look that is slow holds no other run back: a run that falls due while
/// it is in flight is looked at, and written, before it ends.
#[tokio::test]
async fn a_slow_look_holds_no_other_run_back() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(&db, &origin, Some("ghp_test"));
    let reporter = Arc::new(reporter);
    let ev = recorded(&db, "run-slow").await;
    set_run(&st, "run-slow", &[("build", "running")], None);
    st.lock().unwrap().slow = Some(("run-slow".into(), Duration::from_secs(5)));
    let mut looks = Looking::new();
    let started;
    let written = {
        // The slow run is claimed and its look begins; nothing has ended.
        let round = reporter.report_round("a", &mut looks);
        tokio::pin!(round);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut round)
                .await
                .is_err(),
            "the slow look is in flight"
        );
        // Another run falls due meanwhile, and is looked at at once.
        record_as(&db, &ev, "run-fast", ".dagron/ci/lint.yaml").await;
        set_run(&st, "run-fast", &[("lint", "running")], None);
        started = std::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(4), &mut round)
            .await
            .expect("the run that fell due was not held back by the slow look")
            .unwrap()
    };
    assert_eq!(written, 1);
    assert!(started.elapsed() < Duration::from_secs(4));
    assert!(
        row(&db, "run-fast").await.due,
        "written, and due again in a while"
    );
    assert_eq!(looks.len(), 1, "the slow one is still in flight");
}

// ── A GitLab merge request ──────────────────────────────────────────────────

/// A merge request of GitLab's lab/gadgets: its jobs' statuses are named as
/// GitLab names it, and each is of the merge request's source branch, so
/// that GitLab keeps them in that branch's pipeline, which the merge request
/// shows. It has one note, made at the first look and written again in place
/// as its jobs change, with the host's token; and one someone deleted is made
/// again.
#[tokio::test]
async fn a_merge_request_s_statuses_are_its_branch_s_pipeline_and_it_has_one_note() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let forge = Forge::new(None)
        .with_origin(&origin)
        .with_gitlab_tokens(&BTreeMap::from([(
            "gitlab.example".to_string(),
            "glpat-x".to_string(),
        )]))
        .with_mr_notes(true);
    let reporter = Reporter::new(
        db.clone(),
        BTreeMap::from([("default".to_string(), origin.to_string())]),
        "dgr_token".into(),
        Arc::new(forge),
        Some("https://ci.example/runs/{run_id}".into()),
        Arc::new(Metrics::default()),
    );
    let repo = db
        .add_repo("gitlab", "gitlab.example", "lab/gadgets", None, "default")
        .await
        .unwrap()
        .unwrap();
    let mut ev = crate::db::tests::event();
    ev.kind = Kind::PullRequest;
    ev.git_ref = "refs/merge-requests/7/head".into();
    ev.pr = Some(7);
    ev.base_sha = Some("b".repeat(40));
    ev.base_ref = Some("main".into());
    ev.head_ref = Some("topic".into());
    db.record_delivery(&repo, "mr-7", &ev, b"{}").await.unwrap();
    let claimed = db
        .claim_event("w", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    record(&db, &claimed, "run-1").await;
    set_run_at(&st, "run-1", &[("build", "running", 1)], None);

    assert!(reporter.report_one("a").await.unwrap());
    assert!(reporter.comment_one("a").await.unwrap());
    let said = |st: &Shared| -> Vec<(String, String, String)> {
        st.lock()
            .unwrap()
            .gl_statuses
            .iter()
            .map(|s| {
                let at = |k: &str| s[k].as_str().unwrap_or("-").to_string();
                (at("name"), at("state"), at("ref"))
            })
            .collect()
    };
    assert_eq!(
        said(&st),
        [(
            "dagron-ci/ci/build (merge_request)".to_string(),
            "pending".to_string(),
            "topic".to_string()
        )]
    );
    let first = commented(&st);
    assert_eq!(first.len(), 1);
    assert_eq!((first[0].0.as_str(), first[0].1), ("POST", 1));
    assert!(
        first[0].2.contains("### dagron-ci: running\n"),
        "{}",
        first[0].2
    );
    assert_eq!(st.lock().unwrap().comments[0].3, "Bearer glpat-x");

    // The job ends: its status, and the same note written again.
    set_run_at(
        &st,
        "run-1",
        &[("build", "succeeded", 1)],
        Some("succeeded"),
    );
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    comment_due_now(&db).await;
    assert!(reporter.comment_one("a").await.unwrap());
    assert_eq!(said(&st)[1].1, "success");
    assert_eq!(said(&st)[1].2, "topic");
    let notes = commented(&st);
    assert_eq!((notes[1].0.as_str(), notes[1].1), ("PUT", 1));
    assert!(
        notes[1].2.contains("### dagron-ci: passed\n"),
        "{}",
        notes[1].2
    );

    // Someone deletes it: the next thing to say is said in a new one.
    st.lock().unwrap().deleted.push(1);
    set_run_at(
        &st,
        "run-1",
        &[("build", "succeeded", 1), ("test", "failed", 1)],
        Some("failed"),
    );
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    comment_due_now(&db).await;
    assert!(reporter.comment_one("a").await.unwrap());
    let notes = commented(&st);
    assert_eq!(
        notes
            .iter()
            .map(|(how, id, _)| (how.as_str(), *id))
            .collect::<Vec<_>>(),
        [("POST", 1), ("PUT", 1), ("POST", 2)],
        "the write to the deleted note was answered 404, and a new one made"
    );
    assert!(
        notes[2].2.contains(
            "### dagron-ci: 1 job failed
"
        ),
        "{}",
        notes[2].2
    );
}

// ── A pull request's comment ────────────────────────────────────────────────

/// A reporter that keeps a comment on each pull request
/// (CI_GITHUB_PR_COMMENT=on), with or without check runs. `None`, and the
/// test skipped, where there is no `openssl` to make the App's key with.
fn reporter_with_comment(db: &Db, origin: &str, checks: bool) -> Option<Reporter> {
    let dir = tempfile::tempdir().unwrap();
    let (private, _) = crate::checkout::tests::rsa_key(dir.path())?;
    let app = crate::checkout::GitHubApp::new(4242, &private).unwrap();
    let forge = Forge::new(Some("ghp_test".into()))
        .with_origin(origin)
        .with_app(Some(app))
        .with_checks(checks)
        .with_pr_comment(true);
    Some(Reporter::new(
        db.clone(),
        BTreeMap::from([("default".to_string(), origin.to_string())]),
        "dgr_token".into(),
        Arc::new(forge),
        Some("https://ci.example/runs/{run_id}".into()),
        Arc::new(Metrics::default()),
    ))
}

/// Pull request 7 of acme/widgets, and `run_id` recorded for it.
async fn pull_request(db: &Db, run_id: &str) -> Claimed {
    let (_, ev) = claimed(db, None, Kind::PullRequest, Trust::Trusted).await;
    record(db, &ev, run_id).await;
    ev
}

/// Another run of `ev`: of another of its pipelines.
async fn record_as(db: &Db, ev: &Claimed, run_id: &str, pipeline: &str) {
    let run = SubmittedRun {
        run_id: run_id.into(),
        event_id: ev.id.clone(),
        repo_id: ev.repo.id.clone(),
        cell: "default".into(),
        pipeline: pipeline.into(),
        idempotency_key: format!("key-{run_id}"),
        on_failed: false,
    };
    assert!(db.record_run(&run).await.unwrap());
}

/// A new commit on pull request 7, and `run_id` recorded for it: the
/// commit's id.
async fn pushed_to(db: &Db, run_id: &str) -> String {
    let repo = db
        .find_repo("github.com", "acme/widgets")
        .await
        .unwrap()
        .unwrap();
    let mut ev = crate::db::tests::event();
    ev.kind = Kind::PullRequest;
    ev.git_ref = "refs/pull/7/head".into();
    ev.pr = Some(7);
    ev.base_sha = Some("b".repeat(40));
    ev.base_ref = Some("main".into());
    ev.sha = hex::encode(sha2::Sha256::digest(run_id.as_bytes()))[..40].to_string();
    db.record_delivery(&repo, &uuid::Uuid::now_v7().to_string(), &ev, b"{}")
        .await
        .unwrap();
    let claimed = db
        .claim_event("w", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    record(db, &claimed, run_id).await;
    ev.sha
}

/// Each comment GitHub took: how it came, its id, and what it says.
fn commented(st: &Shared) -> Vec<(String, u64, String)> {
    st.lock()
        .unwrap()
        .comments
        .iter()
        .map(|(how, id, body, _)| (how.clone(), *id, body.clone()))
        .collect()
}

/// The comment's row for pull request 7: whether it is due, GitHub's id for
/// it, whether a write is to follow the one in hand, and why it was last
/// not written.
async fn comment_row(db: &Db) -> (bool, Option<String>, bool, Option<String>) {
    match db {
        Db::Sqlite(p) => {
            let (due, id, again, error): (Option<String>, Option<String>, i64, Option<String>) =
                sqlx::query_as(
                    "SELECT due_at, comment_id, again, error FROM ci_pr_comments WHERE pr = 7",
                )
                .fetch_one(p)
                .await
                .unwrap();
            (due.is_some(), id, again != 0, error)
        }
        Db::Postgres(p) => {
            let (due, id, again, error): (Option<String>, Option<String>, bool, Option<String>) =
                sqlx::query_as(
                    "SELECT due_at::TEXT, comment_id, again, error FROM ci_pr_comments WHERE pr = 7",
                )
                .fetch_one(p)
                .await
                .unwrap();
            (due.is_some(), id, again, error)
        }
    }
}

/// How many pull requests have a comment's row.
async fn comment_rows(db: &Db) -> i64 {
    match db {
        Db::Sqlite(p) => sqlx::query_scalar("SELECT COUNT(*) FROM ci_pr_comments")
            .fetch_one(p)
            .await
            .unwrap(),
        Db::Postgres(p) => sqlx::query_scalar("SELECT COUNT(*) FROM ci_pr_comments")
            .fetch_one(p)
            .await
            .unwrap(),
    }
}

/// A comment that waits is due now, as the passing of its wait would make it.
async fn comment_due_now(db: &Db) {
    match db {
        Db::Sqlite(p) => {
            sqlx::query("UPDATE ci_pr_comments SET due_at = '2000-01-01T00:00:00.000Z'")
                .execute(p)
                .await
                .unwrap()
                .rows_affected()
        }
        Db::Postgres(p) => {
            sqlx::query("UPDATE ci_pr_comments SET due_at = now() - interval '1 second'")
                .execute(p)
                .await
                .unwrap()
                .rows_affected()
        }
    };
}

/// A pull request has one comment: made when the first of its runs is
/// looked at, and written again in place as its runs change, with what a
/// job that is over said of its tests. A look that finds nothing changed
/// writes nothing. It goes up with a token for a pull request's comments
/// alone, and the statuses go on as they were.
async fn the_comment_contract(db: &Db) {
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_comment(db, &origin, false) else {
        return;
    };
    let ev = pull_request(db, "run-1").await;
    record_as(db, &ev, "run-2", ".dagron/ci/lint.yaml").await;
    st.lock().unwrap().logs.insert(
        "t-test".into(),
        [
            r#"::dagron-ci::report::{"tests":12,"failures":1,"errors":0,"skipped":0,"findings":0,"more_annotations":0,"more_failed":0,"problems":[]}"#,
            r#"::dagron-ci::failed::{"name":"t::bad","message":"left != right"}"#,
            r#"::dagron-ci::result::{"exit_code":101}"#,
        ]
        .map(str::to_string)
        .to_vec(),
    );
    set_run_at(
        &st,
        "run-1",
        &[
            ("build", "succeeded", 1),
            ("test", "running", 1),
            ("deploy", "pending", 0),
        ],
        None,
    );
    set_run(&st, "run-2", &[("lint", "running")], None);

    // Nothing is due before a run is looked at.
    assert!(!reporter.comment_one("a").await.unwrap());
    assert_eq!(comment_rows(db).await, 0);
    assert!(reporter.report_one("a").await.unwrap());
    assert!(reporter.comment_one("a").await.unwrap());
    // The jobs are kept by name, however the engine listed them.
    let kept: Option<String> = match db {
        Db::Sqlite(p) => sqlx::query_scalar("SELECT jobs FROM ci_runs WHERE run_id = 'run-1'")
            .fetch_one(p)
            .await
            .unwrap(),
        Db::Postgres(p) => sqlx::query_scalar("SELECT jobs FROM ci_runs WHERE run_id = 'run-1'")
            .fetch_one(p)
            .await
            .unwrap(),
    };
    let kept: Vec<Value> = serde_json::from_str(&kept.unwrap()).unwrap();
    let names: Vec<&str> = kept
        .iter()
        .map(|job| job["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["build", "deploy", "test"]);
    let first = commented(&st);
    assert_eq!(first.len(), 1);
    let (how, id, said) = &first[0];
    assert_eq!((how.as_str(), *id), ("POST", 1));
    let sha = &ev.sha[..12];
    assert!(
        said.starts_with(&format!(
            "{}\n### dagron-ci: running\n\nCommit `{sha}`: 4 jobs, 1 passed, 3 not over.\n",
            comment::MARK
        )),
        "{said}"
    );
    for row in [
        "| [`ci`](https://ci.example/runs/run-1) | `build` | passed |  |\n|  | `deploy` | waiting |  |\n|  | `test` | running |  |",
        // Not looked at yet: the run's own word.
        "| [`lint`](https://ci.example/runs/run-2) | | waiting | |",
    ] {
        assert!(said.contains(row), "{row}\n{said}");
    }
    // With a token of its own, for a pull request's comments and no more.
    {
        let st = st.lock().unwrap();
        let (.., auth) = &st.comments[0];
        let nth: usize = auth.strip_prefix("Bearer ghs_").unwrap().parse().unwrap();
        assert_eq!(st.minted[nth - 1], json!({ "pull_requests": "write" }));
        assert!(st.posts.iter().all(|(.., status)| status != auth));
    }
    assert_eq!(
        comment_row(db).await,
        (false, Some("1".into()), false, None)
    );
    assert!(!reporter.comment_one("a").await.unwrap(), "it says it all");

    // The other run is looked at: the same comment, written again, and not
    // at once. A run's jobs change one after another, and the comment is
    // not edited for each.
    assert!(reporter.report_one("a").await.unwrap());
    assert!(!reporter.comment_one("a").await.unwrap(), "not at once");
    assert!(comment_row(db).await.0, "it is due, in a while");
    comment_due_now(db).await;
    assert!(reporter.comment_one("a").await.unwrap());
    let (how, id, said) = commented(&st).pop().unwrap();
    assert_eq!((how.as_str(), id), ("PATCH", 1));
    assert!(
        said.contains("| [`lint`](https://ci.example/runs/run-2) | `lint` | running |  |"),
        "{said}"
    );

    // A look that finds the jobs as they were makes nothing due.
    make_due(db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert!(!comment_row(db).await.0);
    assert!(!reporter.comment_one("a").await.unwrap());
    assert_eq!(commented(&st).len(), 2);

    // The run ends, and a job that is over is asked what it said: once.
    set_run_at(
        &st,
        "run-1",
        &[
            ("build", "succeeded", 1),
            ("test", "failed", 1),
            ("deploy", "skipped", 0),
        ],
        Some("failed"),
    );
    make_due(db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    comment_due_now(db).await;
    assert!(reporter.comment_one("a").await.unwrap());
    let (how, id, said) = commented(&st).pop().unwrap();
    assert_eq!((how.as_str(), id), ("PATCH", 1));
    assert!(said.contains("### dagron-ci: 1 job failed\n"), "{said}");
    assert!(
        said.contains("4 jobs, 1 passed, 1 failed, 1 skipped, 1 not over."),
        "{said}"
    );
    assert!(
        said.contains("|  | `test` | **failed** | 1 of 12 failed |"),
        "{said}"
    );
    assert!(said.contains("|  | `deploy` | skipped |  |"), "{said}");
    assert!(
        said.contains("**Tests that failed**\n\n- `ci` / `test`: `t::bad`\n"),
        "{said}"
    );
    assert_eq!(logs_read(&st), ["t-build", "t-test"]);

    // The statuses are as they would be without it.
    let statuses: Vec<(String, String)> = posted(&st)
        .into_iter()
        .map(|(context, state, _)| (context, state))
        .collect();
    assert!(
        statuses.contains(&("dagron-ci/ci/test (pull_request)".into(), "failure".into())),
        "{statuses:?}"
    );
}

#[tokio::test]
async fn a_pull_request_s_runs_are_one_comment_written_again_in_place() {
    let (db, _dir) = sqlite().await;
    the_comment_contract(&db).await;
}

#[tokio::test]
async fn a_pull_request_s_runs_are_one_comment_written_again_in_place_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    the_comment_contract(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;

    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    a_comment_is_one_writer_s_at_a_time(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;

    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    a_wait_is_kept_and_a_refusal_is_left(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

/// A push has no conversation to write in, and a pull request is given a
/// comment only where one is asked for: nothing is kept for one otherwise.
#[tokio::test]
async fn a_comment_is_a_pull_request_s_and_only_where_it_is_asked_for() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_comment(&db, &origin, false) else {
        return;
    };
    recorded(&db, "run-1").await;
    set_run(&st, "run-1", &[("test", "succeeded")], Some("succeeded"));
    assert!(reporter.report_one("a").await.unwrap());
    assert!(!reporter.comment_one("a").await.unwrap());
    assert_eq!(comment_rows(&db).await, 0);
    assert_eq!(
        logs_read(&st),
        Vec::<String>::new(),
        "no log is read for it"
    );

    // A pull request's run, and a reporter that was not asked for one.
    let (db, _dir) = sqlite().await;
    let Some(off) = reporter_with_checks(&db, &origin) else {
        return;
    };
    st.lock().unwrap().no_checks = true;
    pull_request(&db, "run-2").await;
    set_run(&st, "run-2", &[("test", "succeeded")], Some("succeeded"));
    assert!(off.report_one("a").await.unwrap());
    assert!(!off.comment_one("a").await.unwrap());
    assert_eq!(comment_rows(&db).await, 0);
    assert!(commented(&st).is_empty());
}

/// The comment speaks of the pull request's latest commit: a new commit's
/// runs take the place of the last one's, in the comment there was.
#[tokio::test]
async fn a_new_commit_s_runs_take_the_place_of_the_last_one_s() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_comment(&db, &origin, false) else {
        return;
    };
    let ev = pull_request(&db, "run-1").await;
    set_run(&st, "run-1", &[("test", "failed")], Some("failed"));
    assert!(reporter.report_one("a").await.unwrap());
    assert!(reporter.comment_one("a").await.unwrap());
    let (_, _, said) = commented(&st).pop().unwrap();
    assert!(
        said.contains(&format!("Commit `{}`", &ev.sha[..12])),
        "{said}"
    );
    assert!(said.contains("### dagron-ci: 1 job failed"), "{said}");

    let sha = pushed_to(&db, "run-2").await;
    set_run(&st, "run-2", &[("test", "running")], None);
    assert!(reporter.report_one("a").await.unwrap());
    comment_due_now(&db).await;
    assert!(reporter.comment_one("a").await.unwrap());
    let (how, id, said) = commented(&st).pop().unwrap();
    assert_eq!((how.as_str(), id), ("PATCH", 1));
    assert!(
        said.contains(&format!("Commit `{}`: 1 job, 1 not over.", &sha[..12])),
        "{said}"
    );
    assert!(said.contains("### dagron-ci: running"), "{said}");
    assert!(
        !said.contains("run-1") && !said.contains("failed"),
        "{said}"
    );
}

/// A comment someone deleted is made again when there is something to say,
/// and is the pull request's comment from then on.
#[tokio::test]
async fn a_comment_that_was_deleted_is_made_again() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_comment(&db, &origin, false) else {
        return;
    };
    pull_request(&db, "run-1").await;
    set_run(&st, "run-1", &[("test", "running")], None);
    assert!(reporter.report_one("a").await.unwrap());
    assert!(reporter.comment_one("a").await.unwrap());
    st.lock().unwrap().deleted.push(1);

    set_run(&st, "run-1", &[("test", "succeeded")], Some("succeeded"));
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    comment_due_now(&db).await;
    assert!(reporter.comment_one("a").await.unwrap());
    let all = commented(&st);
    assert_eq!(
        all.iter()
            .map(|(how, id, _)| (how.as_str(), *id))
            .collect::<Vec<_>>(),
        [("POST", 1), ("POST", 2)]
    );
    assert!(all[1].2.contains("### dagron-ci: passed"), "{}", all[1].2);
    assert_eq!(
        comment_row(&db).await,
        (false, Some("2".into()), false, None)
    );
}

/// An App that was not given `pull requests: write` writes no comment and
/// is not asked again at each look, and the run's statuses go on. It is
/// asked again when a run of the pull request next changes.
#[tokio::test]
async fn an_app_without_the_permission_writes_no_comment_and_keeps_its_statuses() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_comment(&db, &origin, false) else {
        return;
    };
    st.lock().unwrap().no_comments = true;
    pull_request(&db, "run-1").await;
    set_run(&st, "run-1", &[("test", "running")], None);
    assert!(reporter.report_one("a").await.unwrap());
    assert!(reporter.comment_one("a").await.unwrap());
    assert!(commented(&st).is_empty());
    let (due, id, _, error) = comment_row(&db).await;
    assert!(!due && id.is_none(), "it is left until a run changes");
    let error = error.unwrap();
    assert!(error.contains("pull requests: write"), "{error}");
    assert!(!reporter.comment_one("a").await.unwrap());
    assert_eq!(posted(&st).len(), 1, "the status went up all the same");

    // Given the permission since, as a reporter that has not asked yet
    // finds it.
    st.lock().unwrap().no_comments = false;
    let Some(other) = reporter_with_comment(&db, &origin, false) else {
        return;
    };
    set_run(&st, "run-1", &[("test", "succeeded")], Some("succeeded"));
    make_due(&db, "run-1").await;
    assert!(other.report_one("b").await.unwrap());
    assert!(other.comment_one("b").await.unwrap());
    assert_eq!(commented(&st).len(), 1);
    assert_eq!(
        comment_row(&db).await,
        (false, Some("1".into()), false, None)
    );
}

/// A rate limit is waited out, and a run that changes meanwhile does not
/// cut the wait short. A comment GitHub does not take for itself is left,
/// with why, until a run changes.
async fn a_wait_is_kept_and_a_refusal_is_left(db: &Db) {
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_comment(db, &origin, false) else {
        return;
    };
    pull_request(db, "run-1").await;
    set_run(&st, "run-1", &[("test", "running")], None);
    st.lock().unwrap().refuse_comments = Some((429, vec![("retry-after", "120")]));
    assert!(reporter.report_one("a").await.unwrap());
    assert!(reporter.comment_one("a").await.unwrap());
    let (due, _, _, error) = comment_row(db).await;
    assert!(due && error.is_some(), "due again, after the wait");
    assert!(!reporter.comment_one("a").await.unwrap(), "not before it");

    // The run ends while the wait goes on: the wait stands.
    set_run(&st, "run-1", &[("test", "succeeded")], Some("succeeded"));
    make_due(db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert!(
        !reporter.comment_one("a").await.unwrap(),
        "the wait is kept"
    );

    // Once it is over, what is written is what the runs then say.
    st.lock().unwrap().refuse_comments = None;
    comment_due_now(db).await;
    assert!(reporter.comment_one("a").await.unwrap());
    let (how, _, said) = commented(&st).pop().unwrap();
    assert_eq!(how, "POST");
    assert!(said.contains("### dagron-ci: passed"), "{said}");
    assert_eq!(
        comment_row(db).await,
        (false, Some("1".into()), false, None)
    );

    // One GitHub does not take for itself is not asked again as it is.
    st.lock().unwrap().refuse_comments = Some((422, vec![]));
    pushed_to(db, "run-2").await;
    set_run(&st, "run-2", &[("test", "running")], None);
    assert!(reporter.report_one("a").await.unwrap());
    comment_due_now(db).await;
    assert!(reporter.comment_one("a").await.unwrap());
    let (due, id, _, error) = comment_row(db).await;
    assert!(!due);
    assert_eq!(
        id.as_deref(),
        Some("1"),
        "the comment there is stays the one"
    );
    assert!(error.unwrap().contains("HTTP 422"));
    assert!(!reporter.comment_one("a").await.unwrap());
}

#[tokio::test]
async fn a_comment_github_does_not_take_is_tried_again_only_when_that_can_help() {
    let (db, _dir) = sqlite().await;
    a_wait_is_kept_and_a_refusal_is_left(&db).await;
}

/// A comment has one writer at a time. A run that changes while it is being
/// written does not take it from its writer: the writer leaves it due, and
/// the next write says what the runs then say. A writer leaves it once.
async fn a_comment_is_one_writer_s_at_a_time(db: &Db) {
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_comment(db, &origin, false) else {
        return;
    };
    pull_request(db, "run-1").await;
    set_run(&st, "run-1", &[("test", "running")], None);
    assert!(reporter.report_one("a").await.unwrap());

    // A writer takes it, and nobody else does while it holds it.
    let held = db
        .claim_comment("writer", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (held.pr, held.comment_id.clone(), held.said.clone()),
        (7, None, None)
    );
    assert_eq!(held.repo.full_name, "acme/widgets");
    let nobody = db.claim_comment("other", Duration::from_secs(60)).await;
    assert!(nobody.unwrap().is_none());

    // The run ends meanwhile.
    set_run(&st, "run-1", &[("test", "succeeded")], Some("succeeded"));
    make_due(db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    let (due, _, again, _) = comment_row(db).await;
    assert!(due && again, "still its writer's, and to be written again");
    let nobody = db.claim_comment("other", Duration::from_secs(60)).await;
    assert!(nobody.unwrap().is_none());

    // The writer leaves it, written from what it had read: due again, as
    // any comment a run changed after it was written.
    let stale = crate::db::CommentWrite {
        said: Some("what-it-wrote".into()),
        ..Default::default()
    };
    assert!(db.finish_comment(&held, "writer", &stale).await.unwrap());
    assert!(
        !db.finish_comment(&held, "writer", &stale).await.unwrap(),
        "once"
    );
    let (due, _, again, _) = comment_row(db).await;
    assert!(due && !again);
    assert!(!reporter.comment_one("b").await.unwrap(), "not at once");
    comment_due_now(db).await;
    assert!(reporter.comment_one("b").await.unwrap());
    let (how, _, said) = commented(&st).pop().unwrap();
    assert_eq!(how, "POST");
    assert!(said.contains("### dagron-ci: passed"), "{said}");
    assert_eq!(
        comment_row(db).await,
        (false, Some("1".into()), false, None)
    );

    // Its runs, one a pipeline, of the latest commit.
    let runs = db.pr_runs(&held.repo.id, 7).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(
        (runs[0].run_id.as_str(), runs[0].status.as_str()),
        ("run-1", "succeeded")
    );
    assert!(db.pr_runs(&held.repo.id, 8).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_run_that_changes_mid_write_leaves_the_comment_due() {
    let (db, _dir) = sqlite().await;
    a_comment_is_one_writer_s_at_a_time(&db).await;
}

/// A run its cell no longer has is said so, in place of jobs nobody can ask
/// after any more.
#[tokio::test]
async fn a_run_that_is_gone_is_said_so() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_comment(&db, &origin, false) else {
        return;
    };
    pull_request(&db, "run-1").await;
    set_run(&st, "run-1", &[("test", "running")], None);
    assert!(reporter.report_one("a").await.unwrap());
    assert!(reporter.comment_one("a").await.unwrap());

    st.lock().unwrap().runs.clear();
    make_due(&db, "run-1").await;
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(row(&db, "run-1").await.status, GONE);
    comment_due_now(&db).await;
    assert!(reporter.comment_one("a").await.unwrap());
    let (how, _, said) = commented(&st).pop().unwrap();
    assert_eq!(how, "PATCH");
    assert!(
        said.contains("| [`ci`](https://ci.example/runs/run-1) | | no longer kept | |"),
        "{said}"
    );
}

/// With check runs, a job's log is read once for both: what it said goes on
/// its check run and in the comment.
#[tokio::test]
async fn with_check_runs_a_job_s_log_is_read_once_for_both() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_comment(&db, &origin, true) else {
        return;
    };
    pull_request(&db, "run-1").await;
    st.lock().unwrap().logs.insert(
        "t-test".into(),
        [
            r#"::dagron-ci::report::{"tests":3,"failures":0,"errors":0,"skipped":0,"findings":0,"more_annotations":0,"more_failed":0,"problems":[]}"#,
        ]
        .map(str::to_string)
        .to_vec(),
    );
    set_run(&st, "run-1", &[("test", "succeeded")], Some("succeeded"));
    assert!(reporter.report_one("a").await.unwrap());
    assert!(reporter.comment_one("a").await.unwrap());
    assert_eq!(logs_read(&st), ["t-test"]);
    assert_eq!(st.lock().unwrap().checks.len(), 1);
    let (_, _, said) = commented(&st).pop().unwrap();
    assert!(said.contains("| `test` | passed | 3 passed |"), "{said}");
    // A token each for its own: the check runs' and the comment's.
    let minted = st.lock().unwrap().minted.clone();
    assert!(minted.contains(&json!({ "checks": "write" })), "{minted:?}");
    assert!(
        minted.contains(&json!({ "pull_requests": "write" })),
        "{minted:?}"
    );
}

/// Let `run_id`'s hold go by, as the passing of `REPORT_HOLD` would.
async fn hold_over(db: &Db, run_id: &str) {
    match db {
        Db::Sqlite(p) => {
            sqlx::query("UPDATE ci_runs SET report_hold_until = NULL WHERE run_id = ?1")
                .bind(run_id)
                .execute(p)
                .await
                .unwrap()
                .rows_affected()
        }
        Db::Postgres(p) => {
            sqlx::query("UPDATE ci_runs SET report_hold_until = NULL WHERE run_id = $1")
                .bind(run_id)
                .execute(p)
                .await
                .unwrap()
                .rows_affected()
        }
    };
}

/// Whether `run_id` is due now.
async fn due_now(db: &Db, run_id: &str) -> bool {
    match db {
        Db::Sqlite(p) => sqlx::query_scalar::<_, bool>(
            "SELECT COALESCE(report_due_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 0) FROM ci_runs WHERE run_id = ?1",
        )
        .bind(run_id)
        .fetch_one(p)
        .await
        .unwrap(),
        Db::Postgres(p) => sqlx::query_scalar::<_, bool>(
            "SELECT COALESCE(report_due_at <= now(), false) FROM ci_runs WHERE run_id = $1",
        )
        .bind(run_id)
        .fetch_one(p)
        .await
        .unwrap(),
    }
}

/// 10.22: a run told of through its cell's outbox is due at once; one told
/// while a look of it is in hand is due again as that look is written, for
/// the look read it before the change; one that follows nothing more is
/// left as it is. Neither comes sooner than its hold: `REPORT_HOLD` after
/// a look, or, after one the forge rate-limited, the wait it said.
async fn the_tell_contract(db: &Db) {
    let (_, ev) = claimed(db, None, Kind::Push, Trust::Trusted).await;
    record(db, &ev, "run-told").await;
    let later = ReportWrite {
        next: Some(Duration::from_secs(600)),
        ..ReportWrite::default()
    };
    let claim = |c: &'static str| async move {
        db.claim_report(c, Duration::from_secs(60))
            .await
            .unwrap()
            .map(|(id, _)| id)
    };
    let tell = |runs: &'static [&'static str]| async move {
        let runs: Vec<String> = runs.iter().map(|r| r.to_string()).collect();
        db.tell_reports(&runs).await.unwrap()
    };
    assert_eq!(claim("a").await.as_deref(), Some("run-told"));
    assert!(db.finish_report("run-told", "a", &later).await.unwrap());
    assert_eq!(claim("a").await, None, "due in ten minutes");

    // Told at once after a look: not before its hold.
    assert_eq!(tell(&["run-told"]).await, 1);
    assert!(!due_now(db, "run-told").await, "held for REPORT_HOLD");
    hold_over(db, "run-told").await;
    assert_eq!(tell(&["run-told", "run-unknown"]).await, 1);
    assert_eq!(
        claim("a").await.as_deref(),
        Some("run-told"),
        "told: due at once"
    );

    // Told while the look is in hand: due again once it is written, at its
    // hold.
    assert_eq!(tell(&["run-told"]).await, 1);
    assert_eq!(claim("b").await, None, "still the look's");
    assert!(db.finish_report("run-told", "a", &later).await.unwrap());
    assert!(!due_now(db, "run-told").await, "not before its hold");
    make_due(db, "run-told").await;
    assert_eq!(
        claim("a").await.as_deref(),
        Some("run-told"),
        "looked at again"
    );
    assert!(db.finish_report("run-told", "a", &later).await.unwrap());
    hold_over(db, "run-told").await;
    assert_eq!(claim("a").await, None, "and only once");

    // A look the forge rate-limited holds the run for the wait it said: a
    // notice does not bring the next look in sooner.
    make_due(db, "run-told").await;
    assert_eq!(claim("a").await.as_deref(), Some("run-told"));
    let limited = ReportWrite {
        error: Some("rate limited".into()),
        next: Some(Duration::from_secs(300)),
        ..ReportWrite::default()
    };
    assert!(db.finish_report("run-told", "a", &limited).await.unwrap());
    assert_eq!(tell(&["run-told"]).await, 1);
    assert!(!due_now(db, "run-told").await, "held for the forge's wait");

    // A record that follows nothing more is not made due by a notice.
    make_due(db, "run-told").await;
    assert_eq!(claim("a").await.as_deref(), Some("run-told"));
    assert!(db
        .finish_report("run-told", "a", &ReportWrite::default())
        .await
        .unwrap());
    hold_over(db, "run-told").await;
    assert_eq!(tell(&["run-told"]).await, 0);
    assert_eq!(claim("a").await, None);
}

#[tokio::test]
async fn a_run_told_of_a_change_is_looked_at_and_one_told_mid_look_again() {
    let (db, _dir) = sqlite().await;
    the_tell_contract(&db).await;
}

#[tokio::test]
async fn a_run_told_of_a_change_is_looked_at_and_one_told_mid_look_again_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    the_tell_contract(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

/// A run no look of which was written yet is looked at before any other
/// that is due, however long that one has waited: its jobs are to read
/// pending before they end, and a run told of its changes is due as often as
/// they come. One looked at that posted nothing (a repository that
/// takes no statuses, a look that failed) is not a new run.
async fn the_first_look_contract(db: &Db) {
    let (_, ev) = claimed(db, None, Kind::Push, Trust::Trusted).await;
    record(db, &ev, "run-busy").await;
    let (id, _) = db
        .claim_report("a", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id, "run-busy");
    let posted = ReportWrite {
        error: Some("the repository takes no statuses".into()),
        next: Some(Duration::from_secs(600)),
        ..ReportWrite::default()
    };
    assert!(db.finish_report("run-busy", "a", &posted).await.unwrap());
    // Due since long ago, and told of a change since.
    make_due(db, "run-busy").await;
    record(db, &ev, "run-new").await;
    let first = db
        .claim_report("a", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap()
        .0;
    let second = db
        .claim_report("a", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!((first.as_str(), second.as_str()), ("run-new", "run-busy"));
}

#[tokio::test]
async fn a_run_s_first_look_comes_before_any_other_s() {
    let (db, _dir) = sqlite().await;
    the_first_look_contract(&db).await;
}

#[tokio::test]
async fn a_run_s_first_look_comes_before_any_other_s_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    the_first_look_contract(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

/// A dagron-api's outbox, as the feed claims it: `events` handed out once,
/// then none; the acknowledged ids kept. `status` answers every claim
/// instead, where set.
async fn outbox_stub(
    events: Value,
    status: Option<StatusCode>,
) -> (String, Arc<Mutex<Vec<Value>>>) {
    #[derive(Clone)]
    struct St {
        events: Arc<Mutex<Option<Value>>>,
        acked: Arc<Mutex<Vec<Value>>>,
        status: Option<StatusCode>,
    }
    async fn claim(State(st): State<St>) -> Response {
        if let Some(code) = st.status {
            return code.into_response();
        }
        let events = st
            .events
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| json!([]));
        axum::Json(json!({ "events": events })).into_response()
    }
    async fn ack(State(st): State<St>, axum::Json(body): axum::Json<Value>) -> StatusCode {
        st.acked.lock().unwrap().push(body["ids"].clone());
        StatusCode::NO_CONTENT
    }
    let st = St {
        events: Arc::new(Mutex::new(Some(events))),
        acked: Arc::default(),
        status,
    };
    let acked = st.acked.clone();
    let app = Router::new()
        .route("/api/outbox/ci/claim", axum::routing::post(claim))
        .route("/api/outbox/ci/ack", axum::routing::post(ack))
        .with_state(st);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (origin, acked)
}

/// The feed makes each told run's record due once and acknowledges what it
/// was handed; a dagron-api with no outbox to hand is `Absent`, and one that
/// fails is an error, which leaves nothing acknowledged.
#[tokio::test]
async fn the_feed_tells_the_records_and_acknowledges_what_it_was_handed() {
    let (db, _dir) = sqlite().await;
    let ev = recorded(&db, "run-fed").await;
    let _ = ev;
    let (id, _) = db
        .claim_report("a", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id, "run-fed");
    let later = ReportWrite {
        next: Some(Duration::from_secs(600)),
        ..ReportWrite::default()
    };
    assert!(db.finish_report("run-fed", "a", &later).await.unwrap());
    hold_over(&db, "run-fed").await;

    let (origin, acked) = outbox_stub(
        json!([
            { "id": "n1", "run_id": "run-fed", "type": "task.started", "payload": {}, "created_at": "x" },
            { "id": "n2", "run_id": "run-fed", "type": "task.finished", "payload": {}, "created_at": "x" },
            { "id": "n3", "run_id": "run-other", "type": "run.completed", "payload": {}, "created_at": "x" }
        ]),
        None,
    )
    .await;
    let (reporter, metrics) = reporter(&db, &origin, None);
    assert_eq!(
        reporter.feed_once("default", &origin).await.unwrap(),
        feed::Fed::Told(3)
    );
    assert!(due_now(&db, "run-fed").await, "its record is due");
    assert_eq!(*acked.lock().unwrap(), [json!(["n1", "n2", "n3"])]);
    assert!(metrics
        .render(None, &[])
        .contains("dagron_ci_report_notices_total 3\n"));
    assert_eq!(
        reporter.feed_once("default", &origin).await.unwrap(),
        feed::Fed::Told(0)
    );

    let (absent, _) = outbox_stub(json!([]), Some(StatusCode::NOT_FOUND)).await;
    assert_eq!(
        reporter.feed_once("default", &absent).await.unwrap(),
        feed::Fed::Absent
    );
    let (failing, acked) = outbox_stub(json!([]), Some(StatusCode::INTERNAL_SERVER_ERROR)).await;
    assert!(reporter.feed_once("default", &failing).await.is_err());
    assert!(acked.lock().unwrap().is_empty());
}

/// A failed run that raises a `failed` event raises it once the look
/// that sees it fail is written, with what its cell says of its failure; a
/// run whose failure raises nothing, and one that succeeded, raise nothing.
async fn the_failed_contract(db: &Db) {
    let (origin, st) = stub().await;
    let (reporter, _) = reporter(db, &origin, Some("ghp_test"));
    let (_, ev) = claimed(db, None, Kind::Push, Trust::Trusted).await;
    for (run, raises) in [("run-1", true), ("run-2", false), ("run-3", true)] {
        let mut submitted = SubmittedRun {
            run_id: run.into(),
            event_id: ev.id.clone(),
            repo_id: ev.repo.id.clone(),
            cell: "default".into(),
            pipeline: ".dagron/ci/ci.yaml".into(),
            idempotency_key: format!("key-{run}"),
            on_failed: false,
        };
        submitted.on_failed = raises;
        assert!(db.record_run(&submitted).await.unwrap());
    }
    set_run(&st, "run-1", &[("test", "failed")], Some("failed"));
    set_run(&st, "run-2", &[("test", "failed")], Some("failed"));
    set_run(&st, "run-3", &[("test", "succeeded")], Some("succeeded"));
    st.lock().unwrap().failures.insert(
        "run-1".into(),
        json!({ "task_name": "test", "fault_class": "user-code", "disposition": "application" }),
    );
    for _ in 0..3 {
        assert!(reporter.report_one("a").await.unwrap());
    }
    assert_eq!(reporter.raise_failures().await.unwrap(), 1, "run-1 alone");
    assert_eq!(reporter.raise_failures().await.unwrap(), 0, "and once");
    let failed = db
        .claim_event("w", Duration::from_secs(60))
        .await
        .unwrap()
        .expect("the failed event is queued");
    assert_eq!(
        (failed.kind.as_str(), failed.trust.as_str()),
        ("failed", "trusted")
    );
    assert_eq!(failed.sha, ev.sha);
    assert_eq!(
        failed.inputs,
        BTreeMap::from([
            (
                "ci_failed_disposition".to_string(),
                "application".to_string()
            ),
            ("ci_failed_fault_class".to_string(), "user-code".to_string()),
            ("ci_failed_job".to_string(), "test".to_string()),
            ("ci_failed_pipeline".to_string(), "ci".to_string()),
            ("ci_failed_run".to_string(), "run-1".to_string()),
        ])
    );
}

#[tokio::test]
async fn a_failed_run_raises_its_failed_event_once() {
    let (db, _dir) = sqlite().await;
    the_failed_contract(&db).await;
}

#[tokio::test]
async fn a_failed_run_raises_its_failed_event_once_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    the_failed_contract(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

// ── Flaky jobs' issues ───────────────────────────────────────────────────────

/// A reporter that keeps flakes and files an issue of a job at its second,
/// as the App, for `issues: write` alone.
fn reporter_with_flakes(db: &Db, origin: &str) -> Option<Reporter> {
    let dir = tempfile::tempdir().unwrap();
    let (private, _) = crate::checkout::tests::rsa_key(dir.path())?;
    let app = crate::checkout::GitHubApp::new(4242, &private).unwrap();
    let forge = Forge::new(Some("ghp_test".into()))
        .with_origin(origin)
        .with_app(Some(app))
        .with_flake_issues(true);
    Some(
        Reporter::new(
            db.clone(),
            BTreeMap::from([("default".to_string(), origin.to_string())]),
            "dgr_token".into(),
            Arc::new(forge),
            Some("https://ci.example/runs/{run_id}".into()),
            Arc::new(Metrics::default()),
        )
        .with_flakes(2, 14),
    )
}

/// `run_id` of `ev` ended: `test` passed on its second attempt, after the
/// first failed; `build` passed on its first.
fn flaked(st: &Shared, run_id: &str) {
    set_run_at(
        st,
        run_id,
        &[("build", "succeeded", 1), ("test", "succeeded", 2)],
        Some("succeeded"),
    );
    st.lock().unwrap().attempts.insert(
        "t-test".into(),
        json!({ "current_attempt": 2, "attempts": [{ "attempt": 1, "reason": "failed" }] }),
    );
}

async fn flake_count(db: &Db) -> i64 {
    match db {
        Db::Sqlite(p) => sqlx::query_scalar("SELECT COUNT(*) FROM ci_flakes")
            .fetch_one(p)
            .await
            .unwrap(),
        Db::Postgres(p) => sqlx::query_scalar("SELECT COUNT(*) FROM ci_flakes")
            .fetch_one(p)
            .await
            .unwrap(),
    }
}

async fn flake_issue_number(db: &Db) -> Option<String> {
    match db {
        Db::Sqlite(p) => sqlx::query_scalar("SELECT issue FROM ci_flake_issues WHERE job = 'test'")
            .fetch_one(p)
            .await
            .unwrap(),
        Db::Postgres(p) => {
            sqlx::query_scalar("SELECT issue FROM ci_flake_issues WHERE job = 'test'")
                .fetch_one(p)
                .await
                .unwrap()
        }
    }
}

/// A trusted run's job that passed only after an attempt failed is kept
/// once a run; at the second, an issue of it is opened, as the App with a
/// token for issues alone, and written again in place at the third. One a
/// person closed is left so, and the flakes since it was closed open
/// another. A run that is not trusted keeps no flake.
async fn the_flake_contract(db: &Db) {
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_flakes(db, &origin) else {
        return;
    };
    let ev = recorded(db, "run-1").await;
    flaked(&st, "run-1");
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(flake_count(db).await, 1, "test flaked; build did not");
    assert!(
        !reporter.flake_issue_one("w").await.unwrap(),
        "one flake files nothing"
    );

    // Looked at again, the run's flake is the one it was.
    record_as(db, &ev, "run-2", ".dagron/ci/ci.yaml").await;
    flaked(&st, "run-2");
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(flake_count(db).await, 2);
    assert!(reporter.flake_issue_one("w").await.unwrap());
    let issues = st.lock().unwrap().issues.clone();
    assert_eq!(issues.len(), 1, "{issues:?}");
    let (how, number, title, said, auth) = &issues[0];
    assert_eq!(
        (how.as_str(), *number, title.as_str()),
        ("POST", 1, "Flaky job: test of ci")
    );
    assert!(said.starts_with(flake::MARK), "{said}");
    assert!(
        said.contains("**`test`** of `ci` passed only after an attempt of it failed, 2 times"),
        "{said}"
    );
    assert!(
        said.contains("[run-2](https://ci.example/runs/run-2)"),
        "{said}"
    );
    // The statuses' token first, then one for the issue alone.
    let minted = st.lock().unwrap().minted.clone();
    assert_eq!(
        minted.last(),
        Some(&json!({ "issues": "write" })),
        "{minted:?}"
    );
    assert_eq!(auth, &format!("Bearer ghs_{}", minted.len()));
    assert_eq!(flake_issue_number(db).await.as_deref(), Some("1"));
    assert!(
        !reporter.flake_issue_one("w").await.unwrap(),
        "written, it is due no more"
    );

    record_as(db, &ev, "run-3", ".dagron/ci/ci.yaml").await;
    flaked(&st, "run-3");
    assert!(reporter.report_one("a").await.unwrap());
    assert!(reporter.flake_issue_one("w").await.unwrap());
    let issues = st.lock().unwrap().issues.clone();
    assert_eq!(
        (issues[1].0.as_str(), issues[1].1),
        ("PATCH", 1),
        "the same issue"
    );
    assert!(issues[1].3.contains("3 times"), "{}", issues[1].3);

    // A person closed it: the flakes since then are counted, and are enough.
    st.lock()
        .unwrap()
        .closed_issues
        .insert(1, "2000-01-01T00:00:00Z".into());
    record_as(db, &ev, "run-4", ".dagron/ci/ci.yaml").await;
    flaked(&st, "run-4");
    assert!(reporter.report_one("a").await.unwrap());
    assert!(reporter.flake_issue_one("w").await.unwrap());
    let issues = st.lock().unwrap().issues.clone();
    assert_eq!(
        issues[2..]
            .iter()
            .map(|i| (i.0.as_str(), i.1))
            .collect::<Vec<_>>(),
        [("PATCH", 1), ("POST", 2)],
        "found closed, then another opened"
    );
    assert_eq!(flake_issue_number(db).await.as_deref(), Some("2"));

    // A pull request from a fork keeps no flake.
    let repo = db
        .find_repo("github.com", "acme/widgets")
        .await
        .unwrap()
        .unwrap();
    let mut fork = crate::db::tests::event();
    fork.kind = Kind::PullRequest;
    fork.trust = Trust::Untrusted;
    fork.git_ref = "refs/pull/8/head".into();
    fork.pr = Some(8);
    fork.sha = "d".repeat(40);
    db.record_delivery(&repo, &uuid::Uuid::now_v7().to_string(), &fork, b"{}")
        .await
        .unwrap();
    let fork = db
        .claim_event("w", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    record(db, &fork, "run-5").await;
    flaked(&st, "run-5");
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(
        flake_count(db).await,
        4,
        "an untrusted run's flake is not kept"
    );
}

/// A repository with its issues turned off answers a new one `410`: a
/// refusal, which the job is not asked of again for until it flakes again.
#[tokio::test]
async fn a_repository_with_its_issues_off_is_not_asked_again_on_a_backoff() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_flakes(&db, &origin) else {
        return;
    };
    st.lock().unwrap().issues_off = true;
    let ev = recorded(&db, "run-1").await;
    flaked(&st, "run-1");
    assert!(reporter.report_one("a").await.unwrap());
    record_as(&db, &ev, "run-2", ".dagron/ci/ci.yaml").await;
    flaked(&st, "run-2");
    assert!(reporter.report_one("a").await.unwrap());
    assert!(reporter.flake_issue_one("w").await.unwrap());
    assert!(st.lock().unwrap().issues.is_empty());
    let Db::Sqlite(p) = &db else { unreachable!() };
    let (due, error): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT due_at, error FROM ci_flake_issues WHERE job = 'test'")
            .fetch_one(p)
            .await
            .unwrap();
    assert_eq!(due, None, "not due again on a backoff");
    assert!(error.is_some_and(|e| e.contains("HTTP 410")));
}

#[tokio::test]
async fn a_job_that_keeps_flaking_is_filed_one_issue() {
    let (db, _dir) = sqlite().await;
    the_flake_contract(&db).await;
}

#[tokio::test]
async fn a_job_that_keeps_flaking_is_filed_one_issue_on_postgres() {
    let Some((db, schema, admin)) = postgres().await else {
        return;
    };
    the_flake_contract(&db).await;
    db.close().await;
    drop_schema(&admin, &schema).await;
}

/// A job that failed, and passed when a person reran the run, is a flake:
/// the rerun starts its attempts again from 1, so its pass is on its first
/// attempt of that life, and the failure is in the life before it.
#[tokio::test]
async fn a_job_that_failed_and_passed_on_a_person_s_rerun_is_a_flake() {
    let (db, _dir) = sqlite().await;
    let (origin, st) = stub().await;
    let Some(reporter) = reporter_with_flakes(&db, &origin) else {
        return;
    };
    recorded(&db, "run-1").await;
    set_run_at(
        &st,
        "run-1",
        &[("build", "succeeded", 1), ("test", "failed", 1)],
        Some("failed"),
    );
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(flake_count(&db).await, 0, "a failure alone is no flake");

    assert!(db.reopen_run("run-1", Duration::ZERO, None).await.unwrap());
    set_run_at(
        &st,
        "run-1",
        &[("build", "succeeded", 1), ("test", "succeeded", 1)],
        Some("succeeded"),
    );
    st.lock().unwrap().attempts.insert(
        "t-test".into(),
        json!({
            "current_attempt": 2,
            "attempts": [{ "attempt": 1, "reason": "rerun", "status": "failed" }]
        }),
    );
    assert!(reporter.report_one("a").await.unwrap());
    assert_eq!(flake_count(&db).await, 1, "test flaked on the rerun");
}
