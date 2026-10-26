//! The run API's log door. `GET /ci/v1/runs/{id}/logs` reads what an event's
//! runs wrote: each job's output, as its cell's dagron-api keeps it.
//!
//! **dagron-ci keeps no log.** A job's output is its engine's, and is read
//! where it is kept: the door asks the cell's dagron-api for each run's log
//! (`GET /api/runs/{id}/logs`) with the caller's own credential, and answers
//! with what that said, a run at a time. Who may read a log is that
//! dagron-api's rule, and so is how much of one is kept.
//!
//! **What a log is narrowed by.** `job` names a job, and may be repeated;
//! `status` keeps the jobs that read so (`failed`, mostly); `q` keeps the
//! lines that hold a text; `limit` caps the lines, and `tail` keeps the last
//! of them instead of the first. They are dagron-api's own, and it is the one
//! that reads them: what it refuses, the door refuses in its words.
//!
//! **Followed** (`follow=true`): one run's log as its cell's
//! dagron-api streams it (`GET /api/runs/{id}/logs/stream`, server-sent
//! events), as the caller, passed on as it comes. An id that names an event
//! of several runs is refused with their ids, each to be followed by its own:
//! `dagron-ci logs ID --follow` follows them all so. `from` and `cursor`
//! say where it begins, as dagron-api takes them.

use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures_util::StreamExt;
use serde_json::{json, Value};

use super::{bearer, database_error, identify, named};
use crate::problem::problem;
use crate::server::AppState;

/// How long a cell's dagron-api may take over one run's log.
const TIMEOUT: Duration = Duration::from_secs(30);

/// How many things one request may narrow a log by, and the longest of them.
const MAX_PARAMS: usize = 32;
const MAX_VALUE: usize = 512;
/// The most cursors a followed log goes on from (one a job), counted apart.
const MAX_CURSORS: usize = 1000;
/// How long a cell's dagron-api may take to begin a followed log.
const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

/// Whether the request follows the log.
fn follows(pairs: &[(String, String)]) -> bool {
    pairs
        .iter()
        .any(|(name, value)| name == "follow" && !matches!(value.as_str(), "" | "0" | "false"))
}

/// What a cell's dagron-api answered to a read of a run's log.
enum Read {
    /// The log, as dagron-api gives it.
    Log(Value),
    /// It refused the request itself (400), in these words.
    Refused(String),
    /// It has no such run.
    Gone,
    /// The caller's credential was not accepted for it (401 or 403).
    Denied(u16),
}

/// What dagron-api is asked with, from what the door was: `job` is its
/// `task`, and the rest go as they came; a followed log (`follow`) takes
/// `from` and `cursor` besides. `Err` is why the request is refused.
fn narrowed(pairs: &[(String, String)], follow: bool) -> Result<Vec<(&'static str, &str)>, String> {
    let cursors = pairs.iter().filter(|(name, _)| name == "cursor").count();
    if pairs.len() - cursors > MAX_PARAMS {
        return Err(format!(
            "a log is narrowed by at most {MAX_PARAMS} parameters"
        ));
    }
    if cursors > MAX_CURSORS {
        return Err(format!(
            "a followed log goes on from at most {MAX_CURSORS} cursors"
        ));
    }
    let mut ask = Vec::with_capacity(pairs.len());
    for (name, value) in pairs {
        if value.len() > MAX_VALUE {
            return Err(format!("{name} is longer than {MAX_VALUE} bytes"));
        }
        let name = match name.as_str() {
            "job" => "task",
            "status" => "status",
            "q" => "q",
            "limit" => "limit",
            "tail" => "tail",
            "follow" => continue,
            "from" if follow => "from",
            "cursor" if follow => "cursor",
            "step" => {
                return Err("a job's steps write one log: narrow it with job, and with q".into())
            }
            other => {
                return Err(format!(
                    "{other:?} is not something a log is narrowed by: job, status, q, limit, tail{}",
                    if follow { "; and a followed one, from and cursor" } else { "" }
                ))
            }
        };
        ask.push((name, value.as_str()));
    }
    Ok(ask)
}

/// Read `run_id`'s log from the dagron-api at `base`, as `credential`: the
/// caller's own. `Err` when it could not be asked, or answered something else.
async fn read(
    http: &reqwest::Client,
    base: &str,
    run_id: &str,
    credential: &str,
    ask: &[(&'static str, &str)],
) -> Result<Read, String> {
    if !crate::rerun::is_run_id(run_id) {
        return Ok(Read::Gone);
    }
    let url = format!("{base}/api/runs/{run_id}/logs");
    let response = http
        .get(&url)
        .query(ask)
        .bearer_auth(credential)
        .timeout(TIMEOUT)
        .send()
        .await
        .map_err(|e| format!("dagron-api at {url}: {e}"))?;
    match response.status().as_u16() {
        200 => response
            .json()
            .await
            .map(Read::Log)
            .map_err(|e| format!("dagron-api at {url} answered something else: {e}")),
        400 => Ok(Read::Refused(
            response
                .text()
                .await
                .unwrap_or_default()
                .trim()
                .chars()
                .take(300)
                .collect(),
        )),
        404 => Ok(Read::Gone),
        code @ (401 | 403) => Ok(Read::Denied(code)),
        code => Err(format!("dagron-api at {url}: HTTP {code}")),
    }
}

/// `GET /ci/v1/runs/{id}/logs`: the log of each of an event's runs, or of the
/// one run `id` names. Any identified caller may ask, viewers included, and
/// dagron-api says who may read.
///
/// 200 with the event's `id`, `state` and `reason`, and `runs`: each with its
/// `id`, `cell`, `pipeline` and `status` as dagron-ci last saw it, and either
/// `logs`, which is dagron-api's own answer (its jobs as `tasks`, its `lines`,
/// `total`, `matched`, `truncated`, `eof`, `limit`), or `error`, which is why
/// that run's log was not read. An event that has submitted nothing has no
/// runs, and its `state` and `reason` say where it stands.
///
/// 400 for a request the door or dagron-api does not take. When no run's log
/// could be read: 503 if dagron-api could not be asked, and the caller's own
/// 401 or 403 if it let them read none.
pub async fn run_logs(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
    headers: HeaderMap,
) -> Response {
    let Some(credential) = bearer(&headers) else {
        return problem(
            StatusCode::UNAUTHORIZED,
            "not authenticated",
            "send a dagron-api session or API token as Authorization: Bearer",
        );
    };
    let found = match named(&st, &id).await {
        Ok(found) => found,
        Err(e) => return database_error(e).1,
    };
    // Nothing of the event is told to a caller who has not been identified.
    let cell = found.as_ref().map(|named| named.event.cell.as_str());
    if let Err((_, response)) = identify(&st, cell, credential).await {
        return response;
    }
    let follow = follows(&pairs);
    let ask = match narrowed(&pairs, follow) {
        Ok(ask) => ask,
        Err(why) => return problem(StatusCode::BAD_REQUEST, "not a log request", why),
    };
    let Some(found) = found.map(|named| named.event) else {
        return problem(
            StatusCode::NOT_FOUND,
            "no such run",
            format!("no event {id:?}"),
        );
    };
    if follow {
        return followed(&st, &found, credential, &ask).await;
    }

    let mut runs = Vec::with_capacity(found.runs.len());
    let (mut any, mut denied, mut unavailable) = (false, None, None);
    for run in &found.runs {
        let mut entry = json!({
            "id": run.id,
            "cell": run.cell,
            "pipeline": run.pipeline,
            "status": run.status,
        });
        let answer = match st.cells.get(&run.cell) {
            Some(base) => read(st.http.to(base), base, &run.id, credential, &ask).await,
            None => Err(format!("cell {:?} is not in CI_CELLS", run.cell)),
        };
        match answer {
            Ok(Read::Log(log)) => {
                any = true;
                entry["logs"] = log;
            }
            // The request is the same for every run: one refusal is all of them.
            Ok(Read::Refused(said)) => {
                return problem(
                    StatusCode::BAD_REQUEST,
                    "not a log request",
                    if said.is_empty() {
                        "dagron-api refused the request".to_string()
                    } else {
                        said
                    },
                )
            }
            Ok(Read::Gone) => {
                entry["error"] = json!("its cell's dagron-api has no such run");
            }
            Ok(Read::Denied(code)) => {
                denied.get_or_insert(code);
                entry["error"] = json!("dagron-api did not let this caller read it");
            }
            Err(why) => {
                tracing::warn!(run_id = %run.id, reason = %why, "a run's log could not be read");
                unavailable = Some(why);
                entry["error"] = json!("dagron-api could not be asked for it");
            }
        }
        runs.push(entry);
    }

    if !any && unavailable.is_some() {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "dagron-api could not be asked",
            "no run's log could be read; try again",
        );
    }
    if let (false, Some(code)) = (any, denied) {
        let (status, title) = match code {
            401 => (StatusCode::UNAUTHORIZED, "not authenticated"),
            _ => (StatusCode::FORBIDDEN, "not allowed"),
        };
        return problem(
            status,
            title,
            format!(
                "dagron-api did not let this caller read a log of event {}",
                found.id
            ),
        );
    }
    (
        StatusCode::OK,
        Json(json!({
            "id": found.id,
            "state": found.state,
            "reason": found.reason,
            "runs": runs,
        })),
    )
        .into_response()
}

/// `follow=true`: the one run `event` names, its log as its cell's
/// dagron-api streams it, as `credential`, passed on as it comes, until it
/// ends, or dagron-ci stops (a client goes on from its cursor).
///
/// 409 for an event that has submitted no run yet; 400 for one of several,
/// with their ids. What dagron-api answers instead of a stream is answered so:
/// 400 in its words, 401, 403, 404, 429 with its `Retry-After`; 503 where it
/// could not be asked.
async fn followed(
    st: &AppState,
    event: &crate::db::EventView,
    credential: &str,
    ask: &[(&'static str, &str)],
) -> Response {
    let run = match event.runs.as_slice() {
        [] => {
            return problem(
                StatusCode::CONFLICT,
                "nothing to follow yet",
                format!(
                    "event {} is {} and has submitted no run: ask again",
                    event.id, event.state
                ),
            )
        }
        [run] => run,
        runs => {
            let ids: Vec<&str> = runs.iter().map(|run| run.id.as_str()).collect();
            return problem(
                StatusCode::BAD_REQUEST,
                "not a log request",
                format!(
                    "event {} has {} runs, and a log is followed a run at a time: follow each by its id ({})",
                    event.id,
                    runs.len(),
                    ids.join(", ")
                ),
            );
        }
    };
    let Some(base) = st.cells.get(&run.cell) else {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "dagron-api could not be asked",
            format!("cell {:?} is not in CI_CELLS", run.cell),
        );
    };
    if !crate::rerun::is_run_id(&run.id) {
        return problem(
            StatusCode::NOT_FOUND,
            "no such run",
            "its cell's dagron-api has no such run",
        );
    }
    let url = format!("{base}/api/runs/{}/logs/stream", run.id);
    let opened = tokio::time::timeout(
        OPEN_TIMEOUT,
        st.follow_http
            .to(base)
            .get(&url)
            .query(ask)
            .bearer_auth(credential)
            .header(header::ACCEPT, "text/event-stream")
            .send(),
    )
    .await;
    let response = match opened {
        Ok(Ok(response)) => response,
        Ok(Err(e)) => {
            tracing::warn!(run_id = %run.id, error = %e, "a run's log could not be followed");
            return problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "dagron-api could not be asked",
                "the run's log could not be followed; try again",
            );
        }
        Err(_) => {
            return problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "dagron-api could not be asked",
                "the run's cell did not begin its log in time; try again",
            )
        }
    };
    let status = response.status().as_u16();
    if status != 200 {
        let retry = response.headers().get(header::RETRY_AFTER).cloned();
        let said: String = response
            .text()
            .await
            .unwrap_or_default()
            .trim()
            .chars()
            .take(300)
            .collect();
        let mut refused = match status {
            400 => problem(StatusCode::BAD_REQUEST, "not a log request", said),
            401 => problem(
                StatusCode::UNAUTHORIZED,
                "not authenticated",
                "dagron-api did not take this caller",
            ),
            403 => problem(
                StatusCode::FORBIDDEN,
                "not allowed",
                "dagron-api did not let this caller read it",
            ),
            404 => problem(
                StatusCode::NOT_FOUND,
                "no such run",
                "its cell's dagron-api has no such run",
            ),
            429 => problem(
                StatusCode::TOO_MANY_REQUESTS,
                "dagron-api is busy",
                "its cell follows as many runs as it may: ask again",
            ),
            code => {
                tracing::warn!(run_id = %run.id, code, "a run's log could not be followed");
                problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "dagron-api could not be asked",
                    format!("the run's cell answered HTTP {code}; try again"),
                )
            }
        };
        if let (429, Some(retry)) = (status, retry) {
            refused.headers_mut().insert(header::RETRY_AFTER, retry);
        }
        return refused;
    }
    let mut stop = st.stop.clone();
    let stopped = async move {
        while !*stop.borrow_and_update() {
            if stop.changed().await.is_err() {
                // No server is stopping it: it lasts as long as its run.
                std::future::pending::<()>().await;
            }
        }
    };
    let body = response.bytes_stream().take_until(Box::pin(stopped));
    let mut out = Response::new(Body::from_stream(body));
    let headers = out.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    out
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::extract::RawQuery;
    use axum::http::Request;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    use super::*;
    use crate::config::Config;
    use crate::db::tests::sqlite;
    use crate::db::{Claimed, Db, Repo, SubmittedRun};
    use crate::dispatch::tests::claimed;
    use crate::event::{Kind, Trust};
    use crate::server::router;
    use crate::Seams;

    /// A cell's dagron-api, as much of it as reading a log takes.
    #[derive(Default)]
    struct Cell {
        /// Each run it has, and the one line its log holds.
        runs: BTreeMap<String, String>,
        /// Each read asked for: the run, its query, and the credential.
        asked: Vec<(String, String, String)>,
        /// What every read is answered, in place of the log.
        answers: Option<u16>,
        /// How long a followed log waits between its first event and its
        /// last.
        pause: std::time::Duration,
    }

    /// What the stub cell's followed log sends: a start, and, after its
    /// pause, its end.
    const STREAMED: [&str; 2] = [
        "event: start\ndata: {\"task_id\":\"t-1\",\"task\":\"build\",\"cursor\":\"t-1:3:1:0\"}\n\n",
        "event: eof\ndata: {\"run_id\":\"run-a\"}\n\n",
    ];

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
                _ => return StatusCode::UNAUTHORIZED.into_response(),
            };
            Json(json!({ "sub": sub, "email": "", "groups": groups })).into_response()
        }
        async fn logs(
            State(cell): State<StubCell>,
            Path(id): Path<String>,
            RawQuery(query): RawQuery,
            headers: HeaderMap,
        ) -> Response {
            let mut cell = cell.lock().unwrap();
            let query = query.unwrap_or_default();
            cell.asked.push((id.clone(), query.clone(), auth(&headers)));
            if let Some(code) = cell.answers {
                return (
                    StatusCode::from_u16(code).unwrap(),
                    "limit must be a number",
                )
                    .into_response();
            }
            match cell.runs.get(&id) {
                Some(line) => Json(json!({
                    "run_id": id,
                    "tasks": [{ "id": "t-1", "name": "build", "status": "failed" }],
                    "lines": [{ "task": "build", "attempt": 1, "n": 1, "text": line }],
                    "total": 1,
                    "matched": 1,
                    "truncated": false,
                    "eof": true,
                    "limit": 2000,
                }))
                .into_response(),
                None => (StatusCode::NOT_FOUND, "run not found").into_response(),
            }
        }
        async fn stream(
            State(cell): State<StubCell>,
            Path(id): Path<String>,
            RawQuery(query): RawQuery,
            headers: HeaderMap,
        ) -> Response {
            let (answers, pause, known) = {
                let mut cell = cell.lock().unwrap();
                cell.asked.push((
                    format!("{id}/stream"),
                    query.unwrap_or_default(),
                    auth(&headers),
                ));
                (cell.answers, cell.pause, cell.runs.contains_key(&id))
            };
            if let Some(code) = answers {
                let mut refused = (
                    StatusCode::from_u16(code).unwrap(),
                    "from=middle: start, end, or tail",
                )
                    .into_response();
                if code == 429 {
                    refused
                        .headers_mut()
                        .insert("retry-after", "7".parse().unwrap());
                }
                return refused;
            }
            if !known {
                return (StatusCode::NOT_FOUND, "run not found").into_response();
            }
            let frames = futures_util::stream::unfold(0, move |n| async move {
                match n {
                    0 => Some((Ok::<_, std::io::Error>(STREAMED[0].to_string()), 1)),
                    1 => {
                        tokio::time::sleep(pause).await;
                        Some((Ok(STREAMED[1].to_string()), 2))
                    }
                    _ => None,
                }
            });
            (
                [("content-type", "text/event-stream")],
                Body::from_stream(frames),
            )
                .into_response()
        }
        let cell: StubCell = Arc::default();
        let app = Router::new()
            .route("/api/me", get(me))
            .route("/api/runs/{id}/logs", get(logs))
            .route("/api/runs/{id}/logs/stream", get(stream))
            .with_state(cell.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, cell)
    }

    struct Rig {
        app: Router,
        cell: StubCell,
        db: Db,
        repo: Repo,
        event: Claimed,
        _dirs: (tempfile::TempDir, tempfile::TempDir),
    }

    async fn rig() -> Rig {
        let (db, db_dir) = sqlite().await;
        let (base, cell) = stub_cell().await;
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "dgr_dagron_ci\n").unwrap();
        let config = Config::from_lookup(|name| match name {
            "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
            "CI_CELLS" => Some(format!("default={base}")),
            "CI_API_TOKEN_FILE" => Some(token.to_str().unwrap().into()),
            _ => None,
        })
        .unwrap();
        let state = AppState::new(db.clone(), &config, Seams::default());
        let (repo, event) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        Rig {
            app: router(state),
            cell,
            db,
            repo,
            event,
            _dirs: (db_dir, dir),
        }
    }

    impl Rig {
        /// A run of the event, whose log in its cell is the one line `line`;
        /// or, without one, a run its cell does not have.
        async fn submitted(&self, run_id: &str, line: Option<&str>) {
            assert!(self
                .db
                .record_run(&SubmittedRun {
                    run_id: run_id.into(),
                    event_id: self.event.id.clone(),
                    repo_id: self.repo.id.clone(),
                    cell: "default".into(),
                    pipeline: format!(".dagron/ci/{run_id}.yaml"),
                    idempotency_key: format!("ci-{run_id}"),
                    on_failed: false,
                })
                .await
                .unwrap());
            if let Some(line) = line {
                self.cell
                    .lock()
                    .unwrap()
                    .runs
                    .insert(run_id.into(), line.into());
            }
        }

        /// `GET /ci/v1/runs/{id}/logs{query}` of a followed log: its status,
        /// content type, `Retry-After` and body as text.
        async fn followed(&self, id: &str, query: &str) -> (StatusCode, String, String, String) {
            let req = Request::get(format!("/ci/v1/runs/{id}/logs{query}"))
                .header("authorization", "Bearer dgr_viewer");
            let res = self
                .app
                .clone()
                .oneshot(req.body(Body::empty()).unwrap())
                .await
                .unwrap();
            let header = |name: &str| {
                res.headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string()
            };
            let (status, kind, retry) =
                (res.status(), header("content-type"), header("retry-after"));
            let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
                .await
                .unwrap();
            (
                status,
                kind,
                retry,
                String::from_utf8_lossy(&bytes).into_owned(),
            )
        }

        /// `GET /ci/v1/runs/{id}/logs{query}`.
        async fn logs(&self, id: &str, query: &str, bearer: Option<&str>) -> (StatusCode, Value) {
            let mut req = Request::get(format!("/ci/v1/runs/{id}/logs{query}"));
            if let Some(bearer) = bearer {
                req = req.header("authorization", format!("Bearer {bearer}"));
            }
            let res = self
                .app
                .clone()
                .oneshot(req.body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = res.status();
            let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
                .await
                .unwrap();
            (
                status,
                serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            )
        }

        fn asked(&self) -> Vec<(String, String, String)> {
            std::mem::take(&mut self.cell.lock().unwrap().asked)
        }
    }

    #[tokio::test]
    async fn the_log_door_reads_each_run_s_log_from_its_cell_as_the_caller() {
        let rig = rig().await;
        let event = rig.event.id.clone();
        let viewer = Some("dgr_viewer");

        // Nobody is told anything before dagron-api says who they are.
        assert_eq!(rig.logs(&event, "", None).await.0, StatusCode::UNAUTHORIZED);
        for id in [event.as_str(), "no-such-event"] {
            assert_eq!(
                rig.logs(id, "", Some("dgr_nobody")).await.0,
                StatusCode::UNAUTHORIZED
            );
        }
        assert_eq!(
            rig.logs("no-such-event", "", viewer).await.0,
            StatusCode::NOT_FOUND
        );

        // An event that has submitted nothing has no log, and says where it
        // stands.
        let (status, body) = rig.logs(&event, "", viewer).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({ "id": event, "state": "claimed", "reason": null, "runs": [] })
        );

        // Each of its runs is read from its cell, by a viewer too, with the
        // caller's credential; and one its cell does not have says so.
        rig.submitted("run-a", Some("error: no such file")).await;
        rig.submitted("run-gone", None).await;
        let (status, body) = rig.logs(&event, "", viewer).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["runs"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["runs"][0],
            json!({
                "id": "run-a",
                "cell": "default",
                "pipeline": ".dagron/ci/run-a.yaml",
                "status": "submitted",
                "logs": {
                    "run_id": "run-a",
                    "tasks": [{ "id": "t-1", "name": "build", "status": "failed" }],
                    "lines": [{
                        "task": "build", "attempt": 1, "n": 1, "text": "error: no such file",
                    }],
                    "total": 1,
                    "matched": 1,
                    "truncated": false,
                    "eof": true,
                    "limit": 2000,
                },
            })
        );
        assert_eq!(
            body["runs"][1],
            json!({
                "id": "run-gone",
                "cell": "default",
                "pipeline": ".dagron/ci/run-gone.yaml",
                "status": "submitted",
                "error": "its cell's dagron-api has no such run",
            })
        );
        assert_eq!(
            rig.asked(),
            [
                (
                    "run-a".to_string(),
                    String::new(),
                    "Bearer dgr_viewer".to_string()
                ),
                (
                    "run-gone".to_string(),
                    String::new(),
                    "Bearer dgr_viewer".to_string()
                ),
            ]
        );

        // What a log is narrowed by goes on as dagron-api reads it: a job is
        // its task.
        let (status, _) = rig
            .logs(
                &event,
                "?job=build&job=unit%20tests&status=failed&q=a%26b&limit=50&tail=true&follow=false",
                viewer,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            rig.asked()[0].1,
            "task=build&task=unit+tests&status=failed&q=a%26b&limit=50&tail=true"
        );

        // An engine run's id, the one a commit status links to, names that
        // run alone.
        let (status, body) = rig.logs("run-a", "", viewer).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["id"], json!(event));
        assert_eq!(body["runs"].as_array().unwrap().len(), 1);
        assert_eq!(body["runs"][0]["id"], "run-a");
        assert_eq!(rig.asked().len(), 1);
    }

    #[tokio::test]
    async fn the_log_door_refuses_what_is_not_built_and_passes_on_what_its_cell_says() {
        let rig = rig().await;
        let event = rig.event.id.clone();
        let operator = Some("dgr_operator");
        rig.submitted("run-a", Some("ok")).await;

        // What the door does not take, it says, and asks its cell nothing.
        for (query, says) in [
            (
                "?cursor=t-1:3:1:0",
                "\"cursor\" is not something a log is narrowed by",
            ),
            (
                "?from=start",
                "\"from\" is not something a log is narrowed by",
            ),
            ("?follow=true&step=2", "a job's steps write one log"),
            ("?step=2", "a job's steps write one log"),
            (
                "?regex=.%2A",
                "\"regex\" is not something a log is narrowed by",
            ),
        ] {
            let (status, body) = rig.logs(&event, query, operator).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
            assert!(body["detail"].as_str().unwrap().contains(says), "{body}");
        }
        let long = format!("?q={}", "x".repeat(MAX_VALUE + 1));
        assert_eq!(
            rig.logs(&event, &long, operator).await.0,
            StatusCode::BAD_REQUEST
        );
        assert!(rig.asked().is_empty());
        // Someone not identified is told nothing of what the door takes.
        assert_eq!(
            rig.logs(&event, "?follow=true", Some("dgr_nobody")).await.0,
            StatusCode::UNAUTHORIZED
        );

        // What its cell refuses, the door refuses in the cell's words.
        rig.cell.lock().unwrap().answers = Some(400);
        let (status, body) = rig.logs(&event, "?limit=many", operator).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["detail"], "limit must be a number");

        // A cell that lets the caller read none of it is the caller's
        // refusal, and one that cannot be asked is worth another try.
        rig.cell.lock().unwrap().answers = Some(403);
        assert_eq!(
            rig.logs(&event, "", operator).await.0,
            StatusCode::FORBIDDEN
        );
        rig.cell.lock().unwrap().answers = Some(401);
        assert_eq!(
            rig.logs(&event, "", operator).await.0,
            StatusCode::UNAUTHORIZED
        );
        rig.cell.lock().unwrap().answers = Some(502);
        let (status, body) = rig.logs(&event, "", operator).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    }

    /// `follow=true`: the one run's log as its cell streams it, as the caller,
    /// passed on byte for byte, for as long as it lasts (longer than the
    /// door's own calls may); what the cell answers instead, answered so; an
    /// event with no run yet, and one of several, said so.
    #[tokio::test]
    async fn the_log_door_follows_one_run_s_log_as_its_cell_streams_it() {
        let rig = rig().await;
        let event = rig.event.id.clone();
        let (status, _, _, body) = rig.followed(&event, "?follow=true").await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert!(body.contains("has submitted no run"), "{body}");

        rig.submitted("run-a", Some("ok")).await;
        rig.cell.lock().unwrap().pause = std::time::Duration::from_secs(6);
        let query = "?follow=true&job=build&from=start&cursor=t-1:3:1:10";
        let (status, kind, _, body) = rig.followed(&event, query).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(kind, "text/event-stream");
        assert_eq!(
            body,
            STREAMED.concat(),
            "passed on as it came, over six seconds"
        );
        assert_eq!(
            rig.asked(),
            [(
                "run-a/stream".to_string(),
                "task=build&from=start&cursor=t-1%3A3%3A1%3A10".to_string(),
                "Bearer dgr_viewer".to_string()
            ),]
        );
        rig.cell.lock().unwrap().pause = std::time::Duration::ZERO;

        // What the cell answers in place of a stream.
        for (code, want) in [
            (400, StatusCode::BAD_REQUEST),
            (403, StatusCode::FORBIDDEN),
            (429, StatusCode::TOO_MANY_REQUESTS),
            (500, StatusCode::SERVICE_UNAVAILABLE),
        ] {
            rig.cell.lock().unwrap().answers = Some(code);
            let (status, _, retry, body) = rig.followed(&event, "?follow=true&from=middle").await;
            assert_eq!(status, want, "{code}: {body}");
            if code == 429 {
                assert_eq!(retry, "7", "its time to ask again");
            }
            if code == 400 {
                assert!(body.contains("from=middle"), "in its words: {body}");
            }
        }
        rig.cell.lock().unwrap().answers = None;

        // An event of two runs: each is followed by its own id.
        rig.submitted("run-b", Some("ok")).await;
        let (status, _, _, body) = rig.followed(&event, "?follow=true").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("run-a, run-b"), "{body}");
        let (status, _, _, body) = rig.followed("run-a", "?follow=true").await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
}
