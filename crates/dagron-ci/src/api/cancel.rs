//! The run API's cancel door. `POST /ci/v1/runs/{id}/cancel` stops what an
//! event set going: each of its runs that is still running, and the event
//! itself if it is still waiting to be dispatched.
//!
//! **A run is cancelled by its cell's dagron-api**, asked with the caller's
//! own credential, as a rerun is: who may cancel is that dagron-api's rule,
//! and the engine's cancel is the one cancel there is. dagron-ci writes
//! nothing of the run. Its record follows the run as it follows any other:
//! the reporter sees it end `cancelled`, says so on the commit, lets its slot
//! go and tells the meter.
//!
//! **Each run is asked about, whatever dagron-ci's record of it says.** A run
//! on record as over may be running again, moved through dagron-api and not
//! seen yet (`rerun.rs`). dagron-api knows, and its cancel of a run that has
//! ended changes nothing.
//!
//! **A cancel asks.** dagron-api calls a job off at once when no worker holds
//! it, and asks a worker to stop one it holds. The run reads `running` until
//! the last of those has stopped (`stopping` counts them), and so does
//! dagron-ci's record.
//!
//! **An event that is waiting is withdrawn.** One no worker holds (it was
//! just queued, or waits for a slot or for another try) ends `skipped`, with
//! who cancelled it, and submits nothing more. One a worker is dispatching
//! now is that worker's: the runs it has submitted are cancelled, a run it
//! submits a moment later is not, and the answer says so
//! (`"event": "dispatching"`, or a 409 with `Retry-After`) for the caller to
//! ask again.
//!
//! **One run.** Asked with an engine run's id, the door stops that run and
//! leaves the rest of its event as it is: its other runs, and the event
//! itself, which is not withdrawn for one run's sake.

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{bearer, database_error, identify, named, Named};
use crate::event::Trust;
use crate::metrics::ApiOutcome;
use crate::problem::problem;
use crate::server::AppState;

/// What a cell's dagron-api answered to a cancel.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Stopped {
    /// It called off this many of the run's jobs. A worker still holds
    /// `stopping` of them: the run reads `running` until they have stopped.
    Cancelled { jobs: u64, stopping: u64 },
    /// Nothing of it was left to call off: it has ended, or the jobs of an
    /// earlier cancel are stopping still.
    Nothing,
    /// dagron-api has no such run.
    Gone,
    /// The caller's credential was not accepted for it (401 or 403).
    Denied(u16),
}

/// dagron-api's answer to `POST /api/runs/{id}/cancel`.
#[derive(Deserialize)]
struct Answer {
    #[serde(default)]
    cancelled: u64,
    #[serde(default)]
    stopping: u64,
}

/// Ask the dagron-api at `base` to cancel `run_id`, as `credential`: the
/// caller's own, or dagron-ci's for a merge group the queue let go of.
/// `Err` when it could not be asked, or answered something else.
pub(crate) async fn ask(
    http: &reqwest::Client,
    base: &str,
    run_id: &str,
    credential: &str,
) -> Result<Stopped, String> {
    if !crate::rerun::is_run_id(run_id) {
        return Ok(Stopped::Gone);
    }
    let url = format!("{base}/api/runs/{run_id}/cancel");
    let response = http
        .post(&url)
        .bearer_auth(credential)
        .send()
        .await
        .map_err(|e| format!("dagron-api at {url}: {e}"))?;
    match response.status().as_u16() {
        200 => {
            let answer: Answer = response
                .json()
                .await
                .map_err(|e| format!("dagron-api at {url} answered something else: {e}"))?;
            Ok(match answer.cancelled {
                0 => Stopped::Nothing,
                jobs => Stopped::Cancelled {
                    jobs,
                    stopping: answer.stopping,
                },
            })
        }
        404 => Ok(Stopped::Gone),
        code @ (401 | 403) => Ok(Stopped::Denied(code)),
        code => Err(format!("dagron-api at {url}: HTTP {code}")),
    }
}

/// `POST /ci/v1/runs/{id}/cancel`: stop an event's runs, and the event if it
/// is still waiting. 200 with the runs that were cancelled (`cancelled`, each
/// with how many of its jobs were called off and how many of those are still
/// stopping), the ones left as they are, each with why (`left`), and what
/// became of the event (`event`):
/// - `withdrawn`: it was waiting to be dispatched, and now never is;
/// - `dispatching`: a worker is on it now, and a run it submits after this
///   is not cancelled: ask again;
/// - `settled`: it had been dispatched, and its runs are all there are.
///
/// 409 when nothing was there to cancel (each run says why), with
/// `Retry-After` when that is because a worker is dispatching the event now;
/// 503 when dagron-api could not be asked; and the caller's own 401 or 403
/// when dagron-api does not let them.
pub async fn cancel_run(
    State(st): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let refuse = |outcome: ApiOutcome, status: StatusCode, title: &str, detail: String| {
        st.metrics.api_request(outcome);
        problem(status, title, detail)
    };
    let fail = |(outcome, response): (ApiOutcome, Response)| {
        st.metrics.api_request(outcome);
        response
    };
    let Some(credential) = bearer(&headers) else {
        return refuse(
            ApiOutcome::Unauthorized,
            StatusCode::UNAUTHORIZED,
            "not authenticated",
            "send a dagron-api session or API token as Authorization: Bearer".into(),
        );
    };
    let found = match named(&st, &id).await {
        Ok(found) => found,
        Err(e) => return fail(database_error(e)),
    };
    // Nothing of the event is told to a caller who has not been identified.
    let cell = found.as_ref().map(|named| named.event.cell.as_str());
    let caller = match identify(&st, cell, credential).await {
        Ok(caller) => caller,
        Err(failure) => return fail(failure),
    };
    if caller.viewer() {
        return refuse(
            ApiOutcome::Forbidden,
            StatusCode::FORBIDDEN,
            "read-only caller",
            "a viewer may read runs, and stop none".into(),
        );
    }
    if caller.trust() == Trust::Agent && !st.agent_runs {
        return refuse(
            ApiOutcome::Forbidden,
            StatusCode::FORBIDDEN,
            "agent caller",
            "this dagron-ci runs no agent's work (CI_AGENT_RUNS=refuse), and an agent stops \
             nobody's"
                .into(),
        );
    }
    let missing = || {
        refuse(
            ApiOutcome::NotFound,
            StatusCode::NOT_FOUND,
            "no such run",
            format!("no event {id:?}"),
        )
    };
    let Some(Named {
        event: mut found,
        one_run,
    }) = found
    else {
        return missing();
    };
    if !caller.owns(&found) {
        return refuse(
            ApiOutcome::Forbidden,
            StatusCode::FORBIDDEN,
            "not the agent's",
            format!(
                "an agent stops the events it asked for, and event {} is not one",
                found.id
            ),
        );
    }

    // An event no worker holds is withdrawn first, so that nothing more is
    // submitted for it while its runs are stopped. It is read again either
    // way: a worker may have had it, and given it back, since the first look.
    // Not for one run's sake: the rest of its event is not what was asked.
    let mut withdrawn = false;
    if found.state == "new" && !one_run {
        let reason = format!(
            "cancelled by {} while it waited to be dispatched",
            caller.actor()
        );
        withdrawn = match st.db.withdraw_event(&found.id, &reason).await {
            Ok(withdrawn) => withdrawn,
            Err(e) => return fail(database_error(e)),
        };
        found = match st.db.event_view(&found.id).await {
            Ok(Some(found)) => found,
            Ok(None) => return missing(),
            Err(e) => return fail(database_error(e)),
        };
    }
    let event = match found.state.as_str() {
        _ if withdrawn => "withdrawn",
        "new" | "claimed" => "dispatching",
        _ => "settled",
    };

    let mut cancelled = Vec::new();
    let mut left = Vec::new();
    let mut unavailable = None;
    for run in &found.runs {
        let mut leave = |why: String| {
            left.push(json!({ "id": run.id, "pipeline": run.pipeline, "why": why }));
        };
        let Some(base) = st.cells.get(&run.cell) else {
            leave(format!("its cell {:?} is not in CI_CELLS", run.cell));
            continue;
        };
        match ask(st.http.to(base), base, &run.id, credential).await {
            Ok(Stopped::Cancelled { jobs, stopping }) => cancelled.push(json!({
                "id": run.id,
                "cell": run.cell,
                "pipeline": run.pipeline,
                "jobs": jobs,
                "stopping": stopping,
            })),
            Ok(Stopped::Nothing) => leave(match run.status.as_str() {
                "submitted" => {
                    "nothing of it was left to cancel: it has ended, or is stopping already".into()
                }
                over => format!("it is over: {over}"),
            }),
            Ok(Stopped::Gone) => leave("its cell's dagron-api has no such run".into()),
            Ok(Stopped::Denied(code)) => {
                // With something of the event stopped already, what was done
                // is what is said, and this run is left with why.
                if withdrawn || !cancelled.is_empty() {
                    leave("dagron-api did not let this caller cancel it".into());
                    continue;
                }
                let (outcome, status, title) = match code {
                    401 => (
                        ApiOutcome::Unauthorized,
                        StatusCode::UNAUTHORIZED,
                        "not authenticated",
                    ),
                    _ => (ApiOutcome::Forbidden, StatusCode::FORBIDDEN, "not allowed"),
                };
                return refuse(
                    outcome,
                    status,
                    title,
                    format!("dagron-api did not let this caller cancel run {}", run.id),
                );
            }
            Err(why) => {
                leave("dagron-api could not be asked for it".into());
                unavailable = Some(why);
            }
        }
    }

    if withdrawn || !cancelled.is_empty() {
        st.metrics.api_request(ApiOutcome::Accepted);
        tracing::info!(
            event_id = %found.id, actor = %caller.actor(), runs = cancelled.len(), withdrawn,
            "cancelled through dagron-ci"
        );
        crate::audit::record(st.audit.as_ref(), || {
            let mut entry = crate::audit::Entry::new(crate::audit::kind::RUN_CANCEL)
                .actor(Some(&caller.actor()))
                .with("event_id", found.id.as_str())
                .with("event", event)
                .with("cancelled", Value::Array(cancelled.clone()))
                .with("left", Value::Array(left.clone()));
            entry.repo = Some(found.repo.clone());
            entry
        });
        return (
            StatusCode::OK,
            Json(json!({
                "id": found.id,
                "event": event,
                "cancelled": cancelled,
                "left": left,
            })),
        )
            .into_response();
    }
    let why = left
        .iter()
        .map(|run| {
            format!(
                "run {} ({}): {}",
                run["id"].as_str().unwrap_or_default(),
                run["pipeline"].as_str().unwrap_or_default(),
                run["why"].as_str().unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    if let Some(reason) = unavailable {
        tracing::warn!(event_id = %found.id, %reason, "a cancel could not be asked for");
        return refuse(
            ApiOutcome::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "dagron-api could not be asked",
            format!("nothing was cancelled; try again. {why}"),
        );
    }
    // Of one run there is nothing more to wait for: it is there, and is over.
    if event == "dispatching" && !one_run {
        let mut response = refuse(
            ApiOutcome::Conflict,
            StatusCode::CONFLICT,
            "being dispatched",
            format!(
                "event {} is being dispatched now, and nothing of it was there to cancel yet: \
                 ask again{}",
                found.id,
                if why.is_empty() {
                    String::new()
                } else {
                    format!(". {why}")
                }
            ),
        );
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        return response;
    }
    refuse(
        ApiOutcome::Conflict,
        StatusCode::CONFLICT,
        "nothing to cancel",
        if why.is_empty() {
            format!(
                "event {} is {}, and submitted no run",
                found.id, found.state
            )
        } else {
            why
        },
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::{get, post};
    use axum::Router;
    use serde_json::Value;
    use tower::ServiceExt;

    use super::*;
    use crate::config::Config;
    use crate::db::tests::{drop_schema, postgres, sqlite};
    use crate::db::{Claimed, Db, Finish, Repo, ReportWrite, SubmittedRun};
    use crate::dispatch::tests::claimed;
    use crate::event::Kind;
    use crate::server::router;
    use crate::Seams;

    /// A cell's dagron-api, as much of it as a cancel takes.
    #[derive(Default)]
    struct Cell {
        /// Each run it has: how many of its jobs a cancel would call off now,
        /// and how many of those a worker holds.
        runs: BTreeMap<String, (u64, u64)>,
        /// Each cancel asked for: the run, and the credential it came with.
        asked: Vec<(String, String)>,
        /// What every cancel is answered, in place of doing it.
        answers: Option<u16>,
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
        async fn cancel(
            State(cell): State<StubCell>,
            Path(id): Path<String>,
            headers: HeaderMap,
        ) -> Response {
            let mut cell = cell.lock().unwrap();
            cell.asked.push((id.clone(), auth(&headers)));
            if let Some(code) = cell.answers {
                return StatusCode::from_u16(code).unwrap().into_response();
            }
            // A second cancel changes nothing, as dagron-api's does not.
            match cell.runs.insert(id.clone(), (0, 0)) {
                Some((cancelled, stopping)) => {
                    Json(json!({ "cancelled": cancelled, "stopping": stopping })).into_response()
                }
                None => {
                    cell.runs.remove(&id);
                    StatusCode::NOT_FOUND.into_response()
                }
            }
        }
        let cell: StubCell = Arc::default();
        let app = Router::new()
            .route("/api/me", get(me))
            .route("/api/runs/{id}/cancel", post(cancel))
            .with_state(cell.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, cell)
    }

    /// dagron-ci over `db` with one cell and its own token for it, which
    /// no cancel is asked with. And the event every run here is of, which a
    /// worker (`w`) holds.
    struct Rig {
        app: Router,
        cell: StubCell,
        db: Db,
        repo: Repo,
        event: Claimed,
        _dir: tempfile::TempDir,
    }

    async fn rig(db: Db) -> Rig {
        rig_with(db, false).await
    }

    /// The rig, its dagron-ci running an agent's work where `agents` says so
    /// (CI_AGENT_RUNS=untrusted).
    async fn rig_with(db: Db, agents: bool) -> Rig {
        let (base, cell) = stub_cell().await;
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "dgr_dagron_ci\n").unwrap();
        let config = Config::from_lookup(|name| match name {
            "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
            "CI_CELLS" => Some(format!("default={base}")),
            "CI_API_TOKEN_FILE" => Some(token.to_str().unwrap().into()),
            "CI_AGENT_RUNS" => agents.then(|| "untrusted".into()),
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
            _dir: dir,
        }
    }

    impl Rig {
        /// A run of the event that dagron-ci submitted, of which its cell
        /// would call off `jobs` jobs, `stopping` of them held by a worker.
        async fn submitted(&self, run_id: &str, jobs: u64, stopping: u64) {
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
            self.cell
                .lock()
                .unwrap()
                .runs
                .insert(run_id.into(), (jobs, stopping));
        }

        /// The reporter's write that ends `run_id` as `status` on dagron-ci's
        /// record. Nothing else is due: a run here is ended before the next
        /// is made.
        async fn ended(&self, run_id: &str, status: &str) {
            let (due, _) = self
                .db
                .claim_report("reporter", Duration::from_secs(60))
                .await
                .unwrap()
                .expect("the run is due for a look");
            assert_eq!(due, run_id);
            let write = ReportWrite {
                status: Some(status.into()),
                ..ReportWrite::default()
            };
            assert!(self
                .db
                .finish_report(run_id, "reporter", &write)
                .await
                .unwrap());
        }

        /// The worker that holds the event leaves it as `finish` says.
        async fn finish(&self, finish: Finish) {
            assert!(self
                .db
                .finish_event(&self.event.id, "w", &finish)
                .await
                .unwrap());
        }

        /// `POST /ci/v1/runs/{id}/cancel`: the status, the body and what
        /// `Retry-After` said.
        async fn cancel_of(
            &self,
            id: &str,
            bearer: Option<&str>,
        ) -> (StatusCode, Value, Option<String>) {
            let mut req = Request::post(format!("/ci/v1/runs/{id}/cancel"));
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
            let retry = res
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
                .await
                .unwrap();
            (
                status,
                serde_json::from_slice(&bytes).unwrap_or(Value::Null),
                retry,
            )
        }

        async fn cancel(&self, bearer: Option<&str>) -> (StatusCode, Value, Option<String>) {
            self.cancel_of(&self.event.id, bearer).await
        }

        /// Each cancel the cell was asked for, in order of run, and forgotten.
        fn asked(&self) -> Vec<(String, String)> {
            let mut asked = std::mem::take(&mut self.cell.lock().unwrap().asked);
            asked.sort();
            asked
        }

        async fn status(&self, run_id: &str) -> String {
            self.db
                .run_commit(run_id)
                .await
                .unwrap()
                .expect("the run is on record")
                .status
        }
    }

    fn pipeline(run_id: &str) -> String {
        format!(".dagron/ci/{run_id}.yaml")
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

    /// Where this dagron-ci runs an agent's work (CI_AGENT_RUNS=untrusted),
    /// an agent stops an event it asked for, and no other; where it does
    /// not, it stops none, its own included.
    #[tokio::test]
    async fn an_agent_stops_the_events_it_asked_for_and_no_other() {
        let (db, _dir) = sqlite().await;
        let rig = rig_with(db, true).await;
        let agent = Some("dgr_agent");
        // The rig's event is a person's push.
        rig.submitted("run-a", 1, 0).await;
        let (status, body, _) = rig.cancel(agent).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["title"], "not the agent's");
        assert!(rig.asked().is_empty());
        // Its own, still waiting, is withdrawn, in its name.
        let id = agents_dispatch(&rig).await;
        let (status, body, _) = rig.cancel_of(&id, agent).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["event"], "withdrawn");
        let event = rig.db.event_view(&id).await.unwrap().unwrap();
        assert_eq!(
            event.reason.as_deref(),
            Some("cancelled by agent:fixer while it waited to be dispatched")
        );

        let (db, _dir) = sqlite().await;
        let rig = rig_with(db, false).await;
        let id = agents_dispatch(&rig).await;
        let (status, body, _) = rig.cancel_of(&id, agent).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["title"], "agent caller");
    }

    async fn the_cancel_door_contract(db: Db) {
        let rig = rig(db).await;
        let operator = Some("dgr_operator");

        // Nobody is told anything before dagron-api says who they are, and
        // one who may only read, or is an agent, stops nothing.
        assert_eq!(rig.cancel(None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            rig.cancel(Some("dgr_nobody")).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            rig.cancel_of("no-such-event", Some("dgr_nobody")).await.0,
            StatusCode::UNAUTHORIZED,
            "not 404: existence is hidden"
        );
        assert_eq!(
            rig.cancel_of("no-such-event", operator).await.0,
            StatusCode::NOT_FOUND
        );
        rig.submitted("run-over", 1, 0).await;
        for reader in ["dgr_viewer", "dgr_agent"] {
            let (status, body, _) = rig.cancel(Some(reader)).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{reader}: {body}");
        }
        assert!(rig.asked().is_empty(), "and its cell was asked for nothing");

        // One of the event's runs is over on dagron-ci's record and in its
        // cell, and one is running: three jobs, one of them on a worker.
        rig.cell
            .lock()
            .unwrap()
            .runs
            .insert("run-over".into(), (0, 0));
        rig.ended("run-over", "succeeded").await;
        rig.submitted("run-a", 3, 1).await;
        let (status, body, _) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({
                "id": rig.event.id,
                // A worker holds the event: what it submits next is not
                // stopped by this.
                "event": "dispatching",
                "cancelled": [{
                    "id": "run-a",
                    "cell": "default",
                    "pipeline": pipeline("run-a"),
                    "jobs": 3,
                    "stopping": 1,
                }],
                "left": [{
                    "id": "run-over",
                    "pipeline": pipeline("run-over"),
                    "why": "it is over: succeeded",
                }],
            })
        );
        // Each run was asked about, the one on record as over too, and with
        // the caller's own credential, never dagron-ci's.
        assert_eq!(
            rig.asked(),
            [
                ("run-a".to_string(), "Bearer dgr_operator".to_string()),
                ("run-over".to_string(), "Bearer dgr_operator".to_string()),
            ]
        );
        // dagron-ci wrote nothing of the run: its end is the reporter's to see.
        assert_eq!(rig.status("run-a").await, "submitted");

        // Again, while the worker still holds the event: nothing was left to
        // stop, and the caller is told to ask again.
        let (status, body, retry) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["title"], "being dispatched");
        assert_eq!(retry.as_deref(), Some("1"));
        let detail = body["detail"].as_str().unwrap();
        assert!(
            detail.contains("run run-a (.dagron/ci/run-a.yaml): nothing of it was left to cancel"),
            "{detail}"
        );

        // With the event settled, that is a plain refusal, and says why of
        // each run.
        rig.finish(Finish::Done("submitted two runs".into())).await;
        let (status, body, retry) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["title"], "nothing to cancel");
        assert_eq!(retry, None);
        let detail = body["detail"].as_str().unwrap();
        assert!(
            detail.contains("nothing of it was left to cancel")
                && detail
                    .contains("run run-over (.dagron/ci/run-over.yaml): it is over: succeeded"),
            "{detail}"
        );

        // A run on record as over that is running again, moved through
        // dagron-api and not seen yet, is stopped all the same.
        rig.cell
            .lock()
            .unwrap()
            .runs
            .insert("run-over".into(), (2, 2));
        let (status, body, _) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["event"], "settled");
        assert_eq!(body["cancelled"][0]["id"], "run-over");
        assert_eq!(body["cancelled"][0]["stopping"], 2);
        assert_eq!(body["left"][0]["id"], "run-a");

        // The cell's answer is the caller's: its refusal, and its absence.
        rig.cell.lock().unwrap().answers = Some(403);
        let (status, body, _) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        rig.cell.lock().unwrap().answers = Some(401);
        assert_eq!(rig.cancel(operator).await.0, StatusCode::UNAUTHORIZED);
        rig.cell.lock().unwrap().answers = Some(500);
        let (status, body, _) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert!(
            body["detail"]
                .as_str()
                .unwrap()
                .starts_with("nothing was cancelled; try again."),
            "{body}"
        );
        // A run its cell no longer has is left, and says so.
        *rig.cell.lock().unwrap() = Cell::default();
        let (status, body, _) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert!(
            body["detail"].as_str().unwrap().contains(
                "run run-a (.dagron/ci/run-a.yaml): its cell's dagron-api has no such run"
            ),
            "{body}"
        );
    }

    #[tokio::test]
    async fn the_cancel_door_stops_what_is_running_and_says_what_it_left() {
        let (db, _dir) = sqlite().await;
        the_cancel_door_contract(db).await;
    }

    #[tokio::test]
    async fn the_cancel_door_stops_what_is_running_on_postgres() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_cancel_door_contract(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// An event no worker holds is withdrawn: it ends `skipped`, with who
    /// cancelled it, no worker takes it after, and what it had submitted on
    /// an earlier turn is stopped.
    async fn the_withdrawal_contract(db: Db) {
        let rig = rig(db).await;
        let operator = Some("dgr_operator");

        // Two of its pipelines' runs are submitted; its third's was not, and
        // the worker gave the event back for another try.
        rig.submitted("run-a", 2, 0).await;
        rig.submitted("run-b", 1, 1).await;
        rig.finish(Finish::Retry {
            reason: "docs.yaml: dagron-api answered 503".into(),
            after: Duration::ZERO,
        })
        .await;

        // One run, by its own id, the one a commit status links to: that run
        // is stopped, and its event is still to be dispatched.
        let (status, body, _) = rig.cancel_of("run-b", operator).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({
                "id": rig.event.id,
                "event": "dispatching",
                "cancelled": [{
                    "id": "run-b",
                    "cell": "default",
                    "pipeline": pipeline("run-b"),
                    "jobs": 1,
                    "stopping": 1,
                }],
                "left": [],
            })
        );
        assert_eq!(
            rig.asked(),
            [("run-b".to_string(), "Bearer dgr_operator".to_string())]
        );
        let event = rig.db.event_view(&rig.event.id).await.unwrap().unwrap();
        assert_eq!(event.state, "new", "not withdrawn for one run's sake");
        // Again, of that run: nothing is left of it, and there is nothing
        // to wait for.
        let (status, body, retry) = rig.cancel_of("run-b", operator).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(
            (body["title"].as_str(), retry),
            (Some("nothing to cancel"), None)
        );

        let (status, body, _) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({
                "id": rig.event.id,
                "event": "withdrawn",
                "cancelled": [{
                    "id": "run-a",
                    "cell": "default",
                    "pipeline": pipeline("run-a"),
                    "jobs": 2,
                    "stopping": 0,
                }],
                "left": [{
                    "id": "run-b",
                    "pipeline": pipeline("run-b"),
                    "why": "nothing of it was left to cancel: it has ended, or is stopping already",
                }],
            })
        );
        let event = rig.db.event_view(&rig.event.id).await.unwrap().unwrap();
        assert_eq!(
            (event.state.as_str(), event.reason.as_deref()),
            (
                "skipped",
                Some("cancelled by u-1 while it waited to be dispatched")
            )
        );
        // It is nobody's to dispatch now, however long they wait.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(rig
            .db
            .claim_event("w-2", Duration::from_secs(60))
            .await
            .unwrap()
            .is_none());

        // Once: a second cancel finds a settled event, and nothing running.
        let (status, body, retry) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["title"], "nothing to cancel");
        assert_eq!(retry, None);
        let event = rig.db.event_view(&rig.event.id).await.unwrap().unwrap();
        assert_eq!(
            event.reason.as_deref(),
            Some("cancelled by u-1 while it waited to be dispatched"),
            "and its reason is the first cancel's"
        );
    }

    #[tokio::test]
    async fn an_event_that_waits_to_be_dispatched_is_withdrawn() {
        let (db, _dir) = sqlite().await;
        the_withdrawal_contract(db).await;
    }

    #[tokio::test]
    async fn an_event_that_waits_to_be_dispatched_is_withdrawn_on_postgres() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_withdrawal_contract(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// One with nothing submitted is withdrawn whole: there is nothing of it
    /// in any cell, and that is a cancel, not a refusal. One a worker holds
    /// with nothing submitted yet is not this door's to stop.
    #[tokio::test]
    async fn an_event_with_no_run_yet_is_withdrawn_or_asked_about_again() {
        let (db, _dir) = sqlite().await;
        let rig = rig(db).await;
        let operator = Some("dgr_operator");

        let (status, body, retry) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["title"], "being dispatched");
        assert_eq!(retry.as_deref(), Some("1"));
        assert!(!body["detail"].as_str().unwrap().ends_with(". "), "{body}");

        rig.finish(Finish::Wait {
            reason: "the build's limit on runs in flight".into(),
            after: Duration::from_secs(30),
        })
        .await;
        let (status, body, _) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({ "id": rig.event.id, "event": "withdrawn", "cancelled": [], "left": [] })
        );
        assert!(rig.asked().is_empty());

        // An event that ended with no run says so.
        let (status, body, _) = rig.cancel(operator).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(
            body["detail"].as_str().unwrap(),
            format!("event {} is skipped, and submitted no run", rig.event.id)
        );
    }
}
