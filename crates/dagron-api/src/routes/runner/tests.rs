//! The runner protocol against a real database (`TEST_DATABASE_URL`; skipped
//! without one). Each test serves a runner class of its own, so that its
//! claims take its own rows of the shared database and no other test's.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use axum::Json;
use serde_json::Value;

use dagron_core::db;
use dagron_core::runner::{Heartbeat, Held, LogSegment, OpenSession, Outcome, Report, Secrets};

use super::*;
use crate::auth::{AuthUser, SessionAuth, SessionClaims};
use crate::routes::control;
use crate::routes::ops::redrive_tests::test_state;

fn admin() -> SessionClaims {
    SessionClaims {
        sub: "admin-1".into(),
        email: "admin@example.com".into(),
        name: "Admin".into(),
        groups: vec!["admin".into()],
        exp: 0,
    }
}

async fn body(resp: Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

fn bearer(token: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    h
}

fn class() -> String {
    format!("rr-{}", uuid::Uuid::new_v4().simple())
}

/// A join token for `labels`, as an admin mints it.
async fn join_token(state: &AppState, labels: &[&str], max_uses: i32) -> String {
    let (status, Json(view)) = admin::create_join_token(
        SessionAuth(admin()),
        State(state.clone()),
        Json(admin::NewJoinToken {
            labels: labels.iter().map(|l| l.to_string()).collect(),
            max_uses: Some(max_uses),
            ttl_secs: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
    view.token.unwrap()
}

/// A runner registered for `label`: its id and credential.
async fn runner(state: &AppState, label: &str) -> (String, String) {
    let token = join_token(state, &[label], 1).await;
    let (status, reg) = body(
        register(
            State(state.clone()),
            Json(dagron_core::runner::RegisterRequest {
                join_token: token,
                name: Some("test-runner".into()),
                labels: vec![],
                os: Some("linux".into()),
                arch: Some("x86_64".into()),
                capacity: 4,
                version: Some("0.1.0".into()),
            }),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{reg}");
    (
        reg["runner_id"].as_str().unwrap().into(),
        reg["token"].as_str().unwrap().into(),
    )
}

async fn session(state: &AppState, token: &str, capacity: u32, secrets: Secrets) -> String {
    let (status, s) = body(
        open_session(
            State(state.clone()),
            bearer(token),
            Json(OpenSession {
                labels: vec![],
                capacity,
                version: "0.1.0".into(),
                protocols: vec![0],
                secrets,
                os: None,
                arch: None,
            }),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{s}");
    assert_eq!(s["protocol"], 0);
    s["session_id"].as_str().unwrap().into()
}

/// A run of `tasks` (YAML list items) in runner class `class`, its tasks
/// made ready as an engine's tick would.
async fn run_of(state: &AppState, class: &str, tasks: &str) -> String {
    let yaml = format!("name: {class}\nrunner_class: {class}\ntasks:\n{tasks}");
    let run = control::submit_yaml_as(state, &yaml, &yaml, &BTreeMap::new(), None)
        .await
        .unwrap();
    db::advance_ready_tasks(&state.write_pool).await.unwrap();
    run
}

async fn claim_now(state: &AppState, token: &str, session: &str) -> Value {
    let (status, claimed) = body(
        claim(
            State(state.clone()),
            Path(session.to_string()),
            Query(ClaimParams { wait: Some(0) }),
            bearer(token),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{claimed}");
    claimed
}

async fn heartbeat_of(
    state: &AppState,
    token: &str,
    session: &str,
    running: Vec<Held>,
) -> (StatusCode, Value) {
    body(
        heartbeat(
            State(state.clone()),
            Path(session.to_string()),
            bearer(token),
            Json(Heartbeat { running }),
        )
        .await,
    )
    .await
}

async fn row(state: &AppState, task: &str) -> (String, Option<String>, Option<String>, i64, i64) {
    sqlx::query_as(
        "SELECT status, claimed_by, session_id, version, attempt FROM task_runs WHERE id = $1",
    )
    .bind(task)
    .fetch_one(&state.write_pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn a_join_token_enrols_once_and_a_session_speaks_a_protocol_both_ends_do() {
    let Some(state) = test_state("runner-register").await else {
        return;
    };
    let label = class();
    let token = join_token(&state, &[&label], 1).await;
    let request = |token: String, labels: Vec<String>| dagron_core::runner::RegisterRequest {
        join_token: token,
        name: None,
        labels,
        os: None,
        arch: None,
        capacity: 2,
        version: None,
    };
    // A label the token does not allow is refused, and spends nothing.
    let (status, _) = body(
        register(
            State(state.clone()),
            Json(request(token.clone(), vec!["other".into()])),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, reg) =
        body(register(State(state.clone()), Json(request(token.clone(), vec![]))).await).await;
    assert_eq!(status, StatusCode::CREATED, "{reg}");
    assert_eq!(reg["labels"], serde_json::json!([label]));
    assert!(reg["token"].as_str().unwrap().starts_with("dgr_"));
    // Used up.
    let (status, again) =
        body(register(State(state.clone()), Json(request(token, vec![]))).await).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{again}");

    let credential = reg["token"].as_str().unwrap().to_string();
    let open = |protocols: Vec<u32>, labels: Vec<String>| OpenSession {
        labels,
        capacity: 1,
        version: "0.1.0".into(),
        protocols,
        secrets: Secrets::Server,
        os: None,
        arch: None,
    };
    let (status, p) = body(
        open_session(
            State(state.clone()),
            bearer(&credential),
            Json(open(vec![7, 8], vec![])),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::UPGRADE_REQUIRED, "{p}");
    assert_eq!(p["title"], "No common protocol");
    let (status, _) = body(
        open_session(
            State(state.clone()),
            bearer(&credential),
            Json(open(vec![0], vec!["other".into()])),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, s) = body(
        open_session(
            State(state.clone()),
            bearer(&credential),
            Json(open(vec![0, 1], vec![])),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{s}");
    assert_eq!(
        (s["protocol"].as_u64(), s["heartbeat_secs"].as_i64()),
        (Some(0), Some(30))
    );
    // Not a credential.
    let (status, _) = body(
        open_session(
            State(state.clone()),
            bearer("dgr_nope_nope_nope"),
            Json(open(vec![0], vec![])),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_claim_takes_its_labels_ready_tasks_and_a_heartbeat_says_which_to_stop() {
    let Some(state) = test_state("runner-claim").await else {
        return;
    };
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 2, Secrets::Server).await;
    let run = run_of(
        &state,
        &label,
        "  - name: a\n    command: [\"echo\", \"a\"]\n  - name: b\n    command: [\"echo\", \"b\"]\n  - name: c\n    command: [\"echo\", \"c\"]\n",
    )
    .await;

    // Two, as the session runs two at once.
    let claimed = claim_now(&state, &token, &session_id).await;
    let tasks = claimed["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 2, "{claimed}");
    // Full: nothing more until one is over.
    assert!(claim_now(&state, &token, &session_id).await["tasks"]
        .as_array()
        .unwrap()
        .is_empty());

    let mut held = Vec::new();
    for job in tasks {
        let task = job["task_id"].as_str().unwrap();
        let (status, by, held_by, version, attempt) = row(&state, task).await;
        assert_eq!(status, "running");
        assert_eq!(held_by.as_deref(), Some(session_id.as_str()));
        assert!(by.unwrap().ends_with(&format!("/{session_id}")));
        assert_eq!(
            (job["fence"].as_i64(), attempt, job["attempt"].as_i64()),
            (Some(version), 1, Some(0))
        );
        let env = job["env"].as_array().unwrap();
        assert!(env
            .iter()
            .any(|e| e["name"] == "DAGRON_TASK_ID" && e["value"] == task));
        assert!(env
            .iter()
            .any(|e| e["name"] == "DAGRON_RUN_ID" && e["value"] == run.as_str()));
        held.push(Held {
            task_id: task.to_string(),
            fence: version,
        });
    }

    let (status, ack) = heartbeat_of(&state, &token, &session_id, held.clone()).await;
    assert_eq!(status, StatusCode::OK, "{ack}");
    assert!(ack["cancel"].as_array().unwrap().is_empty());
    assert_eq!(ack["drain"], false);

    // A cancel reaches the work at the next heartbeat, and so does a fence
    // that is not the task's.
    db::cancel_run(&state.write_pool, &run).await.unwrap();
    let mut stale = held.clone();
    stale[1].fence += 1;
    let (_, ack) = heartbeat_of(&state, &token, &session_id, stale).await;
    let mut cancel: Vec<String> = ack["cancel"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    cancel.sort();
    let mut want: Vec<String> = held.iter().map(|h| h.task_id.clone()).collect();
    want.sort();
    assert_eq!(cancel, want);
}

#[tokio::test]
async fn a_result_is_kept_once_under_its_fence_for_an_engine_to_apply() {
    let Some(state) = test_state("runner-result").await else {
        return;
    };
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 1, Secrets::Server).await;
    run_of(
        &state,
        &label,
        "  - name: t\n    command: [\"false\"]\n    max_attempts: 2\n",
    )
    .await;
    let claimed = claim_now(&state, &token, &session_id).await;
    let job = &claimed["tasks"][0];
    let task = job["task_id"].as_str().unwrap().to_string();
    let fence = job["fence"].as_i64().unwrap();
    let report = |fence: i64| Report {
        session_id: session_id.clone(),
        fence,
        outcome: Outcome::Failed,
        exit_code: Some(1),
        output: "it failed".into(),
    };

    let (status, _) = body(
        result(
            State(state.clone()),
            Path(task.clone()),
            bearer(&token),
            Json(report(fence + 1)),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, kept) = body(
        result(
            State(state.clone()),
            Path(task.clone()),
            bearer(&token),
            Json(report(fence)),
        )
        .await,
    )
    .await;
    assert_eq!(
        (status, kept["recorded"].as_bool()),
        (StatusCode::OK, Some(true))
    );
    let (status, again) = body(
        result(
            State(state.clone()),
            Path(task.clone()),
            bearer(&token),
            Json(report(fence)),
        )
        .await,
    )
    .await;
    assert_eq!(
        (status, again["duplicate"].as_bool()),
        (StatusCode::OK, Some(true))
    );

    // A session holding a task whose result waits holds it still: no new
    // claim, and the session sweep leaves it for its result.
    let taken = db::take_runner_results(&state.write_pool, "engine-test", 1000)
        .await
        .unwrap();
    let word = taken
        .into_iter()
        .find(|w| w.task_id == task)
        .expect("the result is taken");
    assert_eq!(
        (word.fence, word.attempt, word.outcome.as_str()),
        (fence, 0, "failed")
    );
    assert_eq!(word.output.as_deref(), Some("it failed"));
    assert!(word.claimed_by.ends_with(&session_id));
    // Taken: not taken again until a minute has gone by.
    let again = db::take_runner_results(&state.write_pool, "engine-other", 1000)
        .await
        .unwrap();
    assert!(again.iter().all(|w| w.task_id != task));
    db::drop_runner_results(&state.write_pool, &[(task.clone(), fence)])
        .await
        .unwrap();
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runner_results WHERE task_id = $1")
        .bind(&task)
        .fetch_one(&state.write_pool)
        .await
        .unwrap();
    assert_eq!(left, 0);
}

#[tokio::test]
async fn a_session_whose_lease_lapses_has_its_tasks_taken_back() {
    let Some(state) = test_state("runner-expiry").await else {
        return;
    };
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 1, Secrets::Server).await;
    run_of(
        &state,
        &label,
        "  - name: t\n    command: [\"sleep\", \"600\"]\n",
    )
    .await;
    let claimed = claim_now(&state, &token, &session_id).await;
    let task = claimed["tasks"][0]["task_id"].as_str().unwrap().to_string();

    // An engine's own expired-lease sweep passes a session's task by, however
    // old its lease: the session's lease is what holds it.
    sqlx::query(
        "UPDATE task_runs SET lease_expires_at = '2000-01-01T00:00:00+00:00' WHERE id = $1",
    )
    .bind(&task)
    .execute(&state.write_pool)
    .await
    .unwrap();
    db::recover_expired_leases(&state.write_pool).await.unwrap();
    assert_eq!(row(&state, &task).await.0, "running");

    // The runner is killed: its heartbeats stop, and its lease lapses.
    sqlx::query(
        "UPDATE runner_sessions SET lease_expires_at = '2000-01-01T00:00:00+00:00' WHERE id = $1",
    )
    .bind(&session_id)
    .execute(&state.write_pool)
    .await
    .unwrap();
    // Any engine's sweep takes it: another test's may have, already.
    db::recover_expired_sessions(&state.write_pool)
        .await
        .unwrap();
    let (status, by, held_by, _, _) = row(&state, &task).await;
    assert_eq!((status.as_str(), by, held_by), ("ready", None, None));
    let (status, _) = heartbeat_of(&state, &token, &session_id, vec![]).await;
    assert_eq!(status, StatusCode::GONE);

    // Another session of the runner completes it.
    let next = session(&state, &token, 1, Secrets::Server).await;
    let claimed = claim_now(&state, &token, &next).await;
    assert_eq!(claimed["tasks"][0]["task_id"].as_str(), Some(task.as_str()));
    assert_eq!(
        row(&state, &task).await.4,
        2,
        "the lapsed claim spent its attempt, as a lapsed lease does"
    );
}

#[tokio::test]
async fn a_claim_the_runner_never_heard_of_is_given_back_with_its_attempt() {
    let Some(state) = test_state("runner-orphan").await else {
        return;
    };
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 1, Secrets::Server).await;
    run_of(&state, &label, "  - name: t\n    command: [\"true\"]\n").await;
    let claimed = claim_now(&state, &token, &session_id).await;
    let task = claimed["tasks"][0]["task_id"].as_str().unwrap().to_string();

    // Not named, but claimed just now: a heartbeat on its way as the claim
    // was answered. Left alone.
    heartbeat_of(&state, &token, &session_id, vec![]).await;
    assert_eq!(row(&state, &task).await.0, "running");
    // Claimed long ago and still never named: the answer was lost.
    sqlx::query(
        "UPDATE task_runs SET lease_expires_at = '2000-01-01T00:00:00+00:00' WHERE id = $1",
    )
    .bind(&task)
    .execute(&state.write_pool)
    .await
    .unwrap();
    let (status, _) = heartbeat_of(&state, &token, &session_id, vec![]).await;
    assert_eq!(status, StatusCode::OK);
    let (status, by, held_by, _, attempt) = row(&state, &task).await;
    assert_eq!(
        (status.as_str(), by, held_by, attempt),
        ("ready", None, None, 0)
    );
}

#[tokio::test]
async fn a_draining_session_takes_nothing_and_ends_once_it_holds_nothing() {
    let Some(state) = test_state("runner-drain").await else {
        return;
    };
    let label = class();
    let (runner_id, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 1, Secrets::Server).await;
    run_of(
        &state,
        &label,
        "  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n",
    )
    .await;
    let claimed = claim_now(&state, &token, &session_id).await;
    let job = &claimed["tasks"][0];
    let task = job["task_id"].as_str().unwrap().to_string();
    let fence = job["fence"].as_i64().unwrap();

    // An admin drains the runner: its session says so, and takes nothing.
    let Json(drained) = admin::drain_runner(
        AuthUser(admin()),
        State(state.clone()),
        Path(runner_id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(drained["draining"], 1);
    let (_, ack) = heartbeat_of(
        &state,
        &token,
        &session_id,
        vec![Held {
            task_id: task.clone(),
            fence,
        }],
    )
    .await;
    assert_eq!(ack["drain"], true);
    let claimed = claim_now(&state, &token, &session_id).await;
    assert_eq!(
        (
            claimed["drain"].as_bool(),
            claimed["tasks"].as_array().unwrap().len()
        ),
        (Some(true), 0)
    );

    // It still runs one: the session drains.
    let (status, closing) = body(
        close_session(
            State(state.clone()),
            Path(session_id.clone()),
            bearer(&token),
        )
        .await,
    )
    .await;
    assert_eq!(
        (status, closing["running"].as_i64()),
        (StatusCode::ACCEPTED, Some(1))
    );
    // Its result applied, as an engine applies it: the session ends.
    let holder: String = sqlx::query_scalar("SELECT holder FROM runner_sessions WHERE id = $1")
        .bind(&session_id)
        .fetch_one(&state.write_pool)
        .await
        .unwrap();
    assert!(
        db::mark_task_succeeded(&state.write_pool, &task, &holder, fence, Some("ok".into()))
            .await
            .unwrap()
    );
    let (status, closed) = body(
        close_session(
            State(state.clone()),
            Path(session_id.clone()),
            bearer(&token),
        )
        .await,
    )
    .await;
    assert_eq!(
        (status, closed["ended"].as_bool()),
        (StatusCode::OK, Some(true))
    );
    let (status, _) = heartbeat_of(&state, &token, &session_id, vec![]).await;
    assert_eq!(status, StatusCode::GONE);
    // A new session of the runner is not drained.
    let next = session(&state, &token, 1, Secrets::Server).await;
    assert_eq!(
        claim_now(&state, &token, &next).await["tasks"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn a_task_s_log_is_kept_while_its_claim_holds() {
    let Some(state) = test_state("runner-logs").await else {
        return;
    };
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 1, Secrets::Server).await;
    run_of(&state, &label, "  - name: t\n    command: [\"true\"]\n").await;
    let claimed = claim_now(&state, &token, &session_id).await;
    let task = claimed["tasks"][0]["task_id"].as_str().unwrap().to_string();
    let fence = claimed["tasks"][0]["fence"].as_i64().unwrap();
    let segment = |text: &str, fence: i64| LogSegment {
        session_id: session_id.clone(),
        fence,
        text: text.into(),
        reset: false,
    };
    for (seq, text) in [(0u64, "one\n"), (1, "two\n")] {
        let resp = logs(
            State(state.clone()),
            Path((task.clone(), seq)),
            bearer(&token),
            Json(segment(text, fence)),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }
    let log: Option<String> = sqlx::query_scalar("SELECT log FROM task_runs WHERE id = $1")
        .bind(&task)
        .fetch_one(&state.write_pool)
        .await
        .unwrap();
    assert_eq!(log.as_deref(), Some("one\ntwo\n"));
    let resp = logs(
        State(state.clone()),
        Path((task.clone(), 2)),
        bearer(&token),
        Json(segment("late\n", fence + 1)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

/// A runner that did not hear the answer to a segment sends it again, and a
/// segment can be held up on its way past a later one: either is answered as
/// the first was, and written once.
#[tokio::test]
async fn a_log_segment_sent_again_is_written_once() {
    let Some(state) = test_state("runner-logs-again").await else {
        return;
    };
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 1, Secrets::Server).await;
    run_of(&state, &label, "  - name: t\n    command: [\"true\"]\n").await;
    let claimed = claim_now(&state, &token, &session_id).await;
    let task = claimed["tasks"][0]["task_id"].as_str().unwrap().to_string();
    let fence = claimed["tasks"][0]["fence"].as_i64().unwrap();
    let send = |seq: u64, reset: bool, text: &'static str| {
        let (state, task, token, session_id) = (
            state.clone(),
            task.clone(),
            token.clone(),
            session_id.clone(),
        );
        async move {
            logs(
                State(state),
                Path((task, seq)),
                bearer(&token),
                Json(LogSegment {
                    session_id,
                    fence,
                    text: text.into(),
                    reset,
                }),
            )
            .await
            .status()
        }
    };
    let log = || async {
        sqlx::query_scalar::<_, Option<String>>("SELECT log FROM task_runs WHERE id = $1")
            .bind(&task)
            .fetch_one(&state.write_pool)
            .await
            .unwrap()
            .unwrap_or_default()
    };
    for (seq, reset, text) in [
        (0, true, "one\n"),
        (0, true, "one\n"),
        (1, false, "two\n"),
        (1, false, "two\n"),
        (2, false, "three\n"),
    ] {
        assert_eq!(
            send(seq, reset, text).await,
            StatusCode::NO_CONTENT,
            "segment {seq}"
        );
    }
    assert_eq!(log().await, "one\ntwo\nthree\n");
    // Held up past later ones: the first, or one before the last.
    assert_eq!(send(0, true, "one\n").await, StatusCode::NO_CONTENT);
    assert_eq!(send(1, false, "two\n").await, StatusCode::NO_CONTENT);
    assert_eq!(log().await, "one\ntwo\nthree\n");

    // A seal replaces the log, numbered on from the last, and one sent before
    // it that comes after it is not added to what it sealed.
    assert_eq!(
        send(4, true, "sealed\n").await,
        StatusCode::NO_CONTENT,
        "the seal, past a segment 3 that was given up"
    );
    assert_eq!(
        send(3, false, "three and a half\n").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(log().await, "sealed\n");
    for (seq, text) in [(5, "on\n"), (5, "on\n")] {
        assert_eq!(
            send(seq, false, text).await,
            StatusCode::NO_CONTENT,
            "segment {seq}"
        );
    }
    assert_eq!(log().await, "sealed\non\n");

    // One past the next is new text, and is written.
    assert_eq!(send(9, false, "nine\n").await, StatusCode::NO_CONTENT);
    assert_eq!(log().await, "sealed\non\nnine\n");
}

#[tokio::test]
async fn secrets_stay_home_or_come_from_the_run_s_environment_alone() {
    let Some(state) = test_state("runner-secrets").await else {
        return;
    };
    let tasks = "  - name: t\n    command: [\"true\"]\n    env:\n      - name: DEPLOY_KEY\n        value_from: { secret: deploy_key }\n";

    // Home: the name alone is sent, for the runner's own store.
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let home = session(&state, &token, 1, Secrets::Home).await;
    run_of(&state, &label, tasks).await;
    let claimed = claim_now(&state, &token, &home).await;
    let env = claimed["tasks"][0]["env"].as_array().unwrap().clone();
    let key = env.iter().find(|e| e["name"] == "DEPLOY_KEY").unwrap();
    assert_eq!(key["value"], "");
    assert_eq!(key["value_from"]["secret"], "deploy_key");

    // Server: a run with no environment has nothing dagron-api may read for
    // it, and the task fails saying where the secret can be put.
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let server = session(&state, &token, 1, Secrets::Server).await;
    let run = run_of(&state, &label, tasks).await;
    let claimed = claim_now(&state, &token, &server).await;
    assert!(claimed["tasks"].as_array().unwrap().is_empty(), "{claimed}");
    let (status, output): (String, Option<String>) =
        sqlx::query_as("SELECT status, output FROM task_runs WHERE run_id = $1")
            .bind(&run)
            .fetch_one(&state.write_pool)
            .await
            .unwrap();
    assert_eq!(status, "failed");
    let output = output.unwrap();
    assert!(output.contains("DAGRON_RUNNER_SECRETS=home"), "{output}");
}

#[tokio::test]
async fn a_task_an_engine_alone_can_run_is_failed_and_not_given_out() {
    let Some(state) = test_state("runner-refused").await else {
        return;
    };
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 1, Secrets::Server).await;
    let run = run_of(
        &state,
        &label,
        "  - name: w\n    type: wait\n    wait: { for: 1h }\n",
    )
    .await;
    let claimed = claim_now(&state, &token, &session_id).await;
    assert!(claimed["tasks"].as_array().unwrap().is_empty());
    let (status, output): (String, Option<String>) =
        sqlx::query_as("SELECT status, output FROM task_runs WHERE run_id = $1")
            .bind(&run)
            .fetch_one(&state.write_pool)
            .await
            .unwrap();
    assert_eq!(status, "failed");
    assert!(output.unwrap().contains("remote runner"));
}

/// A task that reserves of a token budget is given out while the budget has
/// room for it, handed back to wait while a reservation is out, and failed,
/// saying why, once what was spent leaves it none.
#[cfg(feature = "enterprise")]
#[tokio::test]
async fn a_task_s_token_reservation_is_made_before_it_is_given_out() {
    let Some(state) = test_state("runner-tokens").await else {
        return;
    };
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 2, Secrets::Server).await;
    let yaml = format!(
        "name: {label}\nrunner_class: {label}\nbudget: {{ tokens: 300 }}\ntasks:\n  \
         - {{ name: a, command: [\"true\"], tokens: 200 }}\n  \
         - {{ name: b, command: [\"true\"], tokens: 200 }}\n"
    );
    let run = control::submit_yaml_as(&state, &yaml, &yaml, &BTreeMap::new(), None)
        .await
        .unwrap();
    db::advance_ready_tasks(&state.write_pool).await.unwrap();

    let claimed = claim_now(&state, &token, &session_id).await;
    let jobs = claimed["tasks"].as_array().unwrap();
    assert_eq!(jobs.len(), 1, "one fits the budget: {claimed}");
    let given = jobs[0]["task_id"].as_str().unwrap().to_string();
    let fence = jobs[0]["fence"].as_i64().unwrap();
    let other: String =
        sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = $1 AND id <> $2")
            .bind(&run)
            .bind(&given)
            .fetch_one(&state.write_pool)
            .await
            .unwrap();
    let (status, claimed_by, _, _, attempt) = row(&state, &other).await;
    assert_eq!((status.as_str(), claimed_by, attempt), ("ready", None, 0));
    let tokens = dagron_core::tokens::of_run(&state.read_pool, &run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((tokens.spent, tokens.reserved), (0, 200));

    // The one given out spent 150, as an engine applies its result: 150 and
    // 200 more pass 300, and nothing out can give room back.
    dagron_core::tokens::settle(&state.write_pool, &given, fence, true, Some(150))
        .await
        .unwrap();
    sqlx::query("UPDATE task_runs SET scheduled_at = $1 WHERE id = $2")
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(&other)
        .execute(&state.write_pool)
        .await
        .unwrap();
    let claimed = claim_now(&state, &token, &session_id).await;
    assert!(claimed["tasks"].as_array().unwrap().is_empty(), "{claimed}");
    let (status, output): (String, Option<String>) =
        sqlx::query_as("SELECT status, output FROM task_runs WHERE id = $1")
            .bind(&other)
            .fetch_one(&state.write_pool)
            .await
            .unwrap();
    assert_eq!(status, "failed");
    assert!(output.unwrap().contains("token budget exhausted"));
}

#[tokio::test]
async fn a_revoked_runner_is_refused_and_its_sessions_end() {
    let Some(state) = test_state("runner-revoke").await else {
        return;
    };
    let label = class();
    let (runner_id, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 1, Secrets::Server).await;
    run_of(&state, &label, "  - name: t\n    command: [\"true\"]\n").await;
    let claimed = claim_now(&state, &token, &session_id).await;
    let task = claimed["tasks"][0]["task_id"].as_str().unwrap().to_string();

    let Json(revoked) =
        admin::revoke_runner(AuthUser(admin()), State(state.clone()), Path(runner_id))
            .await
            .unwrap();
    assert_eq!(revoked["sessions_ended"], 1);
    let (status, _) = heartbeat_of(&state, &token, &session_id, vec![]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    db::recover_expired_sessions(&state.write_pool)
        .await
        .unwrap();
    assert_eq!(row(&state, &task).await.0, "ready");
}

#[tokio::test]
async fn the_queue_says_the_work_there_is_for_each_label() {
    let Some(state) = test_state("runner-queue").await else {
        return;
    };
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 2, Secrets::Server).await;
    run_of(
        &state,
        &label,
        "  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n  - name: c\n    command: [\"true\"]\n",
    )
    .await;
    let (status, q) = body(
        queue(
            State(state.clone()),
            Query(QueueParams { labels: None }),
            bearer(&token),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{q}");
    assert_eq!(
        q["labels"][0],
        serde_json::json!({"label": label, "ready": 3, "running": 0, "sessions": 1, "capacity": 2})
    );
    claim_now(&state, &token, &session_id).await;
    let (_, q) = body(
        queue(
            State(state.clone()),
            Query(QueueParams {
                labels: Some(label.clone()),
            }),
            bearer(&token),
        )
        .await,
    )
    .await;
    assert_eq!(
        (
            q["labels"][0]["ready"].as_i64(),
            q["labels"][0]["running"].as_i64()
        ),
        (Some(1), Some(2))
    );
    // Only what the runner serves.
    let (status, _) = body(
        queue(
            State(state.clone()),
            Query(QueueParams {
                labels: Some("other".into()),
            }),
            bearer(&token),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_heartbeat_renews_the_session_and_writes_no_task_row() {
    let Some(state) = test_state("runner-sc4").await else {
        return;
    };
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 4, Secrets::Server).await;
    run_of(
        &state,
        &label,
        "  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n  - name: c\n    command: [\"true\"]\n  - name: d\n    command: [\"true\"]\n",
    )
    .await;
    let claimed = claim_now(&state, &token, &session_id).await;
    let held: Vec<Held> = claimed["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|job| Held {
            task_id: job["task_id"].as_str().unwrap().to_string(),
            fence: job["fence"].as_i64().unwrap(),
        })
        .collect();
    assert_eq!(held.len(), 4);
    let ids: Vec<String> = held.iter().map(|h| h.task_id.clone()).collect();
    // A row's xmin is the transaction that last wrote it.
    let written = |ids: Vec<String>| {
        let pool = state.write_pool.clone();
        async move {
            let rows: Vec<String> = sqlx::query_scalar(
                "SELECT xmin::text FROM task_runs WHERE id = ANY($1) ORDER BY id",
            )
            .bind(&ids)
            .fetch_all(&pool)
            .await
            .unwrap();
            rows
        }
    };
    let lease = |id: String| {
        let pool = state.write_pool.clone();
        async move {
            let lease: String =
                sqlx::query_scalar("SELECT lease_expires_at FROM runner_sessions WHERE id = $1")
                    .bind(&id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            lease
        }
    };
    let rows_before = written(ids.clone()).await;
    let lease_before = lease(session_id.clone()).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let (status, ack) = heartbeat_of(&state, &token, &session_id, held).await;
    assert_eq!(status, StatusCode::OK, "{ack}");
    assert!(ack["cancel"].as_array().unwrap().is_empty());
    assert!(
        lease(session_id).await > lease_before,
        "the session is renewed"
    );
    assert_eq!(
        written(ids).await,
        rows_before,
        "and no task row is written"
    );
}

/// A task that asks for an identity is given a request credential of
/// its own, minted here, which dagron-api's issuer takes while the task runs;
/// by a dagron-api that is no issuer, the task is failed saying so.
/// A task that asks for its run's artifacts is given a credential of its
/// attempt's own with its job, which names it while it runs.
#[tokio::test]
async fn a_task_s_artifact_credential_is_minted_for_its_runner() {
    let Some(state) = test_state("runner-artifact-api").await else {
        return;
    };
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 2, Secrets::Server).await;
    let run = run_of(
        &state,
        &label,
        "  - { name: fix, command: [\"true\"], artifact_api: true }\n  - { name: plain, command: [\"true\"] }\n",
    )
    .await;
    let claimed = claim_now(&state, &token, &session_id).await;
    let jobs = claimed["tasks"].as_array().unwrap();
    assert_eq!(jobs.len(), 2, "{claimed}");
    let job = |name: &str| jobs.iter().find(|j| j["name"] == name).unwrap();
    assert!(job("plain").get("artifact_api").is_none(), "{claimed}");
    let credential = job("fix")["artifact_api"]["token"]
        .as_str()
        .unwrap()
        .to_string();
    let (holder, status) = dagron_core::artifact_api::holder(&state.read_pool, &credential)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (holder.run_id.as_str(), holder.task_name.as_str()),
        (run.as_str(), "fix")
    );
    assert_eq!(status.as_deref(), Some("running"));
}

#[tokio::test]
async fn a_task_s_identity_is_minted_for_its_runner_and_trades_for_a_token() {
    let Some(state) = test_state("runner-identity").await else {
        return;
    };
    let tasks = "  - name: deploy\n    command: [\"true\"]\n    identity:\n      oidc:\n        audience: [sts.amazonaws.com, https://iam.example]\n";

    // No issuer: failed, saying why.
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 1, Secrets::Server).await;
    let run = run_of(&state, &label, tasks).await;
    let claimed = claim_now(&state, &token, &session_id).await;
    assert!(claimed["tasks"].as_array().unwrap().is_empty(), "{claimed}");
    let (status, output): (String, Option<String>) =
        sqlx::query_as("SELECT status, output FROM task_runs WHERE run_id = $1")
            .bind(&run)
            .fetch_one(&state.write_pool)
            .await
            .unwrap();
    assert_eq!(status, "failed");
    assert!(output.unwrap().contains("DAGRON_OIDC_ISSUER"));

    // An issuer: the job carries the credential and the task's first audience.
    let pem = {
        use rsa::pkcs8::EncodePrivateKey;
        rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048)
            .unwrap()
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string()
    };
    let mut state = state;
    state.oidc = Some(std::sync::Arc::new(
        crate::oidc::Issuer::new(
            "https://ci.example.com",
            vec![crate::oidc::key_of(&pem).unwrap()],
            600,
        )
        .unwrap(),
    ));
    let label = class();
    let (_, token) = runner(&state, &label).await;
    let session_id = session(&state, &token, 1, Secrets::Server).await;
    run_of(&state, &label, tasks).await;
    let claimed = claim_now(&state, &token, &session_id).await;
    let job = &claimed["tasks"][0];
    assert_eq!(job["name"], "deploy", "{claimed}");
    assert_eq!(job["identity"]["audience"], "sts.amazonaws.com");
    let credential = job["identity"]["token"].as_str().unwrap();
    assert!(credential.starts_with("dgo_"));
    let asked = |audience: &str| {
        let state = state.clone();
        let headers = bearer(credential);
        let query = serde_json::from_value(serde_json::json!({ "audience": audience })).unwrap();
        async move { crate::oidc::token(State(state), headers, Query(query)).await }
    };
    let Json(answer) = asked("https://iam.example").await.unwrap();
    let jwt = answer["value"].as_str().unwrap();
    let claims: Value = {
        use base64::Engine as _;
        let part = jwt.split('.').nth(1).unwrap();
        serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(part)
                .unwrap(),
        )
        .unwrap()
    };
    assert_eq!(claims["aud"], "https://iam.example");
    assert_eq!(claims["task"], "deploy");
    assert_eq!(claims["attempt"], 1);
    assert_eq!(
        asked("another").await.unwrap_err().0,
        StatusCode::FORBIDDEN,
        "an audience the task was not given"
    );
}
