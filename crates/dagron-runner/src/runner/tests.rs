//! The run loop against a stub of dagron-api's protocol, in-process, with the
//! local executor: what it claims runs, its log and result go up, a task it
//! is told to stop is stopped, a session that is gone is replaced, and a
//! drain ends it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, post};
use axum::{Json, Router};
use dagron_core::isolation::{Enforcer, ExecutorKind};
use dagron_core::runner::{Claimed, Heartbeat, Job, LogSegment, Report, Secrets};
use dagron_executor::executor::LocalExecutor;
use serde_json::json;

use super::*;

#[derive(Default)]
struct Stub {
    jobs: Vec<Job>,
    sessions: Vec<String>,
    /// Sessions answered 410 from now on.
    gone: Vec<String>,
    /// Tasks a heartbeat says to stop.
    cancel: Vec<String>,
    /// Said with the next claim and every heartbeat.
    drain: bool,
    heartbeats: Vec<(String, Vec<String>)>,
    logs: Vec<(String, u64, String)>,
    /// The numbers of the log segments that replace the log.
    resets: Vec<u64>,
    /// Log segments kept and then answered 503, as if the answer were lost.
    lose_log_answers: usize,
    /// Every log segment refused, 409: the log is past what is kept of one.
    refuse_logs: bool,
    /// How long a log segment of each of these tasks takes to be answered.
    slow_logs: HashMap<String, Duration>,
    results: Vec<(String, Report)>,
    closed: Vec<String>,
    /// Each segment and each result as it came, and when.
    events: Vec<(std::time::Instant, String)>,
}

type Shared = Arc<Mutex<Stub>>;

async fn open(State(stub): State<Shared>) -> Response {
    let mut stub = stub.lock().unwrap();
    let id = format!("s{}", stub.sessions.len() + 1);
    stub.sessions.push(id.clone());
    (
        StatusCode::CREATED,
        Json(json!({
            "session_id": id, "protocol": 0, "labels": ["test"], "capacity": 2,
            "lease_secs": 3, "heartbeat_secs": 1,
            "lease_expires_at": "2099-01-01T00:00:00+00:00",
        })),
    )
        .into_response()
}

async fn claim(State(stub): State<Shared>, Path(id): Path<String>) -> Response {
    {
        let mut s = stub.lock().unwrap();
        if s.gone.contains(&id) {
            return StatusCode::GONE.into_response();
        }
        if !s.jobs.is_empty() {
            let tasks = std::mem::take(&mut s.jobs);
            return Json(Claimed {
                tasks,
                drain: s.drain,
            })
            .into_response();
        }
        if s.drain {
            return Json(Claimed {
                tasks: vec![],
                drain: true,
            })
            .into_response();
        }
    }
    // A long-poll that found nothing.
    tokio::time::sleep(Duration::from_millis(100)).await;
    Json(Claimed::default()).into_response()
}

async fn heartbeat(
    State(stub): State<Shared>,
    Path(id): Path<String>,
    Json(hb): Json<Heartbeat>,
) -> Response {
    let mut s = stub.lock().unwrap();
    if s.gone.contains(&id) {
        return StatusCode::GONE.into_response();
    }
    let running: Vec<String> = hb.running.iter().map(|h| h.task_id.clone()).collect();
    let cancel: Vec<String> = running
        .iter()
        .filter(|t| s.cancel.contains(t))
        .cloned()
        .collect();
    s.heartbeats.push((id, running));
    Json(json!({ "lease_expires_at": "2099-01-01T00:00:00+00:00", "cancel": cancel, "drain": s.drain }))
        .into_response()
}

async fn close(State(stub): State<Shared>, Path(id): Path<String>) -> Response {
    stub.lock().unwrap().closed.push(id);
    Json(json!({ "ended": true })).into_response()
}

async fn log(
    State(stub): State<Shared>,
    Path((task, seq)): Path<(String, u64)>,
    Json(seg): Json<LogSegment>,
) -> Response {
    let slow = stub.lock().unwrap().slow_logs.get(&task).copied();
    if let Some(slow) = slow {
        tokio::time::sleep(slow).await;
    }
    let mut s = stub.lock().unwrap();
    if seg.reset {
        s.resets.push(seq);
    }
    s.events
        .push((std::time::Instant::now(), format!("log {task} {seq}")));
    s.logs.push((task, seq, seg.text));
    if s.refuse_logs {
        return StatusCode::CONFLICT.into_response();
    }
    if s.lose_log_answers > 0 {
        s.lose_log_answers -= 1;
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn result(
    State(stub): State<Shared>,
    Path(task): Path<String>,
    Json(report): Json<Report>,
) -> Response {
    let mut s = stub.lock().unwrap();
    s.events
        .push((std::time::Instant::now(), format!("result {task}")));
    s.results.push((task, report));
    Json(json!({ "recorded": true })).into_response()
}

/// A stub dagron-api, served on a port of its own: its URL.
async fn serve(stub: Shared) -> String {
    let app = Router::new()
        .route("/runner/v1/sessions", post(open))
        .route("/runner/v1/sessions/{id}", delete(close))
        .route("/runner/v1/sessions/{id}/claim", post(claim))
        .route("/runner/v1/sessions/{id}/heartbeat", post(heartbeat))
        .route("/runner/v1/tasks/{id}/logs/{seq}", post(log))
        .route("/runner/v1/tasks/{id}/result", post(result))
        .with_state(stub);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn job(task_id: &str, script: &str, env: Vec<dagron_core::dag::EnvVar>) -> Job {
    let mut job: Job = serde_json::from_value(json!({
        "task_id": task_id, "run_id": "run-1", "name": task_id, "fence": 5, "attempt": 0,
        "command": ["sh", "-c", script],
    }))
    .unwrap();
    job.env = env;
    job
}

/// Worker actors are registered by name, process-wide: one runner at a time.
/// Each test holds this until its runner has stopped.
static ONE_RUNNER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start(
    stub: &Shared,
    secrets: Secrets,
) -> (tokio::task::JoinHandle<Result<()>>, watch::Sender<Shutdown>) {
    let url = serve(Arc::clone(stub)).await;
    let cfg = Config {
        url: url.clone(),
        name: None,
        labels: vec![],
        capacity: 2,
        executor: ExecutorKind::Local,
        secrets,
        token_file: "unused".into(),
        join_token_file: None,
        drain_timeout: None,
        oidc_request_url: format!("{url}/oidc/token"),
        artifact_api_url: format!("{url}/task/v1/artifacts"),
    };
    let host = Host {
        kind: ExecutorKind::Local,
        executor: Arc::new(LocalExecutor::default()),
        enforcer: Enforcer::from(ExecutorKind::Local),
        floor: None,
        docker_daemon: None,
        installation: None,
        secrets,
        oidc_request_url: format!("{url}/oidc/token"),
        artifact_api_url: format!("{url}/task/v1/artifacts"),
        metrics: Arc::default(),
    };
    let runner = Runner::new(cfg, Client::new(&url).unwrap().with_token("dgr_test"), host)
        .await
        .unwrap();
    let (tx, rx) = watch::channel(Shutdown::No);
    (tokio::spawn(runner.run(rx)), tx)
}

/// Wait up to 20 s for the stub to say `done`.
async fn until(stub: &Shared, what: &str, done: impl Fn(&Stub) -> bool) {
    for _ in 0..200 {
        if done(&stub.lock().unwrap()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn a_claimed_task_runs_and_its_log_and_result_go_up() {
    let _one = ONE_RUNNER.lock().await;
    let stub: Shared = Default::default();
    stub.lock()
        .unwrap()
        .jobs
        .push(job("t-ok", "echo hello; echo bye", vec![]));
    let (run, stop) = start(&stub, Secrets::Server).await;
    until(&stub, "the result", |s| !s.results.is_empty()).await;
    {
        let s = stub.lock().unwrap();
        let (task, report) = &s.results[0];
        assert_eq!(
            (task.as_str(), report.fence, report.session_id.as_str()),
            ("t-ok", 5, "s1")
        );
        assert_eq!(report.outcome, Outcome::Succeeded);
        assert!(
            report.output.contains("hello") && report.output.contains("bye"),
            "{}",
            report.output
        );
        // Its log went up before its result, from its first segment.
        let log: String = s
            .logs
            .iter()
            .filter(|(t, _, _)| t == "t-ok")
            .map(|(_, _, text)| text.as_str())
            .collect();
        assert!(log.contains("hello"), "{log}");
        assert_eq!(s.logs.iter().find(|(t, _, _)| t == "t-ok").unwrap().1, 0);
    }
    stop.send(Shutdown::Drain).unwrap();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        stub.lock().unwrap().closed,
        ["s1", "s1"],
        "told it drains, then that it is over"
    );
}

/// A tick sends a segment of a task's log: the rest waits, due at the next,
/// or goes with its result.
#[tokio::test]
async fn a_tick_sends_a_segment_of_a_task_s_log_and_the_rest_waits() {
    let stub: Shared = Default::default();
    let client = Client::new(&serve(Arc::clone(&stub)).await)
        .unwrap()
        .with_token("dgr_test");
    let mut bufs = HashMap::new();
    let rest = "y".repeat(10);
    keep(
        &mut bufs,
        LogChunk {
            task_id: "t".into(),
            fence: 5,
            chunk: format!("{}{rest}", "x".repeat(LOG_SEGMENT_MAX)),
            first: true,
        },
    );
    flush(&client, "s1", "t", bufs.get_mut("t").unwrap(), 1).await;
    assert_eq!(stub.lock().unwrap().logs.len(), 1);
    let buf = &bufs["t"];
    assert_eq!(buf.pending, rest);
    assert!(buf.since.is_some(), "the rest is due at the next tick");
    flush_last(&client, "s1", "t", bufs.get_mut("t").unwrap()).await;
    let s = stub.lock().unwrap();
    assert_eq!(s.logs.len(), 2);
    assert_eq!(s.logs[1].2, rest);
}

/// A segment dagron-api may have written, its answer lost, goes again as the
/// same segment: what the task wrote since goes in the next, not in it, or
/// dagron-api, reading the number as written, would never keep it.
#[tokio::test]
async fn a_log_segment_whose_answer_is_lost_goes_again_as_it_was() {
    let stub: Shared = Default::default();
    let client = Client::new(&serve(Arc::clone(&stub)).await)
        .unwrap()
        .with_token("dgr_test");
    let chunk = |text: &str, first: bool| LogChunk {
        task_id: "t".into(),
        fence: 5,
        chunk: text.into(),
        first,
    };
    let mut bufs = HashMap::new();
    keep(&mut bufs, chunk("one\n", true));
    flush(&client, "s1", "t", bufs.get_mut("t").unwrap(), usize::MAX).await;
    keep(&mut bufs, chunk("two\n", false));
    stub.lock().unwrap().lose_log_answers = 1;
    flush(&client, "s1", "t", bufs.get_mut("t").unwrap(), usize::MAX).await;
    // The task writes on before the next tick.
    keep(&mut bufs, chunk("three\n", false));
    flush(&client, "s1", "t", bufs.get_mut("t").unwrap(), usize::MAX).await;
    let sent: Vec<(u64, String)> = stub
        .lock()
        .unwrap()
        .logs
        .iter()
        .map(|(_, seq, text)| (*seq, text.clone()))
        .collect();
    assert_eq!(
        sent,
        [
            (0, "one\n".to_string()),
            (1, "two\n".to_string()),
            (1, "two\n".to_string()),
            (2, "three\n".to_string()),
        ]
    );

    // A seal while one waits replaces it, as the whole log, and is numbered
    // past it: dagron-api may have written the one that waited.
    stub.lock().unwrap().lose_log_answers = 1;
    keep(&mut bufs, chunk("four\n", false));
    flush(&client, "s1", "t", bufs.get_mut("t").unwrap(), usize::MAX).await;
    keep(&mut bufs, chunk("one\ntwo\nthree\nfour\n", true));
    flush(&client, "s1", "t", bufs.get_mut("t").unwrap(), usize::MAX).await;
    keep(&mut bufs, chunk("five\n", false));
    flush(&client, "s1", "t", bufs.get_mut("t").unwrap(), usize::MAX).await;
    let s = stub.lock().unwrap();
    assert_eq!(
        s.logs[4..]
            .iter()
            .map(|(_, seq, text)| (*seq, text.as_str()))
            .collect::<Vec<_>>(),
        [(3, "four\n"), (4, "one\ntwo\nthree\nfour\n"), (5, "five\n")]
    );
    assert_eq!(s.resets, [0, 4], "the first, and the seal");
}

#[tokio::test]
async fn a_failing_task_is_said_failed_with_what_it_printed() {
    let _one = ONE_RUNNER.lock().await;
    let stub: Shared = Default::default();
    stub.lock()
        .unwrap()
        .jobs
        .push(job("t-fail", "echo broken >&2; exit 3", vec![]));
    let (run, stop) = start(&stub, Secrets::Server).await;
    until(&stub, "the result", |s| !s.results.is_empty()).await;
    {
        let s = stub.lock().unwrap();
        let report = &s.results[0].1;
        assert_eq!(report.outcome, Outcome::Failed);
        assert!(report.output.contains("broken"), "{}", report.output);
    }
    stop.send(Shutdown::Drain).unwrap();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_task_the_heartbeat_says_to_stop_is_stopped_and_said_cancelled() {
    let _one = ONE_RUNNER.lock().await;
    let stub: Shared = Default::default();
    {
        let mut s = stub.lock().unwrap();
        s.jobs.push(job("t-long", "sleep 60", vec![]));
        s.cancel.push("t-long".into());
    }
    let (run, stop) = start(&stub, Secrets::Server).await;
    until(&stub, "the cancelled result", |s| !s.results.is_empty()).await;
    assert_eq!(
        stub.lock().unwrap().results[0].1.outcome,
        Outcome::Cancelled
    );
    stop.send(Shutdown::Drain).unwrap();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_session_that_is_gone_has_its_tasks_stopped_and_another_is_opened() {
    let _one = ONE_RUNNER.lock().await;
    let stub: Shared = Default::default();
    stub.lock()
        .unwrap()
        .jobs
        .push(job("t-cut", "sleep 60", vec![]));
    let (run, stop) = start(&stub, Secrets::Server).await;
    until(&stub, "the task held", |s| {
        s.heartbeats
            .iter()
            .any(|(_, r)| r.contains(&"t-cut".to_string()))
    })
    .await;
    stub.lock().unwrap().gone.push("s1".into());
    until(&stub, "a second session", |s| s.sessions.len() >= 2).await;
    until(&stub, "the stopped task's result", |s| {
        !s.results.is_empty()
    })
    .await;
    {
        let s = stub.lock().unwrap();
        assert_eq!(s.results[0].1.outcome, Outcome::Cancelled);
        assert_eq!(s.results[0].1.session_id, "s1");
    }
    stop.send(Shutdown::Drain).unwrap();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// dagron-api gives a lost session's tasks to other runners, so a session
/// lost while it drains has its tasks stopped here too: the drain does not
/// wait for work that now runs elsewhere.
#[tokio::test]
async fn a_session_lost_while_it_drains_has_its_tasks_stopped() {
    let _one = ONE_RUNNER.lock().await;
    let stub: Shared = Default::default();
    stub.lock()
        .unwrap()
        .jobs
        .push(job("t-drain-cut", "sleep 60", vec![]));
    let (run, stop) = start(&stub, Secrets::Server).await;
    until(&stub, "the task held", |s| {
        s.heartbeats
            .iter()
            .any(|(_, r)| r.contains(&"t-drain-cut".to_string()))
    })
    .await;
    stop.send(Shutdown::Drain).unwrap();
    until(&stub, "the drain said", |s| s.closed.contains(&"s1".to_string())).await;
    stub.lock().unwrap().gone.push("s1".into());
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("the drain waited on a task its lost session no longer holds")
        .unwrap()
        .unwrap();
    let s = stub.lock().unwrap();
    assert_eq!(s.results.len(), 1);
    assert_eq!(s.results[0].1.outcome, Outcome::Cancelled);
}

#[tokio::test]
async fn a_drain_dagron_api_asks_for_lets_the_task_finish_and_ends_the_runner() {
    let _one = ONE_RUNNER.lock().await;
    let stub: Shared = Default::default();
    {
        let mut s = stub.lock().unwrap();
        s.jobs.push(job("t-drain", "sleep 1; echo done", vec![]));
        s.drain = true;
    }
    let (run, _stop) = start(&stub, Secrets::Server).await;
    // It exits by itself, once its task is over and said.
    tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let s = stub.lock().unwrap();
    assert_eq!(s.results.len(), 1);
    assert_eq!(s.results[0].1.outcome, Outcome::Succeeded);
    assert_eq!(s.closed.last().map(String::as_str), Some("s1"));
}

#[tokio::test]
async fn secrets_stay_home_and_are_masked_in_what_goes_up() {
    let _one = ONE_RUNNER.lock().await;
    // The runner's own store: DAGRON_SECRET_<NAME>.
    std::env::set_var("DAGRON_SECRET_RUNNER_TEST_TOKEN", "sup3r-s3cret-value");
    let stub: Shared = Default::default();
    let token = dagron_core::dag::EnvVar {
        name: "TOKEN".into(),
        value: String::new(),
        value_from: Some(dagron_core::dag::SecretRef {
            secret: "runner_test_token".into(),
            optional: false,
        }),
    };
    let missing = dagron_core::dag::EnvVar {
        name: "OTHER".into(),
        value: String::new(),
        value_from: Some(dagron_core::dag::SecretRef {
            secret: "runner_test_absent".into(),
            optional: false,
        }),
    };
    {
        let mut s = stub.lock().unwrap();
        s.jobs.push(job("t-home", "echo token=$TOKEN", vec![token]));
        s.jobs.push(job("t-absent", "echo never", vec![missing]));
    }
    let (run, stop) = start(&stub, Secrets::Home).await;
    until(&stub, "both results", |s| s.results.len() == 2).await;
    {
        let s = stub.lock().unwrap();
        let home = &s.results.iter().find(|(t, _)| t == "t-home").unwrap().1;
        assert_eq!(home.outcome, Outcome::Succeeded);
        assert!(
            !home.output.contains("sup3r-s3cret-value"),
            "{}",
            home.output
        );
        assert!(home.output.contains("token=***"), "{}", home.output);
        let absent = &s.results.iter().find(|(t, _)| t == "t-absent").unwrap().1;
        assert_eq!(absent.outcome, Outcome::Failed);
        assert!(
            absent.output.contains("runner_test_absent"),
            "{}",
            absent.output
        );
        assert!(s
            .logs
            .iter()
            .all(|(_, _, text)| !text.contains("sup3r-s3cret-value")));
    }
    stop.send(Shutdown::Drain).unwrap();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// The end of a task's log goes up before its result, though the answers to
/// it are lost: the same segment, number and text, until one is answered.
/// It used to go once, and was dropped at the first answer that was lost.
#[tokio::test]
async fn the_end_of_a_log_goes_up_before_its_result_through_lost_answers() {
    let _one = ONE_RUNNER.lock().await;
    let stub: Shared = Default::default();
    {
        let mut s = stub.lock().unwrap();
        s.jobs.push(job("t-lost", "echo hello; echo bye", vec![]));
        s.lose_log_answers = 2;
    }
    let (run, stop) = start(&stub, Secrets::Server).await;
    until(&stub, "the result", |s| !s.results.is_empty()).await;
    {
        let s = stub.lock().unwrap();
        let sent: Vec<(u64, &str)> = s
            .logs
            .iter()
            .filter(|(t, _, _)| t == "t-lost")
            .map(|(_, seq, text)| (*seq, text.as_str()))
            .collect();
        assert_eq!(sent, [(0, "hello\nbye\n"); 3], "sent again as it was");
        let said = s
            .events
            .iter()
            .position(|(_, e)| e == "result t-lost")
            .unwrap();
        assert!(
            s.events[..said]
                .iter()
                .filter(|(_, e)| e.starts_with("log t-lost"))
                .count()
                == 3
                && s.events[said..].iter().all(|(_, e)| !e.starts_with("log ")),
            "every segment before the result: {:?}",
            s.events.iter().map(|(_, e)| e).collect::<Vec<_>>()
        );
        assert_eq!(s.results[0].1.outcome, Outcome::Succeeded);
    }
    stop.send(Shutdown::Drain).unwrap();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// A log dagron-api keeps no more of (409) is given up at once, and the
/// result is said all the same.
#[tokio::test]
async fn a_log_dagron_api_will_not_keep_does_not_hold_up_the_result() {
    let _one = ONE_RUNNER.lock().await;
    let stub: Shared = Default::default();
    {
        let mut s = stub.lock().unwrap();
        s.jobs.push(job("t-full", "echo hello", vec![]));
        s.refuse_logs = true;
    }
    let (run, stop) = start(&stub, Secrets::Server).await;
    until(&stub, "the result", |s| !s.results.is_empty()).await;
    {
        let s = stub.lock().unwrap();
        assert_eq!(s.results[0].1.outcome, Outcome::Succeeded);
        assert_eq!(s.logs.len(), 1, "tried once: the log is not kept");
    }
    stop.send(Shutdown::Drain).unwrap();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// The end of one task's log, going up slowly after its result, does not
/// hold up another task's log, which goes up at its tick as it did.
#[tokio::test]
async fn a_tail_after_a_result_does_not_hold_up_another_task_s_log() {
    let _one = ONE_RUNNER.lock().await;
    let stub: Shared = Default::default();
    {
        let mut s = stub.lock().unwrap();
        // Three segments of a MiB at most, each answered a second and a
        // half late. On stderr, which is in the log and not the output: an
        // output so long is more than the stub takes in a result.
        s.jobs.push(job(
            "t-slow",
            "yes 'line of the slow task' | head -n 140000 >&2",
            vec![],
        ));
        s.jobs
            .push(job("t-quick", "sleep 1; echo quick; sleep 4", vec![]));
        s.slow_logs
            .insert("t-slow".into(), Duration::from_millis(1500));
    }
    let (run, stop) = start(&stub, Secrets::Server).await;
    until(&stub, "both results", |s| s.results.len() == 2).await;
    {
        let s = stub.lock().unwrap();
        let at = |what: &str| {
            s.events
                .iter()
                .find(|(_, e)| e.starts_with(what))
                .map(|(at, _)| *at)
                .unwrap_or_else(|| panic!("no {what}"))
        };
        assert!(
            s.logs.iter().filter(|(t, _, _)| t == "t-slow").count() >= 3,
            "the slow task's log is three segments"
        );
        assert!(
            at("log t-quick") < at("result t-slow"),
            "the quick task's line went up while the slow one's end was going up"
        );
        let slow: usize = s
            .logs
            .iter()
            .filter(|(t, _, _)| t == "t-slow")
            .map(|(_, _, text)| text.len())
            .sum();
        assert_eq!(slow, 140_000 * 22, "all of it, before its result");
    }
    stop.send(Shutdown::Drain).unwrap();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
