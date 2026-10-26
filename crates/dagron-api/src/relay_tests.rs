//! The live log relay through its routes, on a one-node
//! dagron-api: a task's log written as the engine writes it, into no store, a
//! `file://` store and a store that keeps the open part in the row, and
//! followed by streams opened before it began, in its middle and after its
//! end. What every stream must hold to: each line once, in order, the log the
//! task view reads; a cursor goes on from exactly where it says.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use dagron_artifact::log_segments::{LogKey, SegmentStore};
use dagron_core::db;
use futures::StreamExt;
use serde_json::Value;
use tower::ServiceExt;

use crate::one_node_tests::{one_node, signed_in};
use crate::relay::{Relay, Settings};
use crate::state::AppState;

/// The relay set to answer within a test's patience.
fn quick() -> Settings {
    Settings {
        poll: Duration::from_millis(30),
        linger: Duration::from_millis(100),
        min_gap: Duration::from_millis(5),
        ..Settings::default()
    }
}

/// A one-node dagron-api with `relay` and `store`, and a session.
async fn relayed(settings: Settings, store: Option<SegmentStore>) -> (Router, AppState, String) {
    let (_, mut state) = one_node().await;
    state.relay = Arc::new(Relay::new(settings));
    state.log_store = store.map(Arc::new);
    let app = crate::build_app(state.clone(), "").unwrap();
    let session = signed_in(&app).await;
    (app, state, session)
}

/// A run of one task, claimed: the run, the task, and its attempt's fence.
async fn running(state: &AppState) -> (String, String, i64) {
    let pool = &state.write_pool;
    let yaml = "name: followed\ntasks:\n  - name: talk\n    command: [\"true\"]\n";
    let dag = dagron_core::dag::DagGraph::from_yaml(yaml).unwrap();
    let run = db::create_run(pool, &dag, yaml).await.unwrap();
    db::advance_ready_tasks(pool).await.unwrap();
    let claimed = db::claim_ready(pool, "w", 10).await.unwrap();
    let task = claimed.iter().find(|t| t.run_id == run).expect("claimed");
    (run, task.id.clone(), task.version + 1)
}

async fn settle(state: &AppState, task: &str, status: &str) {
    sqlx::query("UPDATE task_runs SET status = ?1, finished_at = ?2 WHERE id = ?3")
        .bind(status)
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(task)
        .execute(&state.write_pool)
        .await
        .unwrap();
}

/// A task's log written as the engine writes it: begun once, then added
/// to, a segment closed at `size` bytes, cut at a line's end.
struct Writer {
    pool: db::Pool,
    store: Option<SegmentStore>,
    task: String,
    fence: i64,
    gen: i64,
    seq: i64,
    open: String,
    size: usize,
}

impl Writer {
    async fn begin(state: &AppState, task: &str, fence: i64, size: usize) -> Writer {
        let pool = state.write_pool.clone();
        let store = state.log_store.as_deref().cloned();
        let text = "";
        let gen = match &store {
            None => {
                db::append_task_output(&pool, task, fence, text, true)
                    .await
                    .unwrap();
                db::log_open_state(&pool, task, fence)
                    .await
                    .unwrap()
                    .expect("its row")
                    .2
            }
            Some(_) => {
                db::reset_task_log(&pool, task, fence, text)
                    .await
                    .unwrap()
                    .expect("running")
                    .gen
            }
        };
        Writer {
            pool,
            store,
            task: task.to_string(),
            fence,
            gen,
            seq: 0,
            open: String::new(),
            size,
        }
    }

    fn key(&self) -> LogKey {
        LogKey::new(self.task.as_str(), self.fence, self.gen)
    }

    async fn write(&mut self, text: &str) {
        match &self.store {
            Some(store) if !store.open_in_row() => {
                store
                    .append_open(&self.key(), self.seq, text)
                    .await
                    .unwrap();
            }
            _ => {
                db::append_task_output(&self.pool, &self.task, self.fence, text, false)
                    .await
                    .unwrap();
            }
        }
        self.open.push_str(text);
        let Some(store) = self.store.clone() else {
            return;
        };
        while self.open.len() >= self.size {
            let cut = self.open[..self.size]
                .rfind('\n')
                .map_or(self.size, |at| at + 1);
            let piece = self.open[..cut].to_string();
            let rest = self.open[cut..].to_string();
            let key = self.key();
            let name = store.put_segment(&key, self.seq, &piece).await.unwrap();
            if !store.open_in_row() {
                store.write_open(&key, self.seq + 1, &rest).await.unwrap();
            }
            let lines = piece.matches('\n').count() as i64;
            db::close_log_segments(
                &self.pool,
                &self.task,
                self.fence,
                &[(self.seq, name, piece.len() as i64, lines)],
                store.open_in_row().then_some(piece.chars().count() as i64),
            )
            .await
            .unwrap();
            if !store.open_in_row() {
                for older in 0..self.seq {
                    store.remove_open(&key, older).await.unwrap();
                }
            }
            self.seq += 1;
            self.open = rest;
        }
    }
}

/// A stream's frames, as a client reads them.
struct Frames {
    body: axum::body::BodyDataStream,
    buf: Vec<u8>,
}

type Frame = (String, Option<String>, Value);

async fn open(
    app: &Router,
    uri: &str,
    session: &str,
    extra: &[(&str, &str)],
) -> (StatusCode, Option<String>, Frames) {
    let mut req = Request::get(uri);
    if !session.is_empty() {
        req = req.header(
            header::COOKIE,
            format!("{}={session}", crate::auth::SESSION_COOKIE),
        );
    }
    for (k, v) in extra {
        req = req.header(*k, *v);
    }
    let mut req = req.body(Body::empty()).unwrap();
    req.extensions_mut()
        .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 40000))));
    let resp = app.clone().oneshot(req).await.unwrap();
    let retry = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    (
        resp.status(),
        retry,
        Frames {
            body: resp.into_body().into_data_stream(),
            buf: Vec::new(),
        },
    )
}

impl Frames {
    /// The next frame; `None` where the stream ended.
    async fn next(&mut self) -> Option<Frame> {
        loop {
            if let Some(end) = self.buf.windows(2).position(|w| w == b"\n\n") {
                let block = String::from_utf8(self.buf.drain(..end + 2).collect()).unwrap();
                let (mut event, mut id, mut data) = ("message".to_string(), None, String::new());
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("event: ") {
                        event = v.to_string();
                    } else if let Some(v) = line.strip_prefix("id: ") {
                        id = Some(v.to_string());
                    } else if let Some(v) = line.strip_prefix("data: ") {
                        data.push_str(v);
                    }
                }
                if data.is_empty() {
                    continue;
                }
                return Some((event, id, serde_json::from_str(&data).unwrap()));
            }
            match tokio::time::timeout(Duration::from_secs(20), self.body.next()).await {
                Err(_) => panic!(
                    "the stream stalled: {:?}",
                    String::from_utf8_lossy(&self.buf)
                ),
                Ok(None) => return None,
                Ok(Some(chunk)) => self.buf.extend_from_slice(&chunk.unwrap()),
            }
        }
    }

    /// The frames up to and including the first `kind`.
    async fn until(&mut self, kind: &str) -> Vec<Frame> {
        let mut out = Vec::new();
        loop {
            let frame = self
                .next()
                .await
                .unwrap_or_else(|| panic!("ended before {kind}: {out:?}"));
            let done = frame.0 == kind;
            out.push(frame);
            if done {
                return out;
            }
        }
    }

    /// Everything to the stream's end.
    async fn all(&mut self) -> Vec<Frame> {
        let mut out = Vec::new();
        while let Some(frame) = self.next().await {
            out.push(frame);
        }
        out
    }
}

/// The text the `log` frames carried, joined.
fn text_of(frames: &[Frame]) -> String {
    frames
        .iter()
        .filter(|f| f.0 == "log")
        .map(|f| f.2["text"].as_str().unwrap_or_default())
        .collect()
}

fn kinds(frames: &[Frame]) -> Vec<&str> {
    let mut kinds: Vec<&str> = frames.iter().map(|f| f.0.as_str()).collect();
    kinds.dedup();
    kinds
}

/// The task view's whole log.
async fn whole(app: &Router, session: &str, run: &str, task: &str) -> String {
    let (status, _, mut body) = open(
        app,
        &format!("/api/runs/{run}/tasks/{task}/logs"),
        session,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let mut bytes = Vec::new();
    while let Some(chunk) = body.body.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    v["output"].as_str().unwrap().to_string()
}

async fn eventually(what: &str, mut ok: impl FnMut() -> bool) {
    for _ in 0..200 {
        if ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("never: {what}");
}

/// A log with no store: followed from its start as it is written, a line
/// not yet ended held until it ends, then its end and `eof`; one reader for
/// the run, let go once nobody follows it; read again from a cursor, the rest
/// exactly; one task followed carries its cursor as each frame's id, and
/// goes on from `Last-Event-ID`.
#[tokio::test]
async fn a_log_is_followed_from_its_start_and_from_a_cursor() {
    let (app, state, s) = relayed(quick(), None).await;
    let (run, task, fence) = running(&state).await;
    let mut w = Writer::begin(&state, &task, fence, usize::MAX).await;
    w.write("one\ntwo\n").await;

    let (status, _, mut live) = open(
        &app,
        &format!("/api/runs/{run}/logs/stream?from=start"),
        &s,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let first = live.until("log").await;
    assert_eq!(first[0].0, "start");
    assert_eq!(first[0].2["why"], "open");
    assert_eq!(first[0].2["from"], 0);
    assert_eq!(text_of(&first), "one\ntwo\n");
    assert_eq!(state.relay.watched(), 1, "one reader for the run");

    w.write("three\nfou").await;
    let next = live.until("log").await;
    assert_eq!(text_of(&next), "three\n", "a line not ended waits");
    w.write("r\n").await;
    let next = live.until("log").await;
    assert_eq!(text_of(&next), "four\n");

    settle(&state, &task, "succeeded").await;
    let rest = live.all().await;
    assert_eq!(kinds(&rest), ["end", "eof"], "{rest:?}");
    let gen = w.gen;
    assert_eq!(rest[0].2["status"], "succeeded");
    assert_eq!(rest[0].2["complete"], true);
    assert_eq!(rest[0].2["cursor"], format!("{task}:{fence}:{gen}:19"));
    let whole = whole(&app, &s, &run, &task).await;
    assert_eq!(whole, "one\ntwo\nthree\nfour\n");
    drop(live);
    let relay = Arc::clone(&state.relay);
    eventually("the run let go", || relay.watched() == 0).await;

    // From a cursor: the rest, exactly, of a log that is over (not watched).
    let cursor = format!("{task}:{fence}:{gen}:4");
    let (_, _, mut again) = open(
        &app,
        &format!("/api/runs/{run}/logs/stream?cursor={cursor}"),
        &s,
        &[],
    )
    .await;
    let frames = again.all().await;
    assert_eq!(kinds(&frames), ["start", "log", "end", "eof"]);
    assert_eq!(frames[0].2["from"], 4);
    assert_eq!(text_of(&frames), "two\nthree\nfour\n");
    assert_eq!(
        state.relay.watched(),
        0,
        "a run that is over is not watched"
    );

    // One task: each frame's id is its cursor, and Last-Event-ID goes on.
    let uri = format!("/api/runs/{run}/tasks/{task}/logs/stream?from=start");
    let (_, _, mut one) = open(
        &app,
        &uri,
        &s,
        &[("last-event-id", &format!("{task}:{fence}:{gen}:8"))],
    )
    .await;
    let frames = one.all().await;
    assert_eq!(text_of(&frames), "three\nfour\n");
    let log = frames.iter().find(|f| f.0 == "log").unwrap();
    assert_eq!(
        log.1.as_deref(),
        Some(format!("{task}:{fence}:{gen}:19").as_str())
    );
    assert_eq!(log.2["cursor"], format!("{task}:{fence}:{gen}:19"));

    // The task view gives a cursor a stream goes on from.
    let (_, _, mut view) = open(&app, &format!("/api/runs/{run}/tasks/{task}/logs"), &s, &[]).await;
    let mut bytes = Vec::new();
    while let Some(chunk) = view.body.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["cursor"], format!("{task}:{fence}:{gen}:19"));

    // An API token, whose session lasts as long as it does: a stream of it
    // opens, and ends as any.
    let (status, _, token) = crate::one_node_tests::call(
        &app,
        "POST",
        "/api/tokens",
        Some(&s),
        Some(serde_json::json!({ "name": "follow" })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{token}");
    let bearer = format!("Bearer {}", token["token"].as_str().unwrap());
    let (status, _, mut by_token) = open(
        &app,
        &format!("/api/runs/{run}/logs/stream?from=start"),
        "",
        &[("authorization", &bearer)],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text_of(&by_token.all().await), "one\ntwo\nthree\nfour\n");
}

/// A log written again from its start (sealed: masked), followed: the
/// stream says so and sends the new text; another attempt, begun from its
/// start; a filter's lines alone.
#[tokio::test]
async fn a_log_written_again_and_another_attempt_are_each_begun() {
    let (app, state, s) = relayed(quick(), None).await;
    let (run, task, fence) = running(&state).await;
    let mut w = Writer::begin(&state, &task, fence, usize::MAX).await;
    w.write("token hunter2\nINFO fine\n").await;
    let (_, _, mut live) = open(
        &app,
        &format!("/api/runs/{run}/logs/stream?from=start"),
        &s,
        &[],
    )
    .await;
    assert_eq!(
        text_of(&live.until("log").await),
        "token hunter2\nINFO fine\n"
    );
    let (_, _, mut errors) = open(
        &app,
        &format!("/api/runs/{run}/logs/stream?from=start&level=error"),
        &s,
        &[],
    )
    .await;
    errors.until("log").await;

    // Sealed: written again from its start, masked.
    db::append_task_output(
        &state.write_pool,
        &task,
        fence,
        "token ***\nINFO fine\n",
        true,
    )
    .await
    .unwrap();
    let frames = live.until("log").await;
    let start = frames.iter().find(|f| f.0 == "start").expect("a start");
    assert_eq!(start.2["why"], "rewritten");
    assert_eq!(text_of(&frames), "token ***\nINFO fine\n");

    // Another attempt: the row claimed again, its log begun.
    sqlx::query("UPDATE task_runs SET version = version + 1, attempt = attempt + 1, log = NULL WHERE id = ?1")
        .bind(&task)
        .execute(&state.write_pool)
        .await
        .unwrap();
    db::append_task_output(&state.write_pool, &task, fence + 1, "ERROR again\n", true)
        .await
        .unwrap();
    let frames = live.until("log").await;
    let start = frames.iter().find(|f| f.0 == "start").expect("a start");
    assert_eq!(start.2["why"], "attempt");
    assert_eq!(text_of(&frames), "ERROR again\n");
    settle(&state, &task, "failed").await;
    assert_eq!(kinds(&live.all().await), ["end", "eof"]);
    // Filtered: the error line alone, of every writing it followed.
    let frames = errors.all().await;
    let lines: Vec<Value> = frames
        .iter()
        .filter(|f| f.0 == "log")
        .flat_map(|f| f.2["lines"].as_array().unwrap().clone())
        .collect();
    assert_eq!(lines.len(), 1, "{frames:?}");
    assert_eq!(lines[0]["text"], "ERROR again");
    assert_eq!(lines[0]["level"], "error");
    assert!(
        frames.iter().all(|f| f.2.get("text").is_none()),
        "lines, not text, under a filter"
    );
    assert_eq!(whole(&app, &s, &run, &task).await, "ERROR again\n");
}

/// Written as the engine writes it, into a `file://` store and into one
/// that keeps the open part in the row, segments closing under the
/// readers: streams opened before the log began, at staggered moments as it
/// is written, and after its end each send it whole, each line once, in
/// order, as the task view reads it.
#[tokio::test]
async fn a_log_kept_in_segments_is_followed_whole_across_closes() {
    for open_in_row in [false, true] {
        let dir = std::env::temp_dir().join(format!(
            "dagron-api-relay-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let store = match open_in_row {
            false => SegmentStore::Dir(dir.clone()),
            true => SegmentStore::DirOpenInRow(dir.clone()),
        };
        let (app, state, s) = relayed(quick(), Some(store)).await;
        let (run, task, fence) = running(&state).await;
        let uri = format!("/api/runs/{run}/logs/stream?from=start");
        let mut streams = vec![tokio::spawn({
            let (app, s, uri) = (app.clone(), s.clone(), uri.clone());
            async move { open(&app, &uri, &s, &[]).await.2.all().await }
        })];
        let mut w = Writer::begin(&state, &task, fence, 4096).await;
        let mut expect = String::new();
        for n in 0..3000 {
            let line = format!("line {n:05} {}\n", "x".repeat(n % 40));
            w.write(&line).await;
            expect.push_str(&line);
            if n % 100 == 0 {
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            if n % 600 == 300 {
                streams.push(tokio::spawn({
                    let (app, s, uri) = (app.clone(), s.clone(), uri.clone());
                    async move { open(&app, &uri, &s, &[]).await.2.all().await }
                }));
            }
        }
        assert!(w.seq >= 10, "{open_in_row}: segments closed: {}", w.seq);
        settle(&state, &task, "succeeded").await;
        assert_eq!(
            whole(&app, &s, &run, &task).await,
            expect,
            "{open_in_row}: the task view"
        );
        streams.push(tokio::spawn({
            let (app, s, uri) = (app.clone(), s.clone(), uri.clone());
            async move { open(&app, &uri, &s, &[]).await.2.all().await }
        }));
        for (n, stream) in streams.into_iter().enumerate() {
            let frames = stream.await.unwrap();
            assert_eq!(*kinds(&frames).last().unwrap(), "eof", "{open_in_row} #{n}");
            let text = text_of(&frames);
            assert!(
                text == expect,
                "{open_in_row} #{n}: {} bytes of {}",
                text.len(),
                expect.len()
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Where no cursor says: the last of the window, from a line's start;
/// `tail` and `limit`, its last lines; `from=end`, nothing before now.
#[tokio::test]
async fn a_stream_begins_at_its_window_unless_asked_otherwise() {
    let settings = Settings {
        window: 30,
        ..quick()
    };
    let (app, state, s) = relayed(settings, None).await;
    let (run, task, fence) = running(&state).await;
    let mut w = Writer::begin(&state, &task, fence, usize::MAX).await;
    w.write("first line of all\nsecond line\nthird line\nlast\n")
        .await;
    settle(&state, &task, "succeeded").await;
    let read = |q: &'static str| {
        let (app, s, run) = (app.clone(), s.clone(), run.clone());
        async move {
            text_of(
                &open(&app, &format!("/api/runs/{run}/logs/stream?{q}"), &s, &[])
                    .await
                    .2
                    .all()
                    .await,
            )
        }
    };
    assert_eq!(
        read("").await,
        "second line\nthird line\nlast\n",
        "the window, from a line's start"
    );
    assert_eq!(read("tail=1&limit=1").await, "last\n");
    assert_eq!(
        read("from=start").await,
        "first line of all\nsecond line\nthird line\nlast\n"
    );
    assert_eq!(read("from=end").await, "");
}

/// What a stream refuses, and the relay's cap: a run more than it may
/// watch is refused with a time to ask again, and let in once a run is let
/// go.
#[tokio::test]
async fn a_stream_is_refused_what_it_cannot_do_and_past_the_cap() {
    let settings = Settings {
        max_tails: 1,
        ..quick()
    };
    let (app, state, s) = relayed(settings, None).await;
    let (run, task, _) = running(&state).await;
    for (uri, want) in [
        (
            format!("/api/runs/{run}/logs/stream?context=2"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/api/runs/{run}/logs/stream?limit=5"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/api/runs/{run}/logs/stream?cursor=nope"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/api/runs/{run}/logs/stream?view=errors"),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/api/runs/no-such-run/logs/stream".to_string(),
            StatusCode::NOT_FOUND,
        ),
        (
            format!("/api/runs/{run}/tasks/no-such-task/logs/stream"),
            StatusCode::NOT_FOUND,
        ),
    ] {
        assert_eq!(open(&app, &uri, &s, &[]).await.0, want, "{uri}");
    }
    let (status, _, first) = open(&app, &format!("/api/runs/{run}/logs/stream"), &s, &[]).await;
    assert_eq!(status, StatusCode::OK);
    let (other, _, _) = running(&state).await;
    let (status, retry, _) = open(&app, &format!("/api/runs/{other}/logs/stream"), &s, &[]).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(retry.as_deref(), Some("5"));
    drop(first);
    let relay = Arc::clone(&state.relay);
    eventually("the first run let go", || relay.watched() == 0).await;
    let (status, _, _) = open(&app, &format!("/api/runs/{other}/logs/stream"), &s, &[]).await;
    assert_eq!(status, StatusCode::OK);
    settle(&state, &task, "succeeded").await;
}

/// A run of two tasks, the second after the first (or beside it), the
/// first claimed (or both): the run, each task's id, and the first's fence.
async fn two(state: &AppState, after: bool) -> (String, (String, i64), String) {
    let pool = &state.write_pool;
    let deps = if after {
        "\n    depends_on: [first]"
    } else {
        ""
    };
    let yaml = format!(
        "name: two\ntasks:\n  - name: first\n    command: [\"true\"]\n  - name: second\n    command: [\"true\"]{deps}\n"
    );
    let dag = dagron_core::dag::DagGraph::from_yaml(&yaml).unwrap();
    let run = db::create_run(pool, &dag, &yaml).await.unwrap();
    db::advance_ready_tasks(pool).await.unwrap();
    db::claim_ready(pool, "w", 10).await.unwrap();
    let id = |name: &'static str| {
        let run = run.clone();
        async move {
            sqlx::query_as::<_, (String, i64)>(
                "SELECT id, version FROM task_runs WHERE run_id = ?1 AND name = ?2",
            )
            .bind(&run)
            .bind(name)
            .fetch_one(pool)
            .await
            .unwrap()
        }
    };
    let (first, version) = id("first").await;
    let (second, _) = id("second").await;
    (run, (first, version), second)
}

/// The text a stream's `log` frames carried of one task.
fn text_of_task(frames: &[Frame], task: &str) -> String {
    frames
        .iter()
        .filter(|f| f.0 == "log" && f.2["task_id"] == task)
        .map(|f| f.2["text"].as_str().unwrap_or_default())
        .collect()
}

/// A task that has not begun is not over: it is started when a stream
/// opens, and ended once it has run, its log in between.
#[tokio::test]
async fn a_task_that_has_not_begun_is_ended_once_it_has_run() {
    let (app, state, s) = relayed(quick(), None).await;
    let (run, (first, fence), second) = two(&state, true).await;
    let mut w = Writer::begin(&state, &first, fence, usize::MAX).await;
    w.write("first\n").await;
    let (_, _, mut live) = open(
        &app,
        &format!("/api/runs/{run}/logs/stream?from=start"),
        &s,
        &[],
    )
    .await;
    let opened = live.until("log").await;
    settle(&state, &first, "succeeded").await;
    // Claimed, as an engine claims it.
    let fence: i64 = sqlx::query_scalar(
        "UPDATE task_runs SET status = 'running', claimed_by = 'w', attempt = attempt + 1,
             version = version + 1, log = NULL, log_fence = NULL, log_gen = NULL
         WHERE id = ?1 RETURNING version",
    )
    .bind(&second)
    .fetch_one(&state.write_pool)
    .await
    .unwrap();
    let mut w = Writer::begin(&state, &second, fence, usize::MAX).await;
    w.write("second\n").await;
    settle(&state, &second, "succeeded").await;
    let frames: Vec<Frame> = opened.into_iter().chain(live.all().await).collect();
    let of_second: Vec<&str> = frames
        .iter()
        .filter(|f| f.2["task_id"] == second.as_str())
        .map(|f| f.0.as_str())
        .collect();
    assert_eq!(of_second.first(), Some(&"start"), "{frames:?}");
    assert_eq!(of_second.last(), Some(&"end"), "{frames:?}");
    assert_eq!(
        of_second.iter().filter(|k| **k == "end").count(),
        1,
        "one end, after it ran: {frames:?}"
    );
    assert_eq!(text_of_task(&frames, &second), "second\n");
    let end = frames
        .iter()
        .find(|f| f.0 == "end" && f.2["task_id"] == second.as_str())
        .unwrap();
    assert_eq!(end.2["status"], "succeeded");
    assert_eq!(frames.last().unwrap().0, "eof");
}

/// A stream whose client fell behind its run's channel reads what it missed
/// from the store: another attempt of a task that had ended, begun and over
/// while it was behind, among it.
#[tokio::test]
async fn a_stream_that_fell_behind_reads_what_it_missed() {
    let settings = Settings { ring: 1, ..quick() };
    let (app, state, s) = relayed(settings, None).await;
    let (run, (first, fence), second) = two(&state, false).await;
    let second_fence: i64 = sqlx::query_scalar("SELECT version FROM task_runs WHERE id = ?1")
        .bind(&second)
        .fetch_one(&state.write_pool)
        .await
        .unwrap();
    let mut a = Writer::begin(&state, &first, fence, usize::MAX).await;
    let mut b = Writer::begin(&state, &second, second_fence, usize::MAX).await;
    b.write("second once\n").await;
    settle(&state, &second, "failed").await;
    let (_, _, mut live) = open(
        &app,
        &format!("/api/runs/{run}/logs/stream?from=start"),
        &s,
        &[],
    )
    .await;
    let mut seen = Vec::new();
    while !seen
        .iter()
        .any(|f: &Frame| f.0 == "end" && f.2["task_id"] == second.as_str())
    {
        seen.push(live.next().await.expect("the second's end"));
    }
    // Not read from now: the stream falls behind as the first writes, and
    // the second runs again, from its start to its end, meanwhile.
    let mut expect = String::new();
    for n in 0..200 {
        let line = format!("first {n:03}\n");
        a.write(&line).await;
        expect.push_str(&line);
        if n % 10 == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if n == 100 {
            sqlx::query(
                "UPDATE task_runs SET version = version + 1, attempt = attempt + 1, status = 'running',
                     log = NULL, log_fence = NULL, log_gen = NULL WHERE id = ?1",
            )
            .bind(&second)
            .execute(&state.write_pool)
            .await
            .unwrap();
            let mut again = Writer::begin(&state, &second, second_fence + 1, usize::MAX).await;
            again.write("second again\n").await;
            settle(&state, &second, "succeeded").await;
        }
    }
    settle(&state, &first, "succeeded").await;
    let rest = live.all().await;
    let frames: Vec<Frame> = seen.into_iter().chain(rest).collect();
    assert_eq!(
        text_of_task(&frames, &first),
        expect,
        "the first's log, each line once"
    );
    assert_eq!(
        text_of_task(&frames, &second),
        "second once\nsecond again\n"
    );
    assert!(
        frames.iter().any(|f| f.0 == "start"
            && f.2["task_id"] == second.as_str()
            && f.2["why"] == "attempt"),
        "the second's next attempt begun: {frames:?}"
    );
    assert_eq!(frames.last().unwrap().0, "eof");
}

/// A stream whose client stops reading ends, and lets its run go; one ends
/// after the relay's life of a stream; and no more streams are opened than
/// may be.
#[tokio::test]
async fn a_stream_ends_with_a_client_that_stopped_reading_and_after_its_life() {
    let settings = Settings {
        stalled: Duration::from_millis(200),
        life: Duration::from_millis(1500),
        max_streams: 2,
        ..quick()
    };
    let (app, state, s) = relayed(settings, None).await;
    let (run, task, fence) = running(&state).await;
    let mut w = Writer::begin(&state, &task, fence, usize::MAX).await;
    let uri = format!("/api/runs/{run}/logs/stream?from=start");
    let (_, _, stalled) = open(&app, &uri, &s, &[]).await;
    let (_, _, mut lasting) = open(&app, &uri, &s, &[]).await;
    assert_eq!(
        open(&app, &uri, &s, &[]).await.0,
        StatusCode::TOO_MANY_REQUESTS,
        "no more streams than may be"
    );
    for n in 0..40 {
        w.write(&format!("line {n}\n")).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let frames = lasting.all().await;
    assert_ne!(
        frames.last().map(|f| f.0.as_str()),
        Some("eof"),
        "a stream at its life's end ends without eof"
    );
    assert!(text_of(&frames).starts_with("line 0\n"));
    let relay = Arc::clone(&state.relay);
    eventually("the stalled stream let its run go", || relay.watched() == 0).await;
    drop(stalled);
    assert_eq!(
        open(&app, &uri, &s, &[]).await.0,
        StatusCode::OK,
        "a stream ended is one fewer"
    );
    settle(&state, &task, "succeeded").await;
}
