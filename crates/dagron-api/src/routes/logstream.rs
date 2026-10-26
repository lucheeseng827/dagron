//! A run's task logs followed live (`docs/LOG_PIPELINE.md` §5), as
//! server-sent events:
//!
//! - `GET /api/runs/{id}/logs/stream`: the run's tasks, narrowed by `task=`
//!   (an id or a name, repeatable or CSV) and `status=` (a task that matched
//!   once is followed to its end).
//! - `GET /api/runs/{id}/tasks/{tid}/logs/stream`: one task; every frame's
//!   SSE `id` is its cursor, so an EventSource goes on from where it was by
//!   itself (`Last-Event-ID`, which wins over a `cursor=` for the task).
//!
//! **Catch up, then follow, with no seam.** A position is a byte of the
//! writing of a task's log it is in (its attempt and its generation: a log
//! written again from its start, sealed, is another writing). A stream reads
//! each task from where it begins to where the log is, itself, a step at a
//! time; then it takes the relay's passes ([`crate::relay`]), each piece by
//! where it begins: what it has sent already is not sent again, and what it
//! has not seen (it began past where the stream is: a pass the stream was too
//! slow for, or a task the relay began following after the stream read it)
//! the stream reads itself first. So no line is lost or sent twice, and a
//! cursor (`<task_id>:<fence>:<gen>:<byte>`) is enough to go on from on any
//! replica: nothing of a client is kept here.
//!
//! **Where it begins.** At a task's cursor; else `from=start` (its first
//! byte), `from=end` (where it is now), or, by default, the last
//! [`Settings::window`](crate::relay::Settings::window) of it from a line's
//! start (§5.2: the end of a failing task is where the answer is);
//! `tail=1&limit=N` the last `N` lines in that window (that match).
//!
//! **Events**, each with JSON data:
//! - `start` `{task_id, task, attempt, cursor, from, size, why}`: before a
//!   task's first text, and for each writing after: `why` `open`, `attempt`
//!   (another attempt began) or `rewritten` (the log was written again from
//!   its start, masked): what a client held of the task is to be replaced,
//!   unless the cursor is the one it held. `size` is the log's bytes then.
//! - `log` `{task_id, task, attempt, cursor, text}`, or with a filter
//!   `{…, lines: [{level, ts?, text}]}`, the lines that match (a frame of none
//!   still moves the cursor on).
//! - `end` `{task_id, task, attempt, status, cursor, complete}`: the writing
//!   is over and sent; `complete: false` where it was gone before all of it
//!   could be read (a claim takes a retried task's row back).
//! - `eof` `{run_id}`: every followed task is over and sent (with `status=`,
//!   every task of the run). A stream ends after `eof` alone: any other end is
//!   a drop, to be resumed by cursor.
//!
//! The filter is the log views' line predicates (`q`, `exclude`, `regex`,
//! `level`, `case`); `context` is refused (a line's neighbours are not known
//! as it comes), and so is `limit` without `tail` (a stream is not cut).

use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use dagron_core::db::{self, LogTail};
use dagron_logging::logfilter::{self, LogFilter};
use futures::StreamExt;
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;

use crate::auth::AuthUser;
use crate::relay::{cut, over as writing_over, Got, Joined, Layout, Pass, Piece, What};
use crate::state::AppState;

/// The most cursors one request carries, and the most tasks and statuses it
/// names.
const MAX_CURSORS: usize = 1000;
const MAX_NAMED: usize = 1000;
/// How many steps of the relay's a catch-up reads at once, sent a step a
/// frame: a log is read the fewer times for it.
const CATCH_UP_STEPS: usize = 8;
/// How many times a read that found the log moved on under it is made again
/// before the stream lets that writing go.
const MOVED: u32 = 5;

/// A task's statuses that mean no more of its log will come.
fn is_terminal(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "skipped" | "cancelled")
}

/// Where in a writing of a task's log a client is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cursor {
    pub(crate) fence: i64,
    pub(crate) gen: i64,
    pub(crate) pos: u64,
}

/// `<task_id>:<fence>:<gen>:<byte>`.
pub(crate) fn parse_cursor(s: &str) -> Result<(String, Cursor), String> {
    let bad =
        || format!("{s:?} is not a cursor: <task_id>:<fence>:<gen>:<byte>, as a frame gives it");
    let mut parts = s.trim().rsplitn(4, ':');
    let (pos, gen, fence, task) = (parts.next(), parts.next(), parts.next(), parts.next());
    let (Some(pos), Some(gen), Some(fence), Some(task)) = (pos, gen, fence, task) else {
        return Err(bad());
    };
    if task.is_empty() || task.len() > 128 {
        return Err(bad());
    }
    let cursor = Cursor {
        fence: fence.parse().map_err(|_| bad())?,
        gen: gen.parse().map_err(|_| bad())?,
        pos: pos.parse().map_err(|_| bad())?,
    };
    Ok((task.to_string(), cursor))
}

fn cursor_of(task: &str, fence: i64, gen: i64, pos: u64) -> String {
    format!("{task}:{fence}:{gen}:{pos}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Begin {
    #[default]
    Window,
    Start,
    End,
}

/// What a stream was asked for.
#[derive(Debug, Default)]
struct Ask {
    tasks: Vec<String>,
    statuses: Vec<String>,
    cursors: HashMap<String, Cursor>,
    begin: Begin,
    /// `tail=1`: begin at the last this many lines of the window (that match).
    tail: Option<usize>,
    filter: Option<LogFilter>,
}

fn csv(v: &str) -> impl Iterator<Item = String> + '_ {
    v.split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(str::to_string)
}

fn parse(raw: Option<String>) -> Result<Ask, String> {
    let raw = raw.unwrap_or_default();
    let pairs = logfilter::parse_pairs(&raw);
    let filter = LogFilter::from_pairs(&pairs).map_err(|e| e.to_string())?;
    let mut ask = Ask::default();
    let (mut limit, mut tail) = (false, false);
    for (k, v) in &pairs {
        match k.as_str() {
            "task" | "tasks" => ask.tasks.extend(csv(v)),
            "status" | "statuses" => ask.statuses.extend(csv(v)),
            "cursor" | "cursors" => {
                for c in csv(v) {
                    let (task, cursor) = parse_cursor(&c)?;
                    ask.cursors.insert(task, cursor);
                }
                if ask.cursors.len() > MAX_CURSORS {
                    return Err(format!("at most {MAX_CURSORS} cursors"));
                }
            }
            "from" => {
                ask.begin = match v.as_str() {
                    "" | "tail" => Begin::Window,
                    "start" => Begin::Start,
                    "end" => Begin::End,
                    other => {
                        return Err(format!("from={other:?}: start, end, or tail (the default)"))
                    }
                }
            }
            "limit" => limit = true,
            "tail" => tail = logfilter::truthy(v),
            "context" => {
                return Err(
                    "a stream takes no context: a line's neighbours are not known as it comes"
                        .into(),
                )
            }
            "offset" => return Err("a stream goes on from a cursor, not an offset".into()),
            "q" | "contains" | "exclude" | "not" | "regex" | "level" | "levels" | "case" => {}
            other => {
                return Err(format!(
                    "{other:?} is not something a log stream takes: task, status, cursor, from, \
                     tail with limit, and the filter's q, exclude, regex, level and case"
                ))
            }
        }
    }
    if ask.tasks.len() > MAX_NAMED || ask.statuses.len() > MAX_NAMED {
        return Err(format!(
            "a stream names at most {MAX_NAMED} tasks and {MAX_NAMED} statuses"
        ));
    }
    if limit && !tail {
        return Err(
            "a stream is not cut at a number of lines: limit goes with tail, for where it begins"
                .into(),
        );
    }
    if tail {
        ask.tail = Some(filter.effective_limit());
    }
    ask.filter = (!filter.is_noop()).then_some(filter);
    Ok(ask)
}

/// `GET /api/runs/{id}/logs/stream`.
pub async fn run_log_stream(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Response {
    open(state, auth, id, None, raw, None).await
}

/// `GET /api/runs/{id}/tasks/{tid}/logs/stream`.
pub async fn task_log_stream(
    auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
) -> Response {
    let last = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    open(state, auth, id, Some(tid), raw, last).await
}

fn refuse(code: StatusCode, why: impl Into<String>) -> Response {
    (code, why.into()).into_response()
}

async fn open(
    state: AppState,
    auth: AuthUser,
    run: String,
    one: Option<String>,
    raw: Option<String>,
    last: Option<String>,
) -> Response {
    let mut ask = match parse(raw) {
        Ok(ask) => ask,
        Err(why) => return refuse(StatusCode::BAD_REQUEST, why),
    };
    if let Some(last) = last.filter(|l| !l.trim().is_empty()) {
        match parse_cursor(&last) {
            Ok((task, cursor)) => {
                ask.cursors.insert(task, cursor);
            }
            Err(why) => return refuse(StatusCode::BAD_REQUEST, format!("Last-Event-ID: {why}")),
        }
    }
    let rows = match db::log_tails(&state.read_pool, std::slice::from_ref(&run)).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "a log stream could not read its run");
            return refuse(StatusCode::INTERNAL_SERVER_ERROR, "database error");
        }
    };
    if rows.is_empty() {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT CAST(1 AS BIGINT) FROM workflow_runs WHERE id = $1",
        )
        .bind(&run)
        .fetch_optional(&state.read_pool)
        .await;
        match exists {
            Ok(Some(_)) => {}
            Ok(None) => return refuse(StatusCode::NOT_FOUND, "run not found"),
            Err(e) => {
                tracing::error!(error = %e, "a log stream could not read its run");
                return refuse(StatusCode::INTERNAL_SERVER_ERROR, "database error");
            }
        }
    }
    if let Some(tid) = &one {
        if !rows.iter().any(|r| &r.task_id == tid) {
            return refuse(StatusCode::NOT_FOUND, "task not found in this run");
        }
        ask.tasks = vec![tid.clone()];
        ask.statuses.clear();
    }
    let Some(slot) = state.relay.admit() else {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "5")],
            "as many log streams are open as may be: ask again",
        )
            .into_response();
    };
    let mut sub = Sub::new(state.clone(), run, one, ask, &auth);
    for row in &rows {
        sub.note(row);
    }
    // A run whose followed tasks are all over is read from the store and
    // done: it is not watched, and so never refused. One that is not is
    // joined before anything of it is read, so nothing written after the
    // read is missed.
    if !sub.over() {
        match state.relay.join(&state, &sub.run, sub.keys()) {
            Ok(joined) => {
                sub.joined = Some(joined);
                // What changed between the read and the join, heard.
                sub.jobs.push_back(Job::Statuses);
            }
            Err(_) => {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", "5")],
                    "the log relay follows as many runs as it may: ask again",
                )
                    .into_response()
            }
        }
    }
    let starts: Vec<LogTail> = rows
        .into_iter()
        .filter(|r| sub.follows.contains_key(&r.task_id))
        .collect();
    sub.pending_open = starts;
    // The stream is made on a task of its own, a few events ahead of its
    // client: one whose client does not take them in time ends, and lets its
    // run and the relay's text go.
    let stalled = state.relay.settings.stalled;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(8);
    tokio::spawn(async move {
        let _slot = slot;
        let events = sub.events();
        futures::pin_mut!(events);
        loop {
            let next = tokio::select! {
                next = events.next() => next,
                _ = tx.closed() => break,
            };
            let Some(event) = next else {
                break;
            };
            if tx.send_timeout(event, stalled).await.is_err() {
                break;
            }
        }
    });
    Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx))
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// How much of `rest` one frame takes, at most `size`: to a line's end
/// within it, or a character's.
fn frame_len(rest: &[u8], size: usize) -> usize {
    if rest.len() <= size {
        return rest.len();
    }
    if let Some(at) = rest[..size].iter().rposition(|b| *b == b'\n') {
        return at + 1;
    }
    let mut at = size;
    while at > 0 && rest[at] & 0xC0 == 0x80 {
        at -= 1;
    }
    if at == 0 {
        size
    } else {
        at
    }
}

/// A stream's place in one followed task's log.
#[derive(Debug)]
struct Follow {
    name: String,
    attempt: i64,
    status: String,
    fence: i64,
    gen: i64,
    pos: u64,
    started: bool,
    ended: bool,
    /// This writing can no longer be read: the next one is waited for.
    gone: bool,
    moved: u32,
    /// How many times its first position could not be read.
    opens: u32,
}

/// Reading still to do.
#[derive(Debug, Clone)]
enum Job {
    /// A task's log from where the stream is to where the log is now; a line
    /// not yet finished waits for the relay to bring it.
    CatchUp(String),
    /// A task's log from where the stream is to a byte the relay sent from
    /// (or ended at).
    Fill(String, u64),
    /// The run's tasks heard again: after a lag, the statuses the passes
    /// missed.
    Statuses,
}

/// One stream: what it follows, where it is in each, and what is still to
/// send.
struct Sub {
    state: AppState,
    run: String,
    one: Option<String>,
    tasks: Vec<String>,
    statuses: Vec<String>,
    cursors: HashMap<String, Cursor>,
    begin: Begin,
    tail: Option<usize>,
    filter: Option<LogFilter>,
    joined: Option<Joined>,
    /// Every task of the run's status, as last heard.
    status_of: HashMap<String, String>,
    /// Tasks `status=` selected once.
    chosen: HashSet<String>,
    follows: HashMap<String, Follow>,
    /// Tasks whose first position is still to be found, in the run's order.
    pending_open: Vec<LogTail>,
    jobs: VecDeque<Job>,
    current: Option<(Arc<Pass>, usize)>,
    out: VecDeque<Event>,
    deadline: Option<tokio::time::Instant>,
    done: bool,
}

impl Sub {
    fn new(state: AppState, run: String, one: Option<String>, ask: Ask, auth: &AuthUser) -> Sub {
        // The stream ends with its session, and after the relay's `life`
        // whatever its session (an API token's lasts as long as the token,
        // and is not asked of again): it resumes from its cursors as the
        // caller, or not.
        let max_life = state.relay.settings.life;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        let left = Duration::from_secs((auth.0.exp as u64).saturating_sub(now));
        let life = match auth.0.exp {
            0 => max_life,
            _ => left.min(max_life),
        };
        let deadline = tokio::time::Instant::now().checked_add(life);
        Sub {
            state,
            run,
            one,
            tasks: ask.tasks,
            statuses: ask.statuses,
            cursors: ask.cursors,
            begin: ask.begin,
            tail: ask.tail,
            filter: ask.filter,
            joined: None,
            status_of: HashMap::new(),
            chosen: HashSet::new(),
            follows: HashMap::new(),
            pending_open: Vec::new(),
            jobs: VecDeque::new(),
            current: None,
            out: VecDeque::new(),
            deadline,
            done: false,
        }
    }

    /// What the relay is to read of the run for this stream.
    fn keys(&self) -> Option<Vec<String>> {
        (self.statuses.is_empty() && !self.tasks.is_empty()).then(|| self.tasks.clone())
    }

    fn in_scope(&self, row: &LogTail) -> bool {
        self.tasks.is_empty()
            || self
                .tasks
                .iter()
                .any(|t| *t == row.task_id || t.eq_ignore_ascii_case(&row.name))
    }

    /// A task's row heard of: its status kept, and the task followed where
    /// it is in scope and (with `status=`) its status has matched once.
    /// Whether it is followed from now.
    fn note(&mut self, row: &LogTail) -> bool {
        self.status_of
            .insert(row.task_id.clone(), row.status.clone());
        if let Some(f) = self.follows.get_mut(&row.task_id) {
            f.status = row.status.clone();
            f.attempt = row.attempt;
            f.name = row.name.clone();
            return false;
        }
        if !self.in_scope(row) {
            return false;
        }
        if !self.statuses.is_empty() {
            if !self
                .statuses
                .iter()
                .any(|s| s.eq_ignore_ascii_case(&row.status))
            {
                return false;
            }
            self.chosen.insert(row.task_id.clone());
        }
        self.follows.insert(
            row.task_id.clone(),
            Follow {
                name: row.name.clone(),
                attempt: row.attempt,
                status: row.status.clone(),
                fence: row.fence,
                gen: row.gen,
                pos: 0,
                started: false,
                ended: false,
                gone: false,
                moved: 0,
                opens: 0,
            },
        );
        true
    }

    /// Whether every task the stream waits for is over: every one it
    /// follows, or, with `status=`, every task of the run.
    fn over(&self) -> bool {
        if self.statuses.is_empty() {
            self.follows.values().all(|f| is_terminal(&f.status))
        } else {
            self.status_of.values().all(|s| is_terminal(s))
        }
    }

    /// Whether everything followed is over and sent.
    fn finished(&self) -> bool {
        self.pending_open.is_empty()
            && self.over()
            && self.follows.values().all(|f| f.ended || f.gone)
    }

    fn events(self) -> impl futures::Stream<Item = Result<Event, Infallible>> {
        futures::stream::unfold(self, |mut sub| async move {
            loop {
                if let Some(ev) = sub.out.pop_front() {
                    return Some((Ok(ev), sub));
                }
                if sub.done {
                    return None;
                }
                if !sub.pending_open.is_empty() {
                    let row = sub.pending_open.remove(0);
                    sub.begin_task(&row).await;
                    continue;
                }
                if let Some(job) = sub.jobs.pop_front() {
                    if sub.step(&job).await {
                        sub.jobs.push_front(job);
                    }
                    continue;
                }
                if let Some((pass, i)) = sub.current.take() {
                    if i < pass.pieces.len() {
                        let piece = pass.pieces[i].clone();
                        if sub.apply(&piece).await {
                            sub.current = Some((pass, i + 1));
                        } else {
                            // Read first what the piece begins after; then the
                            // piece again.
                            sub.current = Some((pass, i));
                        }
                    }
                    continue;
                }
                if sub.finished() {
                    sub.out.push_back(
                        Event::default()
                            .event("eof")
                            .json_data(json!({ "run_id": sub.run }))
                            .unwrap_or_else(|_| Event::default().event("eof").data("{}")),
                    );
                    sub.done = true;
                    continue;
                }
                if !sub.wait().await {
                    return None;
                }
            }
        })
    }

    /// Wait for the relay's next pass. `false`: the stream is to end (the
    /// relay stopped, its session is over, or it could not watch the run).
    async fn wait(&mut self) -> bool {
        if self.joined.is_none() {
            match self.state.relay.join(&self.state, &self.run, self.keys()) {
                Ok(joined) => {
                    self.joined = Some(joined);
                    self.catch_up_all();
                    return true;
                }
                Err(_) => return false,
            }
        }
        let relay = Arc::clone(&self.state.relay);
        let deadline = self.deadline;
        let joined = self.joined.as_mut().expect("joined above");
        let got = tokio::select! {
            biased;
            _ = relay.stopped() => return false,
            _ = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending::<()>().await,
                }
            } => return false,
            got = joined.rx.recv() => got,
        };
        match got {
            Ok(pass) => self.current = Some((pass, 0)),
            Err(RecvError::Lagged(_)) => {
                joined.rx = joined.rx.resubscribe();
                self.catch_up_all();
            }
            Err(RecvError::Closed) => {
                self.joined = None;
            }
        }
        true
    }

    /// After a lag, or a channel closed: the run's statuses heard again, and
    /// every task followed read from where the stream is, those whose
    /// writing ended or was gone too: the passes missed can have held
    /// another attempt of one, begun and over.
    fn catch_up_all(&mut self) {
        self.jobs.push_back(Job::Statuses);
        let mut tasks: Vec<String> = self
            .follows
            .iter()
            .filter(|(_, f)| f.started)
            .map(|(t, _)| t.clone())
            .collect();
        tasks.sort();
        self.jobs.extend(tasks.into_iter().map(Job::CatchUp));
    }

    fn data(&self, task: &str, extra: Value) -> Value {
        let f = &self.follows[task];
        let mut v = json!({
            "task_id": task,
            "task": f.name,
            "attempt": f.attempt,
            "cursor": cursor_of(task, f.fence, f.gen, f.pos),
        });
        if let (Value::Object(m), Value::Object(e)) = (&mut v, extra) {
            m.extend(e);
        }
        v
    }

    fn push(&mut self, kind: &str, data: Value) {
        let mut ev = Event::default()
            .event(kind)
            .json_data(&data)
            .unwrap_or_else(|_| Event::default().event(kind).data("{}"));
        if self.one.is_some() {
            if let Some(c) = data.get("cursor").and_then(Value::as_str) {
                ev = ev.id(c);
            }
        }
        self.out.push_back(ev);
    }

    /// A task's first position, and its `start`.
    async fn begin_task(&mut self, row: &LogTail) {
        let why;
        let pos;
        let mut row = row.clone();
        if self.follows.get(&row.task_id).is_some_and(|f| f.opens > 0) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            match db::log_tail_of(&self.state.read_pool, &row.task_id).await {
                Ok(Some(again)) => row = again,
                Ok(None) => return self.begin_failed(row),
                Err(e) => {
                    tracing::warn!(task_id = %row.task_id, error = %e, "a log stream could not read a task");
                    return self.begin_failed(row);
                }
            }
        }
        let layout = match Layout::of(self.state.log_store.as_deref(), &row).await {
            Ok(Some(layout)) => layout,
            // Its open part moved under the statement: read again.
            Ok(None) => return self.begin_failed(row),
            Err(e) => {
                tracing::warn!(task_id = %row.task_id, error = %format!("{e:#}"), "a log stream could not read a task's log");
                return self.begin_failed(row);
            }
        };
        let row = &row;
        let total = layout.total();
        match self.cursors.get(&row.task_id).copied() {
            Some(c) if (c.fence, c.gen) == (row.fence, row.gen) => {
                why = "open";
                pos = self.boundary(row, &layout, c.pos.min(total)).await;
            }
            Some(c) if c.fence == row.fence => {
                why = "rewritten";
                pos = self.first(row, &layout, false).await;
            }
            Some(_) => {
                why = "attempt";
                pos = 0;
            }
            None => {
                why = "open";
                pos = self.first(row, &layout, true).await;
            }
        }
        if let Some(f) = self.follows.get_mut(&row.task_id) {
            f.fence = row.fence;
            f.gen = row.gen;
            f.pos = pos;
            f.started = true;
            f.ended = false;
            f.gone = false;
        }
        let data = self.data(
            &row.task_id,
            json!({ "from": pos, "size": total, "why": why }),
        );
        self.push("start", data);
        self.jobs.push_back(Job::CatchUp(row.task_id.clone()));
    }

    /// A task whose first position could not be read: asked for again, a
    /// few times, and then let go until its next writing (it no longer holds
    /// the stream's end back).
    fn begin_failed(&mut self, row: LogTail) {
        let Some(f) = self.follows.get_mut(&row.task_id) else {
            return;
        };
        f.opens += 1;
        if f.opens < MOVED {
            self.pending_open.push(row);
        } else {
            tracing::warn!(task_id = %row.task_id, "a log stream lets a task go: its log could not be read");
            f.started = true;
            f.gone = true;
        }
    }

    /// Where a task with no cursor begins: its first byte, where it is now,
    /// or its window's first line (the last `tail` lines of it that match).
    /// `asked`: the stream's own `from`; a writing begun again begins at its
    /// window but for `from=start`.
    async fn first(&mut self, row: &LogTail, layout: &Layout, asked: bool) -> u64 {
        let total = layout.total();
        match self.begin {
            Begin::Start => return 0,
            Begin::End if asked => return total,
            _ => {}
        }
        let window = self.state.relay.settings.window;
        if total <= window && (self.tail.is_none() || !asked) {
            return 0;
        }
        let from = total.saturating_sub(window).saturating_sub(1);
        let bytes = match self.read(row, layout, from, (total - from) as usize).await {
            Some(bytes) => bytes,
            None => return total.saturating_sub(window),
        };
        // The first line that begins inside the window: after the first
        // line end in it, or at its start if a line ends just before it.
        let mut start = if from == 0 && total <= window {
            0
        } else {
            match bytes.iter().position(|b| *b == b'\n') {
                Some(at) => at + 1,
                None => {
                    let mut at = 1.min(bytes.len());
                    while at < bytes.len() && bytes[at] & 0xC0 == 0x80 {
                        at += 1;
                    }
                    at
                }
            }
        };
        if let (Some(n), true) = (self.tail, asked) {
            // The last n lines (that match) of the window.
            let text = &bytes[start..];
            let mut starts: Vec<usize> = Vec::new();
            let mut at = 0;
            for line in text.split(|b| *b == b'\n') {
                let line_start = at;
                at += line.len() + 1;
                if line.is_empty() && line_start >= text.len() {
                    break;
                }
                let keep = match &self.filter {
                    None => true,
                    Some(f) => f.keep_line(&String::from_utf8_lossy(line)).is_some(),
                };
                if keep {
                    starts.push(line_start);
                }
            }
            if starts.len() > n {
                start += starts[starts.len() - n];
            } else if let Some(first) = starts.first() {
                start += first;
            }
        }
        from + start as u64
    }

    /// `pos` moved on to a character's start.
    async fn boundary(&mut self, row: &LogTail, layout: &Layout, pos: u64) -> u64 {
        if pos == 0 || pos >= layout.total() {
            return pos;
        }
        match self.read(row, layout, pos, 4).await {
            Some(bytes) => pos + bytes.iter().take_while(|b| **b & 0xC0 == 0x80).count() as u64,
            None => pos,
        }
    }

    /// At most `max` bytes of a task's log from `from`, read under the
    /// relay's read permits; `None` where they could not be read.
    async fn read(&self, row: &LogTail, layout: &Layout, from: u64, max: usize) -> Option<Vec<u8>> {
        let _permit = Arc::clone(&self.state.relay.reads)
            .acquire_owned()
            .await
            .ok()?;
        match layout
            .read(
                &self.state.read_pool,
                self.state.log_store.as_deref(),
                row,
                from,
                max,
            )
            .await
        {
            Ok(Got::Bytes(bytes)) => Some(bytes),
            _ => None,
        }
    }

    /// One step of a job: whether there is more of it.
    async fn step(&mut self, job: &Job) -> bool {
        let (task, to) = match job {
            Job::CatchUp(task) => (task.as_str(), None),
            Job::Fill(task, to) => (task.as_str(), Some(*to)),
            Job::Statuses => {
                let rows =
                    match db::log_tails(&self.state.read_pool, std::slice::from_ref(&self.run))
                        .await
                    {
                        Ok(rows) => rows,
                        Err(e) => {
                            tracing::warn!(error = %e, "a log stream could not read its run");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            return true;
                        }
                    };
                for row in rows {
                    let begun = self.note(&row)
                        || self.follows.get(&row.task_id).is_some_and(|f| !f.started)
                            && !self.pending_open.iter().any(|r| r.task_id == row.task_id);
                    if begun {
                        self.pending_open.push(row);
                    }
                }
                return false;
            }
        };
        if !self.follows.contains_key(task) {
            return false;
        }
        let permit = Arc::clone(&self.state.relay.reads).acquire_owned().await;
        let row = match db::log_tail_of(&self.state.read_pool, task).await {
            Ok(Some(row)) => row,
            Ok(None) => {
                // The task's row is gone (its run deleted, an old fan-out's
                // instance): nothing more of it.
                if let Some(f) = self.follows.get_mut(task) {
                    f.gone = true;
                    f.status = "cancelled".into();
                }
                return false;
            }
            Err(e) => {
                drop(permit);
                tracing::warn!(task_id = %task, error = %e, "a log stream could not read a task");
                return self.moved(task).await;
            }
        };
        drop(permit);
        self.note(&row);
        let (mine, ended, gone) = {
            let f = &self.follows[task];
            ((f.fence, f.gen), f.ended, f.gone)
        };
        // A later writing first: one begun (and perhaps over) while this
        // stream was not looking is begun here, whatever the one before was.
        if (row.fence, row.gen) > mine {
            self.another_writing(&row).await;
            return false;
        }
        if (row.fence, row.gen) < mine || gone || (ended && to.is_none()) {
            return false;
        }
        let layout = match Layout::of(self.state.log_store.as_deref(), &row).await {
            Ok(Some(layout)) => layout,
            Ok(None) => return self.moved(task).await,
            Err(e) => {
                tracing::warn!(task_id = %task, error = %format!("{e:#}"), "a log stream could not read a task's log");
                self.follows.get_mut(task).expect("followed").gone = true;
                return false;
            }
        };
        let total = layout.total();
        let over = writing_over(&row);
        let (pos, ended) = {
            let f = &self.follows[task];
            (f.pos, f.ended)
        };
        let target = to.unwrap_or(total);
        if pos >= target {
            if to.is_none() && over && !ended {
                self.end(task, true);
            }
            return false;
        }
        let step = self.state.relay.settings.step;
        let max = ((target - pos) as usize).min(step * CATCH_UP_STEPS);
        let got = {
            let _permit = Arc::clone(&self.state.relay.reads).acquire_owned().await;
            layout
                .read(
                    &self.state.read_pool,
                    self.state.log_store.as_deref(),
                    &row,
                    pos,
                    max,
                )
                .await
        };
        match got {
            // Nothing where the relay said there was: the log moved on.
            Ok(Got::Bytes(bytes)) if bytes.is_empty() => self.moved(task).await,
            Ok(Got::Bytes(bytes)) => {
                let reaches = pos + bytes.len() as u64 >= target;
                // A line not yet ended waits, but for a long one.
                let force = to.is_some()
                    || (over && reaches)
                    || bytes.len() - cut(&bytes, false) >= self.state.relay.settings.line_max;
                let n = match cut(&bytes, force) {
                    // A fill to a byte inside a character (no position the
                    // relay sends is one, but a read is not trusted to say
                    // so): the bytes as they are, and the fill is done.
                    0 if to.is_some() => bytes.len(),
                    n => n,
                };
                if n == 0 {
                    return false;
                }
                self.follows.get_mut(task).expect("followed").moved = 0;
                let mut rest = &bytes[..n];
                while !rest.is_empty() {
                    let take = frame_len(rest, step);
                    self.text(task, &rest[..take]);
                    rest = &rest[take..];
                }
                let f = &self.follows[task];
                f.pos < target || (to.is_none() && over && !f.ended)
            }
            Ok(Got::Gone) => {
                self.follows.get_mut(task).expect("followed").gone = true;
                false
            }
            Ok(Got::Moved) => self.moved(task).await,
            Err(e) => {
                tracing::warn!(task_id = %task, error = %format!("{e:#}"), "a log stream could not read a task's log");
                self.follows.get_mut(task).expect("followed").gone = true;
                false
            }
        }
    }

    /// A read found the log moved on under it: again, a few times, and then
    /// the writing is let go.
    async fn moved(&mut self, task: &str) -> bool {
        let f = self.follows.get_mut(task).expect("followed");
        f.moved += 1;
        if f.moved < MOVED {
            tokio::time::sleep(Duration::from_millis(50)).await;
            return true;
        }
        tracing::warn!(task_id = %task, "a log stream lets a writing of a task's log go: it moved on under every read");
        f.moved = 0;
        f.gone = true;
        false
    }

    /// Send `bytes` of a task's log, from where the stream is.
    fn text(&mut self, task: &str, bytes: &[u8]) {
        let text = String::from_utf8_lossy(bytes).into_owned();
        self.follows.get_mut(task).expect("followed").pos += bytes.len() as u64;
        let extra = match &self.filter {
            None => json!({ "text": text }),
            Some(filter) => {
                let lines: Vec<Value> = text
                    .split_inclusive('\n')
                    .filter_map(|line| {
                        let line = line.strip_suffix('\n').unwrap_or(line);
                        filter.keep_line(line).map(|(level, ts, msg)| match ts {
                            Some(ts) => json!({ "level": level.as_str(), "ts": ts, "text": msg }),
                            None => json!({ "level": level.as_str(), "text": msg }),
                        })
                    })
                    .collect();
                json!({ "lines": lines })
            }
        };
        let data = self.data(task, extra);
        self.push("log", data);
    }

    fn end(&mut self, task: &str, complete: bool) {
        let f = self.follows.get_mut(task).expect("followed");
        if f.ended {
            return;
        }
        f.ended = true;
        let status = f.status.clone();
        let data = self.data(task, json!({ "status": status, "complete": complete }));
        self.push("end", data);
    }

    /// The task's log is a later writing than the stream's: another attempt
    /// (from its start), or the log written again (from its window, or its
    /// start for `from=start`), or, where nothing of the one before was sent,
    /// the same as it, said nothing of.
    async fn another_writing(&mut self, row: &LogTail) {
        let (fence, pos0, started, ended) = {
            let f = &self.follows[&row.task_id];
            (f.fence, f.pos, f.started, f.ended)
        };
        let quiet = row.fence == fence && pos0 == 0 && started;
        let why = if row.fence != fence {
            "attempt"
        } else {
            "rewritten"
        };
        // The attempt before, of which this stream sent something and not
        // its end: over, and not all of it could be read.
        if why == "attempt" && started && !ended && pos0 > 0 {
            self.end(&row.task_id, false);
        }
        let layout = Layout::of(self.state.log_store.as_deref(), row)
            .await
            .ok()
            .flatten();
        let total = layout.map(|l| l.total());
        let pos = match (&layout, quiet || why == "attempt") {
            (Some(layout), false) => self.first(row, layout, false).await,
            _ => 0,
        };
        let f = self.follows.get_mut(&row.task_id).expect("followed");
        f.fence = row.fence;
        f.gen = row.gen;
        f.pos = pos;
        f.ended = false;
        f.gone = false;
        f.moved = 0;
        f.started = true;
        if !quiet {
            let size = total.unwrap_or(0);
            let data = self.data(
                &row.task_id,
                json!({ "from": pos, "size": size, "why": why }),
            );
            self.push("start", data);
        }
        self.jobs.push_back(Job::CatchUp(row.task_id.clone()));
    }

    /// One piece of a pass. `false`: read first what it begins after, then
    /// it again.
    async fn apply(&mut self, piece: &Piece) -> bool {
        let task = piece.task_id.as_ref();
        if let What::Status = piece.what {
            let row = LogTail {
                run_id: self.run.clone(),
                task_id: task.to_string(),
                name: piece.name.to_string(),
                status: piece.status.to_string(),
                attempt: piece.attempt,
                fence: piece.fence,
                gen: piece.gen,
                segments: 0,
                closed: 0,
                row_bytes: None,
                output_bytes: 0,
            };
            if self.note(&row) {
                // Selected from now (status=): from its window, as at a
                // stream's start.
                let row = db::log_tail_of(&self.state.read_pool, task)
                    .await
                    .ok()
                    .flatten();
                match row {
                    Some(row) => self.pending_open.push(row),
                    None => self.jobs.push_back(Job::Statuses),
                }
            }
            return true;
        }
        let Some(f) = self.follows.get(task) else {
            return true;
        };
        if !f.started {
            return true;
        }
        let mine = (f.fence, f.gen);
        let theirs = (piece.fence, piece.gen);

        if theirs < mine {
            return true;
        }
        if theirs > mine {
            let row = db::log_tail_of(&self.state.read_pool, task)
                .await
                .ok()
                .flatten();
            match row {
                Some(row) if (row.fence, row.gen) >= theirs => self.another_writing(&row).await,
                _ => return true,
            }
            // The piece is of the writing the stream is on now: as any.
            let f = &self.follows[task];
            if (f.fence, f.gen) != theirs {
                return true;
            }
        }
        let (gone, pos) = {
            let f = &self.follows[task];
            (f.gone, f.pos)
        };
        if gone {
            if let What::Ended { .. } = piece.what {
                self.end(task, false);
            }
            return true;
        }
        match &piece.what {
            What::Status => true,
            // The relay follows the writing from `end` on: what is before it
            // is this stream's to read.
            What::Began { end } => {
                if *end > pos {
                    self.jobs.push_back(Job::Fill(task.to_string(), *end));
                }
                true
            }
            What::Text { start, end, text } => {
                if *end <= pos {
                    return true;
                }
                if *start > pos {
                    self.jobs.push_back(Job::Fill(task.to_string(), *start));
                    return false;
                }
                let skip = (pos - start) as usize;
                if text.len() as u64 != end - start || !text.is_char_boundary(skip) {
                    // Not the bytes as they are (no text is written so): read
                    // them.
                    self.jobs.push_back(Job::Fill(task.to_string(), *end));
                    return true;
                }
                let text = Arc::clone(text);
                self.text(task, &text.as_bytes()[skip..]);
                true
            }
            What::Grew { end } => {
                if *end > pos {
                    self.jobs.push_back(Job::Fill(task.to_string(), *end));
                }
                true
            }
            What::Ended { end, complete } => {
                if *end > pos {
                    self.jobs.push_back(Job::Fill(task.to_string(), *end));
                    return false;
                }
                if let Some(f) = self.follows.get_mut(task) {
                    f.status = piece.status.to_string();
                }
                self.end(task, *complete);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_is_its_task_attempt_writing_and_byte() {
        let (task, c) = parse_cursor("0b8e-1:7:2:4096").unwrap();
        assert_eq!(
            (task.as_str(), c),
            (
                "0b8e-1",
                Cursor {
                    fence: 7,
                    gen: 2,
                    pos: 4096
                }
            )
        );
        assert_eq!(cursor_of("0b8e-1", 7, 2, 4096), "0b8e-1:7:2:4096");
        for bad in ["", "t:1:2", "t:1:2:x", ":1:2:3", "t:-:2:3"] {
            assert!(parse_cursor(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_stream_is_asked_what_a_stream_can_do() {
        let ask = parse(Some(
            "task=a,b&status=failed&from=start&cursor=t:1:0:5,u:2:1:0&q=err".into(),
        ))
        .unwrap();
        assert_eq!(ask.tasks, ["a", "b"]);
        assert_eq!(ask.statuses, ["failed"]);
        assert_eq!(ask.begin, Begin::Start);
        assert_eq!(ask.cursors.len(), 2);
        assert!(ask.filter.is_some());
        assert_eq!(
            parse(Some("tail=1&limit=20".into())).unwrap().tail,
            Some(20)
        );
        for bad in [
            "context=2",
            "limit=5",
            "offset=10",
            "view=errors",
            "from=middle",
            "cursor=nope",
            "regex=%5B",
        ] {
            assert!(parse(Some(bad.into())).is_err(), "{bad}");
        }
        assert!(parse(None).unwrap().filter.is_none(), "no filter is none");
    }
}
