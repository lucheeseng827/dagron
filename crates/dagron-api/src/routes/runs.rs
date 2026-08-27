//! Run list + detail read endpoints (read pool, all parameterized, auth-gated).

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::state::AppState;

/// Default page size for `GET /api/runs`, capped at MAX_LIMIT.
const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 500;

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct RunSummary {
    pub id: String,
    pub definition_id: String,
    pub status: String,
    pub created_at: String,
    pub finished_at: Option<String>,
    /// Workflow/DAG name from the run's definition (LEFT JOIN; None if the
    /// definition row is missing). Surfaced as the "Workflow" column in the UI.
    pub name: Option<String>,
    /// What started this run — derived, no schema change: a
    /// `schedule_backfills` row claims it as a backfill, a stamped
    /// `schedule_id` marks a cron fire, anything else was human-initiated.
    pub trigger_kind: String,
    /// What a human decided about this run: acknowledged / resolved / ignored,
    /// or absent for one nobody has looked at. `status` is what the engine did;
    /// this is what was done about it, and it is the difference between an
    /// attention queue that drains and one that only ages out.
    pub triage_state: Option<String>,
    pub triage_note: Option<String>,
    pub triaged_at: Option<String>,
    pub triaged_by: Option<String>,
}

/// SELECT list + joins shared by the run list and per-workflow runs: summary
/// columns, the definition name, and the derived trigger kind.
pub(crate) const SUMMARY_SELECT: &str = "SELECT wr.id, wr.definition_id, wr.status, wr.created_at, wr.finished_at, d.name,
        wr.triage_state, wr.triage_note, wr.triaged_at, wr.triaged_by,
        CASE WHEN sb.run_id IS NOT NULL THEN 'backfill'
             WHEN wr.schedule_id IS NOT NULL THEN 'schedule'
             ELSE 'manual' END AS trigger_kind
 FROM workflow_runs wr
 LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
 LEFT JOIN schedule_backfills sb ON sb.run_id = wr.id";

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct TaskRow {
    pub id: String,
    pub name: String,
    pub status: String,
    pub attempt: i64,
    pub output: Option<String>,
    pub scheduled_at: Option<String>,
    pub finished_at: Option<String>,
    /// Why a parked task is parked. Each park form keeps the row `running` with
    /// a NULL lease, so without these the console cannot tell "waiting on
    /// something" from "hung": the time sensor's deadline, the HTTP sensor's
    /// endpoint, the dataset sensor's URI, and the sub-workflow trigger's child
    /// run id (which also gives the console a parent → child link).
    pub wake_at: Option<String>,
    pub wait_url: Option<String>,
    pub wait_dataset: Option<String>,
    pub sub_run_id: Option<String>,
    /// Scheduling metadata worth seeing next to a task: the named pool it drew a
    /// slot from (#21), its dispatch priority (#25), and whether it was resolved
    /// from the memoization store instead of executed (#22) — the last of which
    /// explains an otherwise inexplicable 0s success.
    pub pool: Option<String>,
    pub priority: i64,
    pub cache_hit: bool,
}

/// Why a run failed, in the run detail itself (G-AG6).
///
/// Without this, an agent whose run failed holds a status and a list of task
/// rows and must work out which one broke and then fetch its logs — two more
/// calls and a correlation step, from a model that has nine tools to choose
/// between. The reason belongs in the response it already has.
///
/// Present whenever this run has a failed task, or the run itself failed. That
/// is deliberately not the same as `status == "failed"`: a run whose fail-fast
/// is off can be `running` with a task already dead, and the earlier an agent
/// learns that, the less it wastes.
#[derive(Debug, Serialize, PartialEq)]
pub struct RunFailure {
    /// The failed task this summarises, when there is one. Absent when the run
    /// failed with no task failing — the `run_timeout_secs` sweep fails the run
    /// and *cancels* its tasks, so nothing is left in a `failed` state and the
    /// reason lives on the run row instead.
    pub task_id: Option<String>,
    pub task_name: Option<String>,
    pub attempt: Option<i64>,
    /// How many tasks in this run are `failed`. One failure summarised out of
    /// several is honest only if the caller can see there were several.
    pub failed_tasks: usize,
    /// The tail of the recorded failure text: the task's output, or the run's
    /// own when no task failed. dagron stores one output stream per task, not a
    /// split stdout/stderr, so this is that stream — and for a failed task it
    /// is the error the executor recorded.
    pub message: Option<String>,
    /// Whether `message` is a tail of something longer. A model that cannot
    /// tell a complete error from a clipped one will confidently explain the
    /// wrong half, so say which this is rather than leaving it to be guessed.
    pub truncated: bool,
}

/// The task rows behind a run, used by both `GET /api/runs/{id}` and the
/// failure summary on `GET /api/runs/{id}/wait`. Shared so the two cannot drift:
/// a column added for one and missed by the other would silently change what a
/// failure summary can see depending on which route asked for it.
const RUN_TASK_ROWS_SQL: &str = "SELECT id, name, status, attempt, output, scheduled_at, finished_at,
        wake_at, wait_url, wait_dataset, sub_run_id,
        pool, priority, cache_hit
 FROM task_runs WHERE run_id = $1 ORDER BY name";

/// How much failure text to inline. Enough for a stack trace's business end,
/// bounded so a task that logged a megabyte before dying cannot bloat every
/// poll of the run detail — `GET /api/runs/{id}/logs` remains the full view.
const FAILURE_TAIL_LINES: usize = 20;
const FAILURE_TAIL_BYTES: usize = 4096;

/// Last `max_lines` lines of `text`, further clipped to `max_bytes` from the
/// end. Returns the tail and whether anything was dropped.
fn failure_tail(text: &str, max_lines: usize, max_bytes: usize) -> (String, bool) {
    let trimmed = text.trim_end();
    let lines: Vec<&str> = trimmed.lines().collect();
    let start = lines.len().saturating_sub(max_lines);
    let mut truncated = start > 0;
    let mut out = lines[start..].join("\n");

    if out.len() > max_bytes {
        // Keep the end, not the start: the last thing a dying task said is the
        // part that explains it. Step forward to a char boundary so a clipped
        // multi-byte character cannot panic the slice.
        let mut cut = out.len() - max_bytes;
        while cut < out.len() && !out.is_char_boundary(cut) {
            cut += 1;
        }
        out = out[cut..].to_string();
        truncated = true;
    }
    (out, truncated)
}

/// Build the failure summary from rows already in hand — no extra query, and
/// pure, so the choice of *which* failure to report is testable without a
/// database.
fn summarize_failure(status: &str, run_output: Option<&str>, tasks: &[TaskRow]) -> Option<RunFailure> {
    // Earliest-finished failed task. With fail-fast off several tasks fail
    // independently, and the one that finished first is the likeliest cause
    // rather than a downstream casualty. `finished_at` is RFC3339 UTC
    // everywhere it is written, so the lexicographic minimum is the
    // chronological one; a tie falls back to name, which is the row order.
    let mut culprit: Option<&TaskRow> = None;
    let mut failed_tasks = 0usize;
    for t in tasks.iter().filter(|t| t.status == "failed") {
        failed_tasks += 1;
        let better = match culprit {
            None => true,
            Some(c) => {
                let k = |t: &TaskRow| {
                    (
                        t.finished_at.is_none(),
                        t.finished_at.clone().unwrap_or_default(),
                        t.name.clone(),
                    )
                };
                k(t) < k(c)
            }
        };
        if better {
            culprit = Some(t);
        }
    }

    // No failed task. Only the run's own failure is left to report, and only
    // if it actually failed — a cancelled or succeeded run has no failure, and
    // saying otherwise would make the field mean "look here" on every poll.
    let (task, text) = match culprit {
        Some(t) => (Some(t), t.output.as_deref()),
        None if status == "failed" => (None, run_output),
        None => return None,
    };

    let (message, truncated) = text
        .map(|t| failure_tail(t, FAILURE_TAIL_LINES, FAILURE_TAIL_BYTES))
        .unwrap_or_default();

    Some(RunFailure {
        task_id: task.map(|t| t.id.clone()),
        task_name: task.map(|t| t.name.clone()),
        attempt: task.map(|t| t.attempt),
        failed_tasks,
        message: if message.is_empty() { None } else { Some(message) },
        truncated,
    })
}

#[derive(Debug, Serialize)]
pub struct RunDetail {
    pub id: String,
    pub definition_id: String,
    pub status: String,
    pub input: Option<String>,
    pub output: Option<String>,
    pub created_at: String,
    pub finished_at: Option<String>,
    /// Workflow/DAG name from the run's definition (for the header + backlink).
    pub name: Option<String>,
    /// Derived trigger kind: manual | schedule | backfill.
    pub trigger_kind: String,
    /// What a human decided about this run. `status` is what the engine did;
    /// this is what was done about it.
    pub triage_state: Option<String>,
    pub triage_note: Option<String>,
    pub triaged_at: Option<String>,
    pub triaged_by: Option<String>,
    /// Why this run failed, when it did (G-AG6) — `null` otherwise, so a
    /// caller can branch on presence without inspecting `status`.
    pub failure: Option<RunFailure>,
    pub tasks: Vec<TaskRow>,
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    pub status: Option<String>,
    /// Filter to one workflow's runs (definition name, exact match).
    pub name: Option<String>,
    /// Filter by trigger kind: manual | schedule | backfill.
    pub trigger: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// `GET /api/runs?status=&name=&trigger=&limit=&offset=` — most-recent-first
/// run list with optional status / workflow-name / trigger filters.
pub async fn list_runs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<RunSummary>>, StatusCode> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = params.offset.unwrap_or(0).max(0);

    // Bind each filter as optional: when None, the `$n IS NULL` branch keeps
    // every row, so one parameterized query serves all filter combinations.
    let rows = sqlx::query_as::<_, RunSummary>(&format!(
        "{SUMMARY_SELECT}
         WHERE ($1::text IS NULL OR wr.status = $1)
           AND ($2::text IS NULL OR d.name = $2)
           AND ($3::text IS NULL OR
                CASE WHEN sb.run_id IS NOT NULL THEN 'backfill'
                     WHEN wr.schedule_id IS NOT NULL THEN 'schedule'
                     ELSE 'manual' END = $3)
         ORDER BY wr.created_at DESC
         LIMIT $4 OFFSET $5"
    ))
    .bind(&params.status)
    .bind(&params.name)
    .bind(&params.trigger)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    Ok(Json(rows))
}

/// `GET /api/runs/:id` — one run plus its task rows. 404 if the run is unknown.
pub async fn get_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunDetail>, StatusCode> {
    let run = sqlx::query_as::<_, RunSummaryFull>(
        "SELECT wr.id, wr.definition_id, wr.status, wr.input, wr.output,
                wr.created_at, wr.finished_at, d.name,
                wr.triage_state, wr.triage_note, wr.triaged_at, wr.triaged_by,
                CASE WHEN sb.run_id IS NOT NULL THEN 'backfill'
                     WHEN wr.schedule_id IS NOT NULL THEN 'schedule'
                     ELSE 'manual' END AS trigger_kind
         FROM workflow_runs wr
         LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
         LEFT JOIN schedule_backfills sb ON sb.run_id = wr.id
         WHERE wr.id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?
    .ok_or(StatusCode::NOT_FOUND)?;

    let tasks = sqlx::query_as::<_, TaskRow>(RUN_TASK_ROWS_SQL)
        .bind(&id)
        .fetch_all(&state.read_pool)
        .await
        .map_err(internal)?;

    // Built from `tasks` and the run row, both already fetched — the summary
    // costs no additional round trip to the database.
    let failure = summarize_failure(&run.status, run.output.as_deref(), &tasks);

    Ok(Json(RunDetail {
        id: run.id,
        definition_id: run.definition_id,
        status: run.status,
        input: run.input,
        output: run.output,
        created_at: run.created_at,
        finished_at: run.finished_at,
        name: run.name,
        trigger_kind: run.trigger_kind,
        triage_state: run.triage_state,
        triage_note: run.triage_note,
        triaged_at: run.triaged_at,
        triaged_by: run.triaged_by,
        failure,
        tasks,
    }))
}

#[derive(Debug, Serialize)]
pub struct RunSpec {
    /// The stored, un-expanded DAG YAML this run was created from.
    pub yaml: String,
    /// The workflow/DAG name from the definition row (if the column is set).
    pub name: Option<String>,
}

/// `GET /api/runs/:id/spec` — the DAG spec (YAML) this run was created from, so
/// the UI can pre-fill a "re-run with changes" editor. Read-only; mirrors the
/// SELECT used by `resubmit_run`. 404 if the run (or its definition) is unknown.
pub async fn get_run_spec(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunSpec>, StatusCode> {
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT d.spec, d.name FROM workflow_runs r
         JOIN workflow_definitions d ON d.id = r.definition_id
         WHERE r.id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?;
    let (yaml, name) = row.ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(RunSpec { yaml, name }))
}

#[derive(sqlx::FromRow)]
struct RunSummaryFull {
    id: String,
    definition_id: String,
    status: String,
    input: Option<String>,
    output: Option<String>,
    created_at: String,
    finished_at: Option<String>,
    name: Option<String>,
    trigger_kind: String,
    triage_state: Option<String>,
    triage_note: Option<String>,
    triaged_at: Option<String>,
    triaged_by: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WaitParams {
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct RunResult {
    pub run_id: String,
    pub status: String,
    pub finished: bool,
    /// The `result_from` task's output on success, else null (fast-win #15).
    pub result: Option<String>,
    /// Why the run failed, in the same shape `GET /api/runs/{id}` returns
    /// (G-AG6). `null` unless this wait ended on a failure.
    ///
    /// This route is the one a synchronous caller actually uses — an agent
    /// blocking on a run holds *this* response, not the run detail. Putting the
    /// reason only on the detail route left the summary somewhere the caller
    /// that most needed it never looked: it got `status: "failed"` and had to
    /// start a second investigation with the tools it had.
    pub failure: Option<RunFailure>,
}

/// `GET /api/runs/:id/wait?timeout_secs=` — synchronous invocation (fast-win
/// #15): long-poll a run until it reaches a terminal state (or the timeout
/// elapses) and return its status + result (the `result_from` task's output on
/// success). 404 if the run is unknown; a timed-out wait returns `finished:false`
/// with the live status so the caller re-polls. Backend-agnostic short poll — the
/// engine (which may be a separate process) drives the run to completion.
pub async fn wait_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<WaitParams>,
) -> Result<Json<RunResult>, StatusCode> {
    let timeout =
        std::time::Duration::from_secs(params.timeout_secs.unwrap_or(30).clamp(1, 600));
    let deadline = tokio::time::Instant::now() + timeout;
    // Event-driven wait (LOW_LATENCY R-4): this process already holds a live
    // LISTEN-fed broadcast of task events keyed by run_id, so a waiter blocks
    // on that instead of the old 200 ms busy-poll (5 queries/second per waiter,
    // and up to 200 ms of avoidable tail latency on every sync invocation).
    // Subscribe BEFORE the first status read so a transition landing between
    // the read and the wait is never missed. A slow re-check timer stays as
    // the safety net for the listener's reconnect windows — the same
    // poll-as-backstop contract the engine's reconcile loop uses.
    let mut events = state.tx.subscribe();
    loop {
        let row: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT status, output FROM workflow_runs WHERE id = $1")
                .bind(&id)
                .fetch_optional(&state.read_pool)
                .await
                .map_err(internal)?;
        let Some((status, output)) = row else {
            return Err(StatusCode::NOT_FOUND);
        };
        let finished = matches!(status.as_str(), "succeeded" | "failed" | "cancelled");
        if finished || tokio::time::Instant::now() >= deadline {
            // The failure summary needs the task rows, so fetch them only when
            // there is a failure to explain. A succeeded run — the common case,
            // and the one that decides this endpoint's cost — pays nothing, and
            // neither does a timed-out poll of a still-running run.
            let failure = if status == "failed" {
                let tasks = sqlx::query_as::<_, TaskRow>(RUN_TASK_ROWS_SQL)
                    .bind(&id)
                    .fetch_all(&state.read_pool)
                    .await
                    .map_err(internal)?;
                summarize_failure(&status, output.as_deref(), &tasks)
            } else {
                None
            };
            // Per the contract, `result` is the run output only on success; a
            // failed/cancelled run's output is an error message, not a result.
            let result = if status == "succeeded" { output } else { None };
            return Ok(Json(RunResult { run_id: id, status, finished, result, failure }));
        }
        // Block until this run changes, the stream hiccups (lag/close — treat
        // as "recheck now"), the fallback tick fires, or the deadline lands.
        let recheck_at = std::cmp::min(
            deadline,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        );
        loop {
            tokio::select! {
                ev = events.recv() => match ev {
                    Ok(e) if e.run_id == id => break,
                    Ok(_) => continue, // some other run's event — keep waiting
                    Err(_) => break,   // lagged or sender gone — recheck via DB
                },
                _ = tokio::time::sleep_until(recheck_at) => break,
            }
        }
    }
}

/// Map any DB error to 500 without leaking internals to the client.
fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

#[cfg(test)]
mod failure_summary_tests {
    use super::*;

    /// A task row with only the fields the summary reads; the rest are the
    /// zero values, so a test says what it is about and nothing else.
    fn task(name: &str, status: &str, finished_at: Option<&str>, output: Option<&str>) -> TaskRow {
        TaskRow {
            id: format!("t-{name}"),
            name: name.to_string(),
            status: status.to_string(),
            attempt: 1,
            output: output.map(str::to_string),
            scheduled_at: None,
            finished_at: finished_at.map(str::to_string),
            wake_at: None,
            wait_url: None,
            wait_dataset: None,
            sub_run_id: None,
            pool: None,
            priority: 0,
            cache_hit: false,
        }
    }

    #[test]
    fn a_run_with_nothing_wrong_has_no_failure() {
        let tasks = [task("a", "succeeded", Some("2026-01-01T00:00:00Z"), Some("fine"))];
        assert_eq!(summarize_failure("succeeded", None, &tasks), None);
    }

    /// A cancelled run is not a failed one. Reporting a failure for it would
    /// make the field mean "something to look at" on every terminal run.
    #[test]
    fn a_cancelled_run_is_not_a_failure() {
        let tasks = [task("a", "cancelled", Some("2026-01-01T00:00:00Z"), None)];
        assert_eq!(summarize_failure("cancelled", Some("cancelled by operator"), &tasks), None);
    }

    #[test]
    fn the_failed_task_and_its_output_are_reported() {
        let tasks = [
            task("build", "succeeded", Some("2026-01-01T00:00:01Z"), Some("ok")),
            task("test", "failed", Some("2026-01-01T00:00:02Z"), Some("assertion failed: 2 != 3")),
        ];
        let f = summarize_failure("failed", None, &tasks).expect("a failed task is a failure");
        assert_eq!(f.task_name.as_deref(), Some("test"));
        assert_eq!(f.task_id.as_deref(), Some("t-test"));
        assert_eq!(f.failed_tasks, 1);
        assert_eq!(f.message.as_deref(), Some("assertion failed: 2 != 3"));
        assert!(!f.truncated);
    }

    /// With fail-fast off, several tasks fail independently. The one that
    /// finished first is the likeliest cause; the later ones are usually its
    /// casualties, and pointing an agent at a casualty sends it to fix the
    /// wrong task. The count still shows there were others.
    #[test]
    fn the_earliest_failure_is_the_one_reported() {
        let tasks = [
            task("downstream", "failed", Some("2026-01-01T00:00:09Z"), Some("upstream missing")),
            task("root", "failed", Some("2026-01-01T00:00:02Z"), Some("connection refused")),
        ];
        let f = summarize_failure("failed", None, &tasks).unwrap();
        assert_eq!(f.task_name.as_deref(), Some("root"));
        assert_eq!(f.failed_tasks, 2, "the other failure is still counted");
    }

    /// `cancel_overdue_runs` fails the *run* and cancels its tasks, so a
    /// deadline overrun leaves nothing in a `failed` state. This is precisely
    /// the case an agent would otherwise flail on: a failed run whose tasks all
    /// read "cancelled" and whose reason is nowhere in the task rows.
    #[test]
    fn a_run_that_failed_with_no_failed_task_falls_back_to_the_run_reason() {
        let tasks = [task("slow", "cancelled", Some("2026-01-01T00:00:09Z"), None)];
        let f = summarize_failure("failed", Some("run deadline exceeded (run_timeout_secs)"), &tasks)
            .expect("a failed run always explains itself");
        assert_eq!(f.task_id, None, "no task failed, so none is named");
        assert_eq!(f.failed_tasks, 0);
        assert_eq!(f.message.as_deref(), Some("run deadline exceeded (run_timeout_secs)"));
    }

    /// A failure with no recorded text is still a failure. `message: null`
    /// says "nothing was recorded"; omitting the summary would say "nothing
    /// went wrong", which is a different and wrong answer.
    #[test]
    fn a_failure_with_no_output_still_reports_the_task() {
        let tasks = [task("quiet", "failed", Some("2026-01-01T00:00:02Z"), None)];
        let f = summarize_failure("failed", None, &tasks).unwrap();
        assert_eq!(f.task_name.as_deref(), Some("quiet"));
        assert_eq!(f.message, None);
        assert!(!f.truncated);
    }

    /// A task with a failed row while the run is still `running` — fail-fast
    /// off. Surfacing it early is the whole point: an agent polling the run
    /// can stop waiting on work that is already doomed.
    #[test]
    fn a_still_running_run_reports_a_task_that_already_failed() {
        let tasks = [
            task("a", "failed", Some("2026-01-01T00:00:02Z"), Some("boom")),
            task("b", "running", None, None),
        ];
        let f = summarize_failure("running", None, &tasks).unwrap();
        assert_eq!(f.task_name.as_deref(), Some("a"));
    }

    #[test]
    fn the_tail_keeps_the_last_lines_and_says_it_clipped() {
        let text = (1..=50).map(|i| format!("line {i}")).collect::<Vec<_>>().join("\n");
        let (out, truncated) = failure_tail(&text, 20, 4096);
        assert!(truncated);
        assert!(out.starts_with("line 31"), "kept the last 20 lines, got: {out}");
        assert!(out.ends_with("line 50"), "the end is the part that explains it");
    }

    #[test]
    fn a_short_message_is_not_marked_truncated() {
        let (out, truncated) = failure_tail("one\ntwo\n", 20, 4096);
        assert_eq!(out, "one\ntwo");
        assert!(!truncated);
    }

    /// The byte clip slices a `String`, so it must land on a char boundary —
    /// a task that logs UTF-8 must not be able to panic the run detail.
    #[test]
    fn the_byte_clip_cannot_split_a_multibyte_character() {
        let text = "é".repeat(100); // 200 bytes, one line
        let (out, truncated) = failure_tail(&text, 20, 51);
        assert!(truncated);
        assert!(out.len() <= 51);
        assert!(out.chars().all(|c| c == 'é'), "no replacement or partial char");
    }

    #[test]
    fn an_empty_output_is_reported_as_no_message_not_an_empty_one() {
        let tasks = [task("blank", "failed", Some("2026-01-01T00:00:02Z"), Some("   \n"))];
        let f = summarize_failure("failed", None, &tasks).unwrap();
        assert_eq!(f.message, None);
    }
}
